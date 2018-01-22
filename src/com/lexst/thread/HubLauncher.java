/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com  All rights reserved
 * 
 * basic launcher of manager site (top site and home site)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/23/2011
 * 
 * @see com.lexst.thread
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.thread;

import java.util.*;

import org.w3c.dom.*;

import com.lexst.log.client.*;
import com.lexst.util.host.*;
import com.lexst.xml.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * <backup-sites>
 * 	<copy-time> 30 </copy-time> <!-- second -->
 *  <active-time> 5 </active-time>  
 *  <detect-time> 180 </detect-time>
 *  
 * 	<backup-site>
 *		<ip> 192.168.0.122 </ip>
 *		<tcp-port> 8866 </tcp-port>
 *		<udp-port> 8866 </udp-port>
 * 	</backup-site>
 * 
 * 	<backup-site>
 *		<ip> 192.168.0.123 </ip>
 *		<tcp-port> 6688 </tcp-port>
 *		<udp-port> 6688 </udp-port>
 * 	</backup-site>
 *  
 * </backup-sites>
 */

/**
 * 管理节点启动器，区别于工作节点启动器。是HOME、TOP节点的运行基类。<br>
 */
public abstract class HubLauncher extends BasicLauncher {

	/** 是否处于运行节点状态 **/
	private boolean runflag;

	/** 复制运行节点资源间隔时间 **/
	private int copyInterval;

	/** 通信激活时间 **/
	private int activeInterval;

	/** 互相检测间隔时间 **/
	private int detectInterval;

	/** 备份节点地址集合 */
	protected ArrayList<SiteHost> backups = new ArrayList<SiteHost>();

	/**
	 * 初始化管理节点启动器
	 */
	protected HubLauncher() {
		super();
		this.setRunsite(false);
		this.setCopyInterval(30);
		this.setActiveInterval(5);
		this.setDetectInterval(5 * 60);
	}
	
	/**
	 * 设置从运行节点复制资源的间隔时间。单位：秒
	 * @param second
	 */
	protected void setCopyInterval(int second) {
		if (second < 1) {
			throw new IllegalArgumentException("invalid copy time:" + second);
		}
		this.copyInterval = second;
	}
	
	/**
	 * 返回复制资源间隔时间，单位：秒
	 * @return
	 */
	public int getCopyInterval() {
		return this.copyInterval;
	}
	
	/**
	 * 设置通信激活间隔时间，单位：秒
	 * @param second
	 */
	protected void setActiveInterval(int second) {
		if (second < 1) {
			throw new IllegalArgumentException("invalid active time:" + second);
		}
		this.activeInterval = second;
	}
	
	/**
	 * 返回通信激活间隔时间，单位：秒
	 * @return
	 */
	public int getActiveInterval() {
		return this.activeInterval;
	}
	
	/**
	 * 设置检测间隔时间，单位：秒
	 * @param second
	 */
	protected void setDetectInterval(int second) {
		if (second < 1) {
			throw new IllegalArgumentException("invalid detect time:" + second);
		}
		this.detectInterval = second;
	}
	
	/**
	 * 返回检测间隔时间，单位：秒
	 * @return
	 */
	public int getDetectInterval() {
		return this.detectInterval;
	}

	/**
	 * 设置为运行节点或者不是
	 * 
	 * @param b
	 */
	protected void setRunsite(boolean b) {
		this.runflag = b;
	}

	/**
	 * 判断是不是运行节点
	 * 
	 * @return
	 */
	public boolean isRunsite() {
		return this.runflag;
	}

	/**
	 * 选择一台主机,必须是哈希码最大的
	 * @param host
	 * @return
	 */
	public SiteHost voting(SiteHost[] hosts) {
		if (this.runflag) {
			return null;
		}

		Map<Integer, SiteHost> map = new HashMap<Integer, SiteHost>(16);
		for (SiteHost host : hosts) {
			int hash = host.hashCode();
			map.put(hash, host);
		}

		int value = 0;
		for (int hash : map.keySet()) {
			if (value == 0 || value < hash) value = hash;
		}

		SiteHost host = map.get(value);
		return host;
	}

	/**
	 * 解析并且保存后备节点地址<br>
	 * 后备节点监视运行节点，通常有多个。当运行节点故障时，通过协商从中选择一个代替故障节点，成为新的运行节点。
	 * 
	 * @param document
	 * @return
	 */
	protected boolean loadBackups(Document document) {
		NodeList list = document.getElementsByTagName("backup-sites");
		// 允许没有后备节点
		if (list == null || list.getLength() == 0) {
			return true;
		}
		// 配置只有一个，超过即错误
		if (list.getLength() != 1) {
			return false;
		}

		Element elem = (Element) list.item(0);

		// 备份节点复制运行节点参数间隔时间
		XMLocal xml = new XMLocal();
		String value = xml.getValue(elem, "copy-time");
		try {
			this.setCopyInterval(Integer.parseInt(value));
		} catch (NumberFormatException exp) {
			Logger.error(exp);
			return false;
		}
		
		value = xml.getValue(elem, "active-time");
		try {
			this.setActiveInterval(Integer.parseInt(value));
		} catch (NumberFormatException exp) {
			Logger.error(exp);
			return false;
		}
		
		value = xml.getValue(elem, "detect-time");
		try {
			this.setDetectInterval(Integer.parseInt(value));
		} catch (NumberFormatException exp) {
			Logger.error(exp);
			return false;
		}

		// 其它备节地址(备份节点相互也需要通信，在运行节点故障时协商选出新的运行节点)
		list = elem.getElementsByTagName("backup-site");
		if (list == null || list.getLength() == 0) return false;

		int size = list.getLength();
		for (int i = 0; i < size; i++) {
			elem = (Element) list.item(i);
			String ip = xml.getValue(elem, "ip");
			String tcport = xml.getValue(elem, "tcp-port");
			String udport = xml.getValue(elem, "udp-port");

			// 保存后备节点地址
			try {
				InetAddress inet = InetAddress.getByName(ip);
				SiteHost host = new SiteHost(inet, Integer.parseInt(tcport), Integer.parseInt(udport));
				backups.add(host);
			} catch (UnknownHostException e) {
				Logger.error(e);
				return false;
			} catch (NumberFormatException e) {
				Logger.error(e);
				return false;
			}
		}
		return true;
	}

}
/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * this file is part of lexst, lexst basic pool
 * 
 * @author scott.liang lexst@126.com
 * @version 1.0 5/2/2009
 * 
 * @see com.lexst.pool
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.pool;

import java.io.*;

import com.lexst.log.client.*;
import com.lexst.remote.client.home.*;
import com.lexst.util.host.*;

/**
 * 工作节点管理池，提供向远程节点发向UDP数据包的工作。<br>
 * 
 * 提供向HOME节点的连接服务。<br>
 *
 */
public abstract class JobPool extends Pool {

	/** HOME节点服务器绑定地址 */
	private SiteHost remote;

	/**
	 * default
	 */
	protected JobPool() {
		super();
	}
	
	/**
	 * 备份HOME节点绑定地址
	 * @param host
	 */
	public void setHome(SiteHost host) {
		this.remote = new SiteHost(host);
	}

	/**
	 * 返回HOME节点绑定地址
	 * @return
	 */
	public SiteHost getHome() {
		return this.remote;
	}

	/**
	 * 连接HOME节点，返回HomeClient句柄
	 * 
	 * @param stream - 选择数据流模式或者不是
	 * @return
	 */
	protected HomeClient bring(boolean stream) {
		SocketHost address = (stream ? remote.getStreamHost() : remote.getPacketHost());
		HomeClient client = new HomeClient(stream, address);
		for (int i = 0; i < 3; i++) {
			try {
				client.reconnect();
				return client;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		return null;
	}

	/**
	 * return a home client handle
	 * 
	 * @return
	 */
	protected HomeClient bring(SiteHost home) {
		if (remote == null || !remote.equals(home)) {
			remote = new SiteHost(home);
		}
		return this.bring(true);
	}
	
	/**
	 * 默认选择流模式，启动与HOME节点的连接
	 * 
	 * @return
	 */
	protected HomeClient bring() {
		return bring(true);
	}

	/**
	 * 关闭与HOME节点的连接
	 * 
	 * @param client
	 */
	protected void complete(HomeClient client) {
		if (client == null)
			return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
	}

//	/** 服务池检查时间间隔 **/
//	protected long checkTime;
//
//	/** 删除超时时间 **/
//	protected long deleteTime;
//
//	/** 节点更新超时 **/
//	protected long refreshTimeout;
//
//	/** UDP数据包发送接口 **/
//	private IPacketListener listener;
//
//	/**
//	 * default
//	 */
//	protected JobPool() {
//		super();
//		this.setSiteTimeout(20);
//		this.setDeleteTimeout(60);
//	}
//
//	/**
//	 * 设置UDP数据包监听器
//	 * @param s
//	 */
//	public void setPacketListener(IPacketListener s) {
//		this.listener = s;
//	}
//
//	/**
//	 * 返回UDP数据包监听器
//	 * @return
//	 */
//	public IPacketListener getPacketListener() {
//		return this.listener;
//	}
//
//	public void setDeleteTimeout(int second) {
//		if (second >= 5) {
//			deleteTime = second * 1000;
//		}
//	}
//
//	public long getDeleteTimeout() {
//		return this.deleteTime;
//	}
//
//	public void setSiteTimeout(int second) {
//		if (second >= 5) {
//			this.refreshTimeout = second * 1000;
//		}
//	}
//
//	public int getSiteTimeout() {
//		return (int) (this.refreshTimeout / 1000);
//	}
//
//	/**
//	 * send timeout packet to remote
//	 * @param remote
//	 * @param local
//	 * @return
//	 */
//	protected boolean sendTimeout1(SiteHost remote, SiteHost local, int num) {
//		for (int i = 0; i < num; i++) {
//			Command command = new Command(Request.NOTIFY, Request.COMEBACK);
//			Packet packet = new Packet(command);
//			// local listen server address
//			packet.addMessage(new Message(Key.SERVER_IP, local.getSpecifyAddress()));
//			packet.addMessage(new Message(Key.SERVER_TCPORT, local.getTCPort()));
//			packet.addMessage(new Message(Key.SERVER_UDPORT, local.getUDPort()));
//			packet.addMessage(new Message(Key.TIMEOUT, refreshTimeout)); // second
//			SocketHost address = remote.getPacketHost();
//			// send to client
//			listener.send(address, packet);
//		}
//		return true;
//	}
}
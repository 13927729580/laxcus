/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com  All rights reserved
 * 
 * lexst launcher basic class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/5/2009
 * 
 * @see com.lexst.thread
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.thread;

import java.io.*;
import java.net.*;

import org.w3c.dom.*;

import com.lexst.fixp.*;
import com.lexst.fixp.monitor.*;
import com.lexst.invoke.*;
import com.lexst.invoke.impl.*;
import com.lexst.log.client.*;
import com.lexst.util.host.*;
import com.lexst.xml.*;

/**
 * 进程启动器，所有节点的启动器均从这里派生。<br>
 * 进程启动器提供的服务：FIXP UDP/TCP 服务器的启动/停止，收发与其它节点的通信和远程RPC调用。<br>
 *
 */
public abstract class BasicLauncher extends VirtualThread {
	
	/** user operate */
	public final static int NONE = 0;
	public final static int LOGIN = 1;
	public final static int RELOGIN = 2;
	public final static int LOGOUT = 3;

	/** RPC 分派实现接口 */
	private RPCInvokerImpl rpcImpl;

	/** FIXP UDP包分派接口 **/
	protected PacketInvoker packetImpl;

	/** FIXP TCP流分派接口 **/
	protected StreamInvoker streamImpl;

	/** FIXP 流模式监听服务器 **/
	protected FixpStreamMonitor fixpStream;

	/** FIXP 包模式监听服务器 **/
	protected FixpPacketMonitor fixpPacket;
	
	// send refresh site
	protected boolean arouse;
	/** 节点最大超时时间 **/
	protected long siteTimeout;
	/** 节点最近一次回应时间 **/
	private long replyTime;
	
	// user operate type
	private int operate;
	
	/** 授权远程关闭的网络地址列表 **/
	private ShutdownSheet shutdownSheet = new ShutdownSheet();
	
	/** 各节点资源配置目录 */
	private File resourcePath;
	
	/**
	 * default
	 */
	protected BasicLauncher() {
		super();
		rpcImpl = new RPCInvokerImpl();
		fixpStream = new FixpStreamMonitor();
		fixpPacket = new FixpPacketMonitor(2);
		arouse = false;
		setSiteTimeout(20);
		replyTime = System.currentTimeMillis();
		this.setOperate(BasicLauncher.NONE);
	}
	
	public void setOperate(int value) {
		if (BasicLauncher.NONE <= value && value <= BasicLauncher.LOGOUT) {
			this.operate = value;
			if (super.isRunning()) super.wakeup();
		}
	}

	public int getOperate() {
		return this.operate;
	}
	
	public boolean isNoneOperate() {
		return this.operate == BasicLauncher.NONE;
	}

	public boolean isLoginOperate() {
		return this.operate == BasicLauncher.LOGIN;
	}

	public boolean isReloginOperate() {
		return this.operate == BasicLauncher.RELOGIN;
	}

	public boolean isLogoutOperate() {
		return this.operate == BasicLauncher.LOGOUT;
	}

	public SocketHost getStreamHost() {
		return fixpStream.getLocal();
	}

	public SocketHost getPacketHost() {
		return fixpPacket.getLocal();
	}

	public void setSiteTimeout(int second) {
		if (second >= 1) {
			siteTimeout = second * 1000;
		}
	}

	public int getSiteTimeout() {
		return (int) (siteTimeout / 1000);
	}

	/**
	 * fixp packet monitor send to here
	 */
	public void comeback() {
		this.arouse = true;
		this.wakeup();
	}

	/**
	 * @param cls
	 * @return
	 */
	protected boolean addInstance(Class<?> cls) {
		return rpcImpl.addInstance(cls);
	}

	/**
	 * 启动RPC监听服务
	 * @param clazz
	 * @param local
	 * @return
	 */
	protected boolean loadListen(Class<?>[] clazz, SiteHost local) {
		// 检查选择一个有效的本地地址
		InetAddress inet = local.getInetAddress();
		if (inet.isAnyLocalAddress() || inet.isLoopbackAddress()) {
			inet = Address.select();
			local.getAddress().setAddress(inet);
		}

		fixpStream.setLocal(inet, local.getTCPort());
		fixpPacket.setLocal(inet, local.getUDPort());

		boolean success = true;
		for (int i = 0; clazz != null && i < clazz.length; i++) {
			success = rpcImpl.addInstance(clazz[i]);
			if (!success) return false;
		}

		if (success) {
			fixpStream.setRPCall(rpcImpl);
			fixpPacket.setRPCall(rpcImpl);
			fixpStream.setStreamCall(streamImpl);
			fixpPacket.setPacketCall(packetImpl);
		}
		// start stream listener
		if (success) {
			success = fixpStream.start();
		}
		// start packet listener
		if (success) {
			success = fixpPacket.start();
			if (!success) {
				fixpStream.stop();
			}
		}

		// 等待FIXP监听器进入线程状态
		if (success) {
			while (!fixpStream.isRunning()) {
				this.delay(200);
			}
			while (!fixpPacket.isRunning()) {
				this.delay(200);
			}
		}

		return success;
	}

	/**
	 * stop fixp service
	 */
	protected void stopListen() {
		Notifier not1 = new Notifier();
		Notifier not2 = new Notifier();
		fixpStream.stop(not1);
		fixpPacket.stop(not2);

		while(!not1.isKnown()) {
			this.delay(200);
		}
		while(!not2.isKnown()) {
			this.delay(200);
		}
	}

	/**
	 * refresh reply time (active reply)
	 */
	public void refreshEndTime() {
		replyTime = System.currentTimeMillis();
	}
	
	protected boolean isMaxSiteTimeout() {
		return System.currentTimeMillis() - replyTime >= siteTimeout * 3;
	}

	protected boolean isSiteTimeout() {
		return System.currentTimeMillis() - replyTime >= siteTimeout;
	}

	/**
	 * send active packet to target host(top or home)
	 * @param num
	 * @param sitetype
	 * @param remote
	 */
	protected void hello(int num, int sitetype, SocketHost remote) {
		Command cmd = new Command(Request.NOTIFY, Request.HELO);
		Packet packet = new Packet(cmd);
		packet.addMessage(Key.BIND_IP, fixpPacket.getLocal().getSpecifyAddress());
		packet.addMessage(Key.SITE_TYPE, sitetype);
		packet.addMessage(Key.IP, fixpStream.getLocal().getSpecifyAddress());
		packet.addMessage(Key.TCPORT, fixpStream.getLocal().getPort());
		packet.addMessage(Key.UDPORT, fixpPacket.getLocal().getPort());
		// send a request packet to home site
		for (int i = 0; i < num; i++) {
			if (i > 0) this.delay(10);
			fixpPacket.send(remote, packet);
		}
	}
	
	/**
	 * ping to home site or top site
	 * @param sitetype
	 * @param remote
	 */
	protected void hello(int sitetype, SiteHost remote) {
		int number = 1;
		if (arouse) {
			number = 3;
			arouse = false;
		}
		for (int i = 0; i < number; i++) {
			hello(number, sitetype, remote.getPacketHost());
		}
	}

	/**
	 * 解析HOME节点地址
	 * 
	 * @param document
	 * @return
	 */
	protected SiteHost splitHome(Document document) {
		XMLocal xml = new XMLocal();
		Element elem = (Element) document.getElementsByTagName("home-site").item(0);
		String ip = xml.getValue(elem, "ip");
		String tcport = xml.getValue(elem, "tcp-port");
		String udport = xml.getValue(elem, "udp-port");
		
		try {
			// 根据域名或者IP地址，返回一个网络地址类
			InetAddress inet = InetAddress.getByName(ip);
			// 如果是自回路地址或者通配符地址，选择一个有效的地址
			if(inet.isLoopbackAddress() || inet.isAnyLocalAddress() || inet.isMulticastAddress()) {
				inet = Address.select();
			}
			return new SiteHost(inet, Integer.parseInt(tcport), Integer.parseInt(udport));
		} catch (UnknownHostException e) {
			Logger.error(e);
		} catch (NumberFormatException e) {
			Logger.error(e);
		}
		
		return null;
	}

	/**
	 * 解析当前节点地址
	 * @param document
	 * @return
	 */
	protected SiteHost splitLocal(Document document) {
		XMLocal xml = new XMLocal();
		Element elem = (Element) document.getElementsByTagName("local-site").item(0);
		String ip = xml.getValue(elem, "ip");
		String tcport = xml.getValue(elem, "tcp-port");
		String udport = xml.getValue(elem, "udp-port");

		try {
			InetAddress inet = InetAddress.getByName(ip);
			if (inet.isMulticastAddress() || inet.isLoopbackAddress() || inet.isAnyLocalAddress()) {
				inet = Address.select();
			}
			return new SiteHost(inet, Integer.parseInt(tcport), Integer.parseInt(udport));
		} catch (UnknownHostException e) {
			Logger.error(e);
		} catch (NumberFormatException e) {
			Logger.error(e);
		}

		return null;
	}

	/**
	 * 加载解析远程关闭服务
	 * 
	 * @param document
	 * @return
	 */
	protected boolean loadShutdown(Document document) {
		XMLocal xml = new XMLocal();
		Element element = (Element) document.getElementsByTagName("accept-shutdown-address").item(0);

		String[] items = xml.getXMLValues(element.getElementsByTagName("ip"));
		if (items == null) return true;
		try {
			for (int i = 0; i < items.length; i++) {
				Address address = new Address(items[i]);
				this.shutdownSheet.add(address);
			}
		} catch (UnknownHostException e) {
			Logger.error(e);
			return false;
		}
		
		// 收缩内存空间
		this.shutdownSheet.trim();

		return true;
	}
	
	/**
	 * 检查地址是否在停止表集合中
	 * @param address
	 * @return
	 */
	public boolean inShutdown(Address address) {
		Logger.debug("BasicLauncher.inShutdown, address is: %s", address);

		//1. 远程地址与本地地址一致，证明是在一台主机上，允许退出
		Address[] locales = Address.locales();
		if (address.matchsIn(locales)) {
			return true;
		}
		//2. 检查集合中的地址是否匹配
		return this.shutdownSheet.contains(address);
	}

	/**
	 * 加载解析FIXP安全服务
	 * 
	 * @param document
	 * @return
	 */
	protected boolean loadSecurity(Document document) {
		XMLocal xml = new XMLocal();
		// 解析安全配置文件
		String filename = xml.getXMLValue(document.getElementsByTagName("security-file"));

		if (filename != null && filename.length() > 0) {
			Security safe = new Security();
			if (!safe.parse(filename)) {
				return false;
			}
			fixpPacket.setSecurity(safe);
			fixpStream.setSecurity(safe);
		}

		return true;
	}

	/**
	 * 读磁盘文件
	 * 
	 * @param file
	 * @return
	 */
	protected byte[] readFile(File file) {
		if (!file.exists()) return null;
		byte[] data = new byte[(int) file.length()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(data);
			in.close();
			return data;
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		return null;
	}

	/**
	 * 写磁盘文件
	 * 
	 * @param file
	 * @param b
	 * @return
	 */
	protected boolean flushFile(File file, byte[] b) {
		try {
			FileOutputStream out = new FileOutputStream(file);
			out.write(b);
			out.close();
			return true;
		} catch (IOException exp) {

		}
		return false;
	}
	
	/**
	 * 返回资源配置目录
	 * @return
	 */
	public File getResourcePath() {
		return this.resourcePath;
	}

	/**
	 * 建立配置文件存储目录
	 * @param path
	 * @return
	 */
	protected boolean createResourcePath(String path) {
		File dir = new File(path);
		boolean success = (dir.exists() && dir.isDirectory());
		if (!success) {
			success = dir.mkdirs();
		}
		if (success) {
			try {
				resourcePath = dir.getCanonicalFile();
			} catch (IOException e) {
				resourcePath = dir.getAbsoluteFile();
				Logger.error(e);
			}
		}
		Logger.note(success, "BasicLauncher.createResourcePath, directory is '%s'", resourcePath);
		return success;
	}
	
	/**
	 * 生成资源配置下的文件
	 * @param filename
	 * @return
	 */
	protected File buildResourceFile(String filename) {
		return new File(resourcePath, filename);
	}
}
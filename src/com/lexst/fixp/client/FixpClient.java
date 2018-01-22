/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * fixp basic client
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/16/2009
 * 
 * @see com.lexst.fixp.client
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.fixp.client;

import java.net.*;

import com.lexst.util.host.*;
import com.lexst.fixp.Cipher;

class FixpClient {
	
	/** 服务器连接地址 **/
	protected SocketHost remote = new SocketHost(SocketHost.NONE);

	/** 本地绑定地址 **/
	protected InetAddress bindIP;

	/** 调试状态 */
	protected boolean debug;

	/** 连接超时，单位：秒 */
	protected int connect_timeout;

	/** 接收超时，单位：秒 */
	protected int receive_timeout;

	/** SOCKET接收缓冲区 */
	protected int receive_buffsize;

	/** SOCKET发送缓冲区 */
	protected int send_buffsize;

	/** security cipher */
	protected Cipher cipher;
	
	/**
	 * default
	 */
	public FixpClient() {
		super();
		this.setConnectTimeout(60);		//连接超时60秒
		this.setReceiveTimeout(0); 		//0是无限期接收
		this.setReceiveBuffSize(512);
		this.setSendBuffSize(512);
		this.setDebug(false);
	}

	/**
	 * 设置服务器连接地址
	 * @param host
	 */
	public void setRemote(SocketHost host) {
		this.remote = new SocketHost(host);
	}

	/**
	 * 返回服务器连接地址
	 * @return
	 */
	public SocketHost getRemote() {
		return this.remote;
	}
	
	/**
	 * 设置客户端绑定地址
	 * @param s
	 */
	public void setBindIP(InetAddress s) {
		this.bindIP = s;
	}
	
	/**
	 * 返回本地绑定地址
	 * @return
	 */
	public InetAddress getBindIP() {
		return this.bindIP;
	}

	/**
	 * 设置调试状态
	 * @param b
	 */
	public void setDebug(boolean b) {
		debug = b;
	}

	/**
	 * 判断是否调试状态
	 * @return
	 */
	public boolean isDebug() {
		return debug;
	}

	/**
	 * 睡眠时间
	 * @param timeout
	 */
	protected synchronized void delay(long timeout) {
		try {
			this.wait(timeout);
		} catch (InterruptedException exp) {

		}
	}
	
	/**
	 * 唤醒
	 */
	protected synchronized void wakeup() {
		try {
			this.notify();
		} catch (IllegalMonitorStateException exp) {

		}
	}

	/**
	 * 设置超时时间(秒)
	 * 
	 * @param second
	 */
	public void setConnectTimeout(int second) {
		if (second > 1) connect_timeout = second;
	}

	/**
	 * 返回超时时间
	 * 
	 * @return
	 */
	public int getConnectTimeout() {
		return connect_timeout;
	}

	/**
	 * 设置SOCKET接收超时时间
	 * @param second
	 */
	public void setReceiveTimeout(int second) {
		if (second >= 0) {
			receive_timeout = second;
		}
	}
	
	/**
	 * 返回SOCKET接收超时时间
	 * @return
	 */
	public int getReceiveTimeout() {
		return receive_timeout;
	}

	/**
	 * socket receive buffer size
	 * @param size
	 */
	public void setReceiveBuffSize(int size) {
		if(size >= 128) {
			receive_buffsize = size;
		}
	}

	public int getReceiveBuffSize() {
		return receive_buffsize;
	}

	/**
	 * socket send buffer size
	 *
	 * @param size
	 */
	public void setSendBuffSize(int size) {
		if(size >= 128) {
			send_buffsize = size;
		}
	}

	public int getSendBuffSize() {
		return send_buffsize;
	}

	/**
	 * set cipher instance of null
	 * 
	 * @param cipher
	 */
	public void setCipher(Cipher cipher) {
		this.cipher = cipher;
	}

	public Cipher getCipher() {
		return this.cipher;
	}
}
/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * fixp tcp server
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/18/2009
 * 
 * @see com.lexst.fixp.monitor
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.fixp.monitor;

import java.io.*;
import java.net.*;
import java.util.Vector;

import com.lexst.invoke.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;

/**
 * FIXP数据流监听服务器
 *
 */
public class FixpStreamMonitor extends FixpMonitor implements IStreamListener {
	
	/** FIXP监听器默认绑定端口 */
	public final static int FIXP_STREAM_PORT = 5000;

	/** 打印日志标记 **/
	private boolean print;

	/** 本地主机绑定地址 **/
	private SocketHost local;

	/** 服务器SOCKET句柄 **/
	private ServerSocket server;

	/** RPC调用句柄 **/
	private RPCInvoker rpcInstance;

	/** 数据流句柄 **/
	private StreamInvoker streamInstance;

	// task instance set
	private Vector<StreamTask> array = new Vector<StreamTask>(20);

	/**
	 * default
	 */
	public FixpStreamMonitor() {
		super();
		this.print = true; //default is print log
		local = new SocketHost(SocketHost.TCP);
		local.setPort(FixpStreamMonitor.FIXP_STREAM_PORT);
	}

	/**
	 * @param ip
	 * @param port
	 */
	public FixpStreamMonitor(InetAddress ip, int port) {
		this();
		this.setLocal(ip, port);
	}

	/**
	 * @param host
	 */
	public FixpStreamMonitor(SocketHost host) {
		this();
		this.setLocal(host);
	}

//	/**
//	 * set local address
//	 * @param ip
//	 * @param port
//	 */
//	public void setLocal(String ip, int port) {
//		local.setIP(ip);
//		local.setPort(port);
//	}
	
	/**
	 * 设置本地主机地址
	 */
	public void setLocal(InetAddress address, int port) {
		this.local.setInetAddress(address);
		this.local.setPort(port);
	}
	
	/**
	 * @param host
	 */
	public void setLocal(SocketHost host) {
		this.setLocal(host.getInetAddress(), host.getPort());
//		local.setIP(host.getIP());
//		local.setAddress(host.getAddress());
//		local.setPort(host.getPort());
	}
	/**
	 * return local address
	 * @return
	 */
	public SocketHost getLocal() {
		return this.local;
	}

	/**
	 * set RPC handle
	 * @param instance
	 */
	public void setRPCall(RPCInvoker instance) {
		rpcInstance = instance;
	}
	public RPCInvoker getRPCall() {
		return rpcInstance;
	}

	/**
	 * set stream call handle
	 * @param instance
	 */
	public void setStreamCall(StreamInvoker instance) {
		streamInstance = instance;
	}
	public StreamInvoker getStreamCall() {
		return streamInstance;
	}

	public void setPrint(boolean b) {
		this.print = b;
	}
	public boolean isPrint() {
		return this.print;
	}

	/**
	 * remove a object
	 */
	@Override
	public boolean remove(StreamTask task) {
		return array.remove(task);
	}

	/**
	 *
	 */
	public void stop() {
		super.stop();
		this.close();
	}

	/**
	 */
	public void stop(Notifier notify) {
		super.stop(notify);
		this.close();
	}

	/**
	 * close socket
	 */
	private void close() {
		if (server == null) return;
		try {
			server.close();
		} catch (IOException exp) {

		} finally {
			server = null;
		}
	}

	/**
	 * bind socket
	 * @return
	 */
	private boolean bind() {
		// 如果是通配符地址选择一个可能有效的地址
		if (!local.isValid()) {
			local.setInetAddress(Address.select());
		}
		
		int port = local.getPort();

		if(print) {
			com.lexst.log.client.Logger.info("FixpStreamMonitor.bind, bind to %s:%d", local.getAddress(), port);
		}

		InetSocketAddress address = new InetSocketAddress(local.getInetAddress(), port);
		try {
			server = new ServerSocket();
			server.bind(address);
			// when port not match
			if (port != server.getLocalPort()) {
				this.local.setPort(server.getLocalPort());
			}
			return true;
		} catch (IOException exp) {
			if (print) {
				com.lexst.log.client.Logger.error(exp);
			}
		} catch (Throwable exp) {
			if(print) {
				com.lexst.log.client.Logger.error(exp);
			}
		}
		return false;
	}

	/**
	 * re-bind to local address
	 * @return boolean
	 */
	private boolean rebind() {
		boolean success = false;
		while (!this.isInterrupted()) {
			this.close();
			this.delay(500); // sleep 0.5 second, free native socket
			success = this.bind();
			if (success) break;
			this.delay(2000); // sleep 2 second, retry
		}
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// close socket
		this.close();
		// close stream task
		for(StreamTask task : array) {
			task.stop();
		}
		// wait to empty
		while(!array.isEmpty()) {
			this.delay(1000);
		}
		if(print) {
			com.lexst.log.client.Logger.info("FixpStreamMonitor.finish, finished!");
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return bind();
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		if (print) {
			com.lexst.log.client.Logger.info("FixpStramMonitor.process, into...");
		}

		while (!isInterrupted()) {
			try {
				Socket socket = server.accept();
				StreamTask task = new StreamTask(socket, this, rpcInstance, streamInstance);
				task.setSecurity(this.security);
				// save socket handle
				array.add(task);
				// start thread
				task.start();
			} catch (IOException exp) {
				// rebind socket...
				this.rebind();
			}
		}

		if (print) {
			com.lexst.log.client.Logger.info("FixpStreamMonitor.process, exit...");
		}
	}

}
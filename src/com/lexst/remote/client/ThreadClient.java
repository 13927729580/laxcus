/**
 *
 */
package com.lexst.remote.client;

import java.io.*;

/**
 * 线程客户端. 供子类使用. 子类必须实现"execute"方法
 *
 */
public abstract class ThreadClient extends RemoteClient implements Runnable {

	/** 线程句柄 **/
	private Thread thread;

	/** 停止标识 **/
	private boolean interrupted;

	/** 线程睡眠时间 **/
	private long sleep;
	
	/**
	 * construct method
	 */
	public ThreadClient(boolean stream) {
		super(stream);
		setSleep(10000L);
		thread = null;
		this.interrupted = false;
	}

	/**
	 * @param interfaceName
	 */
	public ThreadClient(boolean stream, String interfaceName) {
		this(stream);
		this.setInterfaceName(interfaceName);
	}
	
	/**
	 * set default delay time
	 * @param timeout (milli-second)
	 */
	public void setSleep(long timeout) {
		if (timeout >= 1000L) {
			this.sleep = timeout;
		}
	}

	public long getSleep() {
		return this.sleep;
	}

	/**
	 * thread wait
	 */
	protected void sleep() {
		this.delay(sleep);
	}

	/**
	 * stop thread and close socket
	 */
	public void stop() {
		interrupted = true;
		if(this.isRunning()) wakeup();
	}

	/**
	 * check thread status
	 * @return
	 */
	public boolean isInterrupted() {
		return this.interrupted;
	}

	/**
	 * @return
	 */
	public boolean isRunning() {
		return this.thread != null;
	}

	/**
	 * 如果线程未启动，启动线程。否则唤醒它
	 */
	public void wakeupThread() {
		if (thread != null) {
			this.wakeup();
		} else {
			thread = new Thread(this);
			thread.start();
		}
	}

	/**
	 * start thread
	 */
	public void start() {
		if (thread == null) {
			thread = new Thread(this);
			thread.start();
		}
	}

	/**
	 * 通知线程，停止运行 stop thread
	 */
	public void interrupt() {
		this.interrupted = true;
		if (this.isRunning()) this.wakeup();
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while (!isInterrupted()) {
			// 执行任务(子类实现)
			execute();
		}
		// 释放资源
		this.release();
		thread = null;
	}

	/**
	 * 释放连接
	 */
	private void release() {
		try {
			if (isConnected()) {
				this.exit();
			}
		} catch (IOException exp) {
			exp.printStackTrace();
		} catch (Throwable exp) {
			exp.printStackTrace();
		} finally {
			this.close();
		}
	}

	/**
	 * 在线程里执行远程服务
	 */
	protected abstract void execute();
}
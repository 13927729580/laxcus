/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com  All rights reserved. lexst@126.com
 * 
 * lexst thread basic class
 * 
 * @author scott.liang
 * 
 * @version 1.0 2/1/2009
 * 
 * @see com.lexst.thread
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.thread;

/**
 * 线程基础类，所有基于线程运行的类全部由此派生。<br>
 *
 */
public abstract class VirtualThread implements Runnable {
	
	/** 线程句柄 */
	private Thread thread;

	/** 线程运行标记 */
	private boolean running;

	/** 线程中止标记 **/
	private boolean interrupted;

	/** 线程延时时间，单位:毫秒 */
	private long sleep;

	/** 线程停止后，是否退出JVM。默认是TRUE。(用于各节点的Launcher) */
	private boolean exitVM;

	/** 是否打印日志 **/
	private boolean logging;

	/** 消息通知接口 **/
	private Notifier notifier;

	/**
	 * Constructs a basic thread
	 */
	public VirtualThread() {
		super();
		this.setSleep(5);
		this.interrupted = false;
		this.thread = null;
		this.running = false;
		this.exitVM = false;
		this.logging = false;
		this.notifier = null;
	}

	/**
	 * 设置退出JAVA虚拟机标记
	 * 
	 * @param b
	 */
	public void setExitVM(boolean b) {
		this.exitVM = b;
	}

	/**
	 * 线程停止是否退出JAVA虚拟机
	 * 
	 * @return
	 */
	public boolean isExitVM() {
		return this.exitVM;
	}

	/**
	 * 设置日志打印标记
	 * @param b
	 */
	public void setLogging(boolean b) {
		this.logging = b;
	}
	
	/**
	 * 是否打印日志
	 * @return
	 */
	public boolean isLogging() {
		return this.logging;
	}

	/**
	 * set sleep time, unit:second
	 * @param second
	 */
	public void setSleep(int second) {
		if (second >= 1) {
			this.sleep = second * 1000L;
		}
	}

	/**
	 * return sleep time, unit: milli-second
	 * @return
	 */
	public int getSleep() {
		return (int) (sleep / 1000L);
	}

	/**
	 * 线程延时等待
	 * @param timeout
	 */
	public synchronized void delay(long timeout) {
		try {
			super.wait(timeout);
		}catch(InterruptedException exp) {

		}
	}

	/**
	 * sleep
	 */
	protected void sleep() {
		this.delay(this.sleep);
	}

	/**
	 * 唤醒线程
	 */
	protected synchronized void wakeup() {
		try {
			this.notify();
		}catch(IllegalMonitorStateException exp) {

		}
	}

	/**
	 * 启动线程，并且调用init方法。<br>
	 * 成功返回TRUE，失败返回FALSE。<br>
	 *
	 * @return boolean
	 */
	public boolean start() {
		synchronized (this) {
			if (thread != null) {
				return false;
			}
		}
		// init service
		boolean success = init();
		if (!success) {
			// print log
			if (logging) {
				com.lexst.log.client.Logger.gushing();
			}
			// exit java vm
			if (exitVM) {
				System.exit(0);
			}
			return false; // failed
		}
		// start thread
		thread = new Thread(this);
		thread.start();
		return true;
	}

	/**
	 * 停止线程运行
	 */
	public void stop() {
		if (interrupted) return;
		interrupted = true;
		this.wakeup();
	}

	/**
	 * 停止线程运行并且通知调用接口
	 * @param noti
	 */
	public void stop(Notifier noti) {
		this.notifier = noti;
		this.stop();
	}

	/**
	 * check interrupt
	 * @return
	 */
	public boolean isInterrupted() {
		return this.interrupted;
	}

	/**
	 * @param b
	 */
	protected void setInterrupted(boolean b) {
		this.interrupted = b;
	}

	/**
	 * check running status
	 */
	public boolean isRunning() {
		return running && thread != null;
	}

	/**
	 * run task
	 */
	public void run() {
		this.running = true;
		while(!isInterrupted()) {
			this.process();
		}
		this.finish();
		// notify handle
		if (notifier != null) {
			notifier.wakeup();
		}
		this.running = false;
		thread = null;
		// enforce JVM
		if(exitVM) {
			System.exit(0);
		}
	}

	/**
	 * init service
	 * @return
	 */
	public abstract boolean init();

	/**
	 * process task
	 */
	public abstract void process();

	/**
	 * stop service
	 */
	public abstract void finish();
}
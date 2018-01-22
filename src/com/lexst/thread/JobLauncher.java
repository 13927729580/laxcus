/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com  All rights reserved
 * 
 * basic launcher of job site (log, data, work, build, call)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/8/2009
 * 
 * @see com.lexst.thread
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.thread;

import java.io.*;

import com.lexst.log.client.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.util.lock.*;
import com.lexst.visit.*;

/**
 * 工作节点启动器，提供向HOME节点的连接，多向锁服务。<br>
 *
 */
public abstract class JobLauncher extends BasicLauncher {

	/** 多向锁(多读单写模式) */
	private MutexLock lock = new MutexLock();

	/** 运行状态的HOME节点主机地址 */
	private SiteHost remote;
	
	/**
	 * 初始化工作节点启动器
	 */
	protected JobLauncher() {
		super();
		this.setLogging(true);
	}
	
	/**
	 * 单向锁定
	 * @return
	 */
	protected boolean lockSingle() {
		return lock.lockSingle();
	}

	/**
	 * 解除单向锁定
	 * @return
	 */
	protected boolean unlockSingle() {
		return lock.unlockSingle();
	}

	/**
	 * 多向锁定
	 * @return
	 */
	protected boolean lockMulti() {
		return lock.lockMulti();
	}

	/**
	 * 解除多向锁定
	 * @return
	 */
	protected boolean unlockMulti() {
		return lock.unlockMulti();
	}

	/**
	 * 设置HOME节点地址
	 * @param host
	 */
	public void setHubSite(SiteHost host) {
		this.remote = new SiteHost(host);
	}
	
	/**
	 * 返回HOME节点地址
	 * @return
	 */
	public SiteHost getHubSite() {
		return this.remote;
	}
	
	/**
	 * 启动客户端日志服务
	 * @param family - 当前节点类型(data, build, call, work)
	 * @param client
	 * @return
	 */
	protected boolean loadLog(int family, HomeClient client) {
		SiteHost host = null;
		// 从HOME节点申请一个日志节点地址
		if (family != Site.NONE && client != null) {
			try {
				host = client.findLogSite(family);
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			if (host == null) {
				Logger.error("JobLauncher.loadLog, cannot find log site!");
				return false;
			}
		}
		// 启动客户端日志服务
		return Logger.loadService(host);
	}

	/**
	 * 停止日志服务
	 */
	protected void stopLog() {
		Logger.stopService();
	}

	/**
	 * 连接HOME节点，返回连接句柄
	 * @return
	 */
	protected HomeClient bring() {
		SocketHost address = this.remote.getStreamHost();
		HomeClient client = new HomeClient(true, address);
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
	 * @param client
	 */
	protected void complete(HomeClient client) {
		if(client == null) return;
		try {
			client.exit();
			client.close();
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
	}

	/**
	 * get site timeout (second)
	 * @param siteType
	 * @param client
	 * @return
	 */
	protected boolean loadTimeout(int siteType, HomeClient client) {
		boolean success = false;
		try {
			int second = client.getSiteTimeout(siteType);
			if (second >= 5) {
				setSiteTimeout(second);
				success = true;
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		return success;
	}

	/**
	 * 从HOME节点取得当前时间，设置本地系统时间。<br>
	 * 实现全环境时间统一(误差小于1秒) <br><br>
	 * 
	 * @param client
	 */
	protected boolean loadTime(HomeClient client) {
		boolean nullable = (client == null);
		if (nullable) client = bring();
		if (client == null) return false;

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if(client.isClosed()) client.reconnect();
				long time = client.currentTime();
				Logger.info("JobLauncher.loadTime, set time %d", time);
				if (time != 0L) {
					int ret = SystemTime.set(time);
					success = (ret == 0);
					break;
				}
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(500);
		}
		if(nullable) complete(client);
		
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		String value = sdf.format(new java.util.Date(System.currentTimeMillis()));
		Logger.note(success, "JobLauncher.loadTime, current time:%s", value);
		
		return success;
	}
}
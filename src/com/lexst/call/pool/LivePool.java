/**
 * 
 */
package com.lexst.call.pool;

import java.util.*;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.site.*;
import com.lexst.site.live.*;
import com.lexst.util.host.*;

/**
 * CALL节点上的LIVE节点管理池。管理范围包括：注册/注销LIVE节点，刷新LIVE节点激活时间。<br>
 * 与TOP节点上的对LIVE节点管理有所不同，这里不检测LIVE节点的超时，达到删除时间即清除。<br>
 *   通知超时的LIVE节点激活，删除超时节点
 *
 */
public class LivePool extends JobPool {
	
	private static LivePool selfHandle = new LivePool();

	/** LIVE节点主机地址 -> 节点属性 **/
	private Map<SiteHost, LiveSite> mapSite = new HashMap<SiteHost, LiveSite>();

	/** LIVE主机地址 -> 最后更新时间 **/
	private Map<SiteHost, Long> mapTime = new HashMap<SiteHost, Long>();

	/**
	 * 初始化LIVE节点管理池
	 */
	private LivePool() {
		super();
	}
	
	/**
	 * 返回LIVE管理池静态句柄
	 * @return
	 */
	public static LivePool getInstance() {
		return LivePool.selfHandle;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("LivePool.process, into...");
		while(!isInterrupted()) {
			this.check();
			this.delay(1000);
		}
		Logger.info("LivePool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		this.mapSite.clear();
		this.mapTime.clear();
	}
	
	/**
	 * LIVE节点注册
	 * @param object
	 * @return
	 */
	public boolean add(Site object) {
		if(object == null || !object.isLive()) {
			return false;
		}
		LiveSite site = (LiveSite)object;
		SiteHost host =	site.getHost();
		
		boolean success = false;
		super.lockSingle();
		try {
			//1.检查地址是否已经注册
			if(mapSite.containsKey(host)) {
				Logger.warning("LivePool.add, %s existed", host);
				return false;
			}
			//2. 记录登陆
			mapSite.put(host, site);
			mapTime.put(host, System.currentTimeMillis());
			success = true;
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		Logger.note(success, "LivePool.add, live site %s", host);
		return success;
	}
	
	/**
	 * LIVE节点注销
	 * @param host
	 * @return
	 */
	public boolean remove(SiteHost host) {
		boolean success = false;
		super.lockSingle();
		try {
			mapTime.remove(host);
			success = mapSite.remove(host) != null;
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		Logger.note(success, "LivePool.remove, live site %s", host);
		return success;
	}

	/**
	 * LIVE节点更新(先删除再增加)
	 * @param object
	 * @return
	 */
	public boolean update(Site object) {
		if (object == null || !object.isLive()) {
			return false;
		}
		SiteHost host = object.getHost();
		Logger.info("LivePool.update, live site %s", host);
		// 注销
		this.remove(host);
		// 重新注册
		return add(object);
	}
	
	/**
	 * LIVE节点刷新(发送UDP包保持激活状态)
	 * 
	 * @param host
	 * @return
	 */
	public boolean refresh(SiteHost host) {
		boolean success = false;
		super.lockSingle();
		try {
			if (mapSite.containsKey(host)) {
				mapTime.put(host, System.currentTimeMillis());
				success = true;
			}
		} catch (Throwable t) {
			Logger.fatal(t);
		} finally {
			super.unlockSingle();
		}
		return success;
	}

	
	private void check() {

	}

}
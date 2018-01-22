package com.lexst.pool.site;

import java.util.*;

import com.lexst.algorithm.choose.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.work.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;

/**
 * aggregate阶段任务所有配置的存储池。<br>
 * 工作范围: <br>
 * <1> 从HOME节点上提取各WORK节点上的任务命名。<br>
 * <2> 维护并且管理存储的命名。<br>
 *
 */
public class ToPool extends JobPool implements ToChooser {

	/** 静态句柄(全局唯一) **/
	private static ToPool selfHandle = new ToPool();

	/** 任务命名 -> WORK节点集合 **/
	private Map<Naming, SiteSet> mapNaming = new TreeMap<Naming, SiteSet>();

	/** HOME节点通知，更新全部WORK节点记录 **/
	private boolean refresh;

	/** To数据池监器，由所在宿主节点实现并且赋值  **/
	private ToListener listener;

	/**
	 * default
	 */
	private ToPool() {
		super();
	}

	/**
	 * @return
	 */
	public static ToPool getInstance() {
		return ToPool.selfHandle;
	}

	/**
	 * 设置宿主监听器
	 * @param s
	 */
	public void setToListener(ToListener s) {
		this.listener = s;
	}

	/**
	 * 返回宿主监听器
	 * @return
	 */
	public ToListener getToListener() {
		return this.listener;
	}

	@Override
	public List<Naming> getNamings() {
		List<Naming> array = new ArrayList<Naming>();
		super.lockMulti();
		try {
			array.addAll(mapNaming.keySet());
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return array;
	}

	@Override
	public SiteSet findToSites(Naming naming) {
		super.lockMulti();
		try {
			return mapNaming.get(naming);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	/**
	 * 通知，更新全部命名
	 */
	public void refresh() {
		this.refresh = true;
		this.wakeup();
	}

	@Override
	public boolean init() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void process() {
		Logger.info("ToPool.process, into...");

		this.delay(10000);
		refresh = true;

		while (!super.isInterrupted()) {
			if (refresh) {
				this.refresh = false;
				this.refreshSite();
			}
			this.delay(5000);
		}
		Logger.info("ToPool.process, exit");
	}

	@Override
	public void finish() {
		// TODO Auto-generated method stub
	}

	/**
	 * 
	 */
	private void refreshSite() {
		HomeClient client = super.bring(false);
		if (client == null) {
			Logger.error("ToPool.refreshSite, cannot connect home-site:%s", getHome());
			return;
		}

		boolean error = true;
		WorkSite[] sites = null;
		try {
			sites = (WorkSite[]) client.batchWorkSite();
			error = false;
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		this.complete(client);

		if (error) {
			Logger.warning("ToPool.refreshSite, visit error!");
			return;
		}

		Logger.debug("ToPool.refreshSite, work site size:%d", (sites == null ? 0 : sites.length));

		List<SiteHost> allsite = new ArrayList<SiteHost>();

		super.lockSingle();
		try {
			mapNaming.clear();
			for (int i = 0; sites != null && i < sites.length; i++) {
				SiteHost host = sites[i].getHost();
				allsite.add(host);
				for (Naming naming : sites[i].list()) {
					SiteSet set = mapNaming.get(naming);
					if (set == null) {
						set = new SiteSet();
						mapNaming.put(naming, set);
					}
					set.add(host);
				}
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}

		// 更知宿主节点当前记录已经更新
		this.listener.updateWorkRecord();
	}

}

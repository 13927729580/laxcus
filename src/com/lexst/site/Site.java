/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com  All rights reserved
 * 
 * lexst basic class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/2/2009
 * 
 * @see com.lexst.site
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.site;

import java.io.*;
import java.net.*;

import com.lexst.util.host.*;

public class Site implements Serializable, Comparable<Site> {

	private static final long serialVersionUID = 4323980166469030137L;

	/** 节点家族类型定义 **/
	public final static int NONE = 0;

	public final static int TOP_SITE = 1;

	public final static int HOME_SITE = 2;

	public final static int LIVE_SITE = 3;

	public final static int LOG_SITE = 4;

	public final static int DATA_SITE = 5;

	public final static int CALL_SITE = 6;

	public final static int WORK_SITE = 7;

	public final static int BUILD_SITE = 8;

	/** 节点类型 */
	private int family;

	/** 当前节点绑定的本机地址 **/
	private SiteHost local;

	/**
	 * default construct
	 */
	public Site() {
		super();
		this.family = Site.NONE;
	}

	/**
	 * @param family
	 */
	public Site(int family) {
		this();
		this.setFamily(family);
	}

	/**
	 * @param address
	 * @param tcport
	 * @param udport
	 */
	public Site(int family, InetAddress address, int tcport, int udport) {
		this(family);
		local = new SiteHost(address, tcport, udport);
	}

	/**
	 * @param site
	 */
	public Site(Site site) {
		this();
		this.family = site.family;
		this.local = new SiteHost(site.local);
	}

	/**
	 * 设置节点类型
	 * 
	 * @param id
	 */
	public void setFamily(int id) {
		if (Site.TOP_SITE <= id && id <= Site.BUILD_SITE) {
			this.family = id;
			return;
		}
		throw new IllegalArgumentException("invalid site type");
	}

	/**
	 * 返回节点类型
	 * 
	 * @return
	 */
	public int getFamily() {
		return this.family;
	}

	public static String translate(int type) {
		switch (type) {
		case Site.TOP_SITE:
			return "TOP";
		case Site.HOME_SITE:
			return "HOME";
		case Site.LIVE_SITE:
			return "LIVE";
		case Site.LOG_SITE:
			return "LOG";
		case Site.WORK_SITE:
			return "WORK";
		case Site.DATA_SITE:
			return "DATA";
		case Site.CALL_SITE:
			return "CALL";
		case Site.BUILD_SITE:
			return "BUILD";
		}
		return "NONE";
	}

	/**
	 * TOP节点
	 * 
	 * @return
	 */
	public boolean isTop() {
		return family == Site.TOP_SITE;
	}

	/**
	 * HOME节点
	 * 
	 * @return
	 */
	public boolean isHome() {
		return family == Site.HOME_SITE;
	}

	/**
	 * SQL LIVE 节点
	 * 
	 * @return
	 */
	public boolean isLive() {
		return family == Site.LIVE_SITE;
	}

	/**
	 * CALL节点
	 * 
	 * @return
	 */
	public boolean isCall() {
		return this.family == Site.CALL_SITE;
	}

	/**
	 * DATA节点
	 * 
	 * @return
	 */
	public boolean isData() {
		return family == Site.DATA_SITE;
	}

	/**
	 * 日志节点
	 * 
	 * @return
	 */
	public boolean isLog() {
		return family == Site.LOG_SITE;
	}

	/**
	 * WORK节点
	 * 
	 * @return
	 */
	public boolean isWork() {
		return family == Site.WORK_SITE;
	}

	/**
	 * BUILD节点
	 * 
	 * @return
	 */
	public boolean isBuild() {
		return family == Site.BUILD_SITE;
	}

	/**
	 * 返回网络地址
	 * 
	 * @return
	 */
	public InetAddress getInetAddress() {
		return this.local.getInetAddress();
	}

	/**
	 * 设置网络地址
	 * 
	 * @param address
	 */
	public void setInetAddress(InetAddress address) {
		this.local.setInetAddress(address);
	}

	/**
	 * 返回TCP端口号
	 * 
	 * @return
	 */
	public int getTCPort() {
		return local.getTCPort();
	}

	/**
	 * 返回UDP端口号
	 * 
	 * @return
	 */
	public int getUDPort() {
		return local.getUDPort();
	}

	/**
	 * 设置节点地址
	 * 
	 * @param ip
	 * @param tcport
	 * @param udport
	 */
	public void setHost(InetAddress ip, int tcport, int udport) {
		local = new SiteHost(ip, tcport, udport);
	}

	/**
	 * 设置节点地址
	 * 
	 * @param host
	 */
	public void setHost(SiteHost host) {
		this.local = new SiteHost(host);
	}

	/**
	 * 返回节点地址
	 * 
	 * @return
	 */
	public SiteHost getHost() {
		return this.local;
	}

	/**
	 * 复制
	 * 
	 * @param site
	 */
	public void set(Site site) {
		this.family = site.family;
		this.local = new SiteHost(site.local);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof Site)) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((Site) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return local.hashCode();
	}

	/*
	 * 比较排序
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Site site) {
		int ret = (family == site.family ? 0 : (family < site.family ? -1 : 1));
		if (ret == 0) {
			ret = local.compareTo(site.local);
		}
		return ret;
	}
}
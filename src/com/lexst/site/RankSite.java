/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com All rights reserved
 * 
 * rank site, lexst basic class
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

import java.net.*;

/**
 * 
 * 分级节点定义，适用于DATA节点和BUILD节点
 *
 */
public class RankSite extends Site {
	
	private static final long serialVersionUID = 9134748576787989029L;

	/** 节点级别：主节点和从节点 */
	public final static byte PRIME_SITE = 1;

	public final static byte SLAVE_SITE = 2;

	/** 所属节点级别 */
	private byte rank;

	/**
	 * 初始化分级节点
	 */
	protected RankSite() {
		super();
		rank = 0;
	}

	/**
	 * @param family
	 */
	protected RankSite(int family) {
		super(family);
		rank = 0;
	}

	/**
	 * @param family
	 * @param address
	 * @param tcport
	 * @param udport
	 */
	protected RankSite(int family, InetAddress address, int tcport, int udport) {
		super(family, address, tcport, udport);
		rank = 0;
	}

	/**
	 * @param family
	 * @param strIP
	 * @param tcport
	 * @param udport
	 */
	protected RankSite(int family, String strIP, int tcport, int udport) throws UnknownHostException {
		this(family, InetAddress.getByName(strIP), tcport, udport);
	}

	/**
	 * @param site
	 */
	public RankSite(Site site) {
		super(site);
		rank = 0;
	}

	/**
	 * 设置节点级别(主节点/从节点)
	 * @param i
	 */
	public void setRank(byte i) {
		if (i != RankSite.PRIME_SITE && i != RankSite.SLAVE_SITE) {
			throw new IllegalArgumentException("invalid site rank");
		}
		this.rank = i;
	}

	/**
	 * 返回节点级别
	 * @return
	 */
	public byte getRank() {
		return this.rank;
	}

	/**
	 * 判断是不是主节点
	 * @return
	 */
	public boolean isPrime() {
		return rank == RankSite.PRIME_SITE;
	}

	/**
	 * 判断是不是从节点
	 * @return
	 */
	public boolean isSlave() {
		return rank == RankSite.SLAVE_SITE;
	}

}
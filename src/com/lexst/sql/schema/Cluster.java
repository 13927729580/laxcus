/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com. All rights reserved
 * 
 * lexst cluster address
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 12/9/2011
 * @see com.lexst.sql.schema
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.schema;

import java.io.*;
import java.util.*;

import com.lexst.util.host.*;

/** 
 * 集群类(cluster)的作用是：TOP建表分配时，根据cluster参数向一个或者多个HOME集群分派建表任务(最少向一个HOME集群分派)。
 * 达到在多个HOME集群下运行一个表的目的。<br><br>
 * 
 * 参数包括两种：<br>
 * <1> 指定建表的HOME集群数量 <br>
 * <2> 指定建表分派到的HOME节点主机地址 <br><br>
 *  
 * 两种参数互斥，二选一。首先检查HOME集群地址集，在无效情况下选择HOME集群数量，HOME集群数最少一个。<br>
 */
public class Cluster implements Serializable, Cloneable {

	private static final long serialVersionUID = -8507306085928065296L;

	/** HOME节点数 */
	private int sites;

	/** HOME节点地址  */
//	private List<String> array = new ArrayList<String>();
	
	private ArrayList<Address> array = new ArrayList<Address>();

	/**
	 * default
	 */
	public Cluster() {
		super();
		sites = 0;
	}
	
	/**
	 * @param sites
	 */
	public Cluster(int sites) {
		this();
		this.setSites(sites);
	}
	
	/**
	 * @param cluster
	 */
	public Cluster(Cluster cluster) {
		this();
		this.set(cluster);
	}

	/**
	 * 复制
	 * @param cluster
	 */
	public void set(Cluster cluster) {
		this.sites = cluster.sites;
		this.array.clear();
		for(Address address : cluster.array) {
			this.add(address);
		}
//		this.array.addAll(clusters.array);
	}

	/**
	 * 指定HOME节点主机数量
	 * @param i
	 */
	public void setSites(int i) {
		this.sites = i;
	}

	/**
	 * 返回HOME节点主机数量
	 * @return
	 */
	public int getSites() {
		return this.sites;
	}

	/**
	 * 保存HOME节点地址
	 * @param address
	 * @return
	 */
	public boolean add(Address address) {
		if(!array.contains(address)) {
			return array.add((Address) address.clone());
		}
		return false;
	}
	
	/**
	 * 删除HOME节点主机地址
	 * @param address
	 * @return
	 */
	public boolean remove(Address address) {
		return this.array.remove(address);
	}
	
	/**
	 * 返回HOME节点主机集合
	 * @return
	 */
	public List<Address> list() {
		return this.array;
	}

	/**
	 * 判断空
	 * @return
	 */
	public boolean isEmpty() {
		return array.isEmpty();
	}

	/**
	 * 地址集合数
	 * @return
	 */
	public int size() {
		return array.size();
	}

	/**
	 * 清除地址集合
	 */
	public void clear() {
		array.clear();
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Cluster(this);
	}
	
//	/**
//	 * 保存主机IP地址
//	 * 
//	 * @param ip
//	 * @return
//	 */
//	public boolean add(String ip) {
//		if (array.contains(ip)) return false;
//		return array.add(ip);
//	}
//
//	/**
//	 * remove a host address
//	 * @param ip
//	 * @return
//	 */
//	public boolean remove(String ip) {
//		return array.remove(ip);
//	}
//
//	public List<String> list() {
//		return array;
//	}

}
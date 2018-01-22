/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * lexst container class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/2/2009
 * 
 * @see com.lexst.pool
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.host;

import java.io.*;
import java.util.*;

/**
 * 服务器绑定的主机地址集合。<br>
 */
public final class SiteSet implements Serializable, Cloneable {

	private static final long serialVersionUID = 467621905930261956L;

	/** 调用下标，从0开始(平均循环调用) */
	private int index;

	/** 站点地址集合 */
	private ArrayList<SiteHost> array = new ArrayList<SiteHost>(5);

	/**
	 * 默认指定
	 */
	public SiteSet() {
		super();
		this.index = 0;
	}

	/**
	 * 创建对象，指定内存空间
	 * 
	 * @param capacity
	 */
	public SiteSet(int capacity) {
		this();
		if (capacity < 5) capacity = 5;
		array.ensureCapacity(capacity);
	}
	
	/**
	 * 初始化并且保存一组主机地址
	 * @param hosts
	 */
	public SiteSet(SiteHost[] hosts) {
		this();
		add(hosts);
	}
	
	/**
	 * 初始化并且保存一组主机地址
	 * @param list
	 */
	public SiteSet(Collection<SiteHost> list) {
		this();
		this.add(list);
		this.trim();
	}
	
	/**
	 * 复制对象
	 * @param set
	 */
	public SiteSet(SiteSet set) {
		this(set.list());
		this.index = set.index;
	}

	/**
	 * 增加一个主机地址
	 * @param host
	 * @return
	 */
	public boolean add(SiteHost host) {
		if(host == null || array.contains(host)) {
			return false;
		}
		return array.add( (SiteHost) host.clone());
	}

	/**
	 * 增加一组主机地址
	 * @param hosts
	 * @return
	 */
	public int add(SiteHost[] hosts) {
		int size = array.size();
		for (int i = 0; hosts != null && i < hosts.length; i++) {
			add(hosts[i]);
		}
		return array.size() - size;
	}
	
	/**
	 * 增加一组主机地址
	 * @param list
	 * @return
	 */
	public int add(Collection<SiteHost> list) {
		int size = array.size();
		for (SiteHost host : list) {
			this.add(host);
		}
		return array.size() - size;
	}

	/**
	 * 删除一个主机地址
	 * @param host
	 * @return
	 */
	public boolean remove(SiteHost host) {
		return this.array.remove(host);
	}
	
	/**
	 * 删除一组主机地址.返回删除的数量
	 * @param list
	 * @return
	 */
	public int remove(Collection<SiteHost> list) {
		int size = array.size();
		for (SiteHost host : list) {
			this.remove(host);
		}
		return size - array.size();
	}

	/**
	 * 检查主机地址是否存在
	 * @param host
	 * @return
	 */
	public boolean exists(SiteHost host) {
		return array.contains(host);
	}
	
	/**
	 * 返回主机地址集合
	 * @return
	 */
	public List<SiteHost> list() {
		return this.array;
	}
	
	/**
	 * 判断内存是否为空
	 * @return
	 */
	public boolean isEmpty() {
		return this.array.isEmpty();
	}
	
	/**
	 * 返回主机地址数量
	 * @return
	 */
	public int size() {
		return this.array.size();
	}

	/**
	 * 将数组空间调整为实际大小(删除多余的空间)
	 */
	public void trim() {
		this.array.trimToSize();
	}
	
	/**
	 * 复制并且返回主机地址数组
	 * @return
	 */
	public SiteHost[] toArray() {
		SiteHost[] s = new SiteHost[array.size()];
		int i = 0;
		for (SiteHost host : array) {
			s[i++] = (SiteHost) host.clone();
		}
		return s;
	}
	
	/**
	 * 循环依次调用每一个主机地址
	 * @return
	 */
	public synchronized SiteHost next() {
		int size = array.size();
		if (size > 0) {
			if (index >= size) index = 0;
			return array.get(index++);
		}
		return null;
	}
	
	/**
	 * 找到指定的主机开始，返回它的下一个主机地址
	 * @param previous
	 * @return
	 */
	public synchronized SiteHost next(SiteHost previous) {
		int size = array.size();
		if (size == 0) return null;
		for (int i = 0; i < size; i++) {
			if (array.get(i) != previous) continue;
			if (i + 1 < size) {
				index = i + 1;
				return array.get(index);
			}
		}
		return array.get(index = 0);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new SiteSet(this);
	}
}
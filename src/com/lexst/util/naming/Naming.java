/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * naming object (string value)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 1/20/2010
 * 
 * @see com.lexst.util.naming
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.naming;

import java.io.*;
import java.util.*;

/**
 * 所有基于名称判断的数据，包括 热发布任务命名，数据库名称等(统一忽略大小写)<br>
 */
public final class Naming implements Serializable, Cloneable, Comparable<Naming> {
	
	private static final long serialVersionUID = 264011252802994522L;
	
	/** 名称(忽略大小写) */
	private String name;

	/** 散列码 */
	private int hash;

	/**
	 * default
	 */
	public Naming() {
		super();
		name = "";
		hash = 0;
	}

	/**
	 * @param name
	 */
	public Naming(String name) {
		this();
		this.set(name);
	}
	
	/**
	 * @param object
	 */
	public Naming(Naming object) {
		this();
		this.set(object.name);
		hash = object.hash;
	}

	/**
	 * 设置名称
	 * @param s
	 */
	public void set(String s) {
		if (s == null) {
			this.name = "";
		} else {
			this.name = new String(s);
		}
	}

	/**
	 * 返回名称
	 * @return
	 */
	public String get() {
		return this.name;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != Naming.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((Naming) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == 0) {
			hash = Arrays.hashCode(name.toLowerCase().getBytes());
		}
		return hash;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Naming(this);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Naming naming) {
		if (naming == null) {
			return 1;
		}
		
		return name.compareToIgnoreCase(naming.name);
	}
}
/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * sql virtual object 
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/5/2009
 * 
 * @see com.lexst.sql.statement
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.statement;

import java.io.*;

/**
 * 所有计算命令的基础类，定义子类命令ID
 * 
 */
public class Compute implements Serializable, Cloneable {

	private static final long serialVersionUID = 580512745851971752L;

	/** 操作命令(标准SQL和非SQL) */
	public final static byte SELECT_METHOD = 1;
	public final static byte DELETE_METHOD = 2;
	public final static byte UPDATE_METHOD = 3;
	public final static byte INSERT_METHOD = 4;
	public final static byte SORT_METHOD = 5;
	public final static byte CONDUCT_METHOD = 6;

	/** SQL语句各操作单元编号 */
	public final static byte SPACE = 1;
	public final static byte CONDITION = 2;
	public final static byte COLUMNIDS = 3; // show columnId
	public final static byte CHUNKS = 4;
	public final static byte ORDERBY = 5;
	public final static byte RANGE = 6;
	public final static byte SNATCH = 7;
	public final static byte SDOTSET = 8;
	public final static byte SHOWSHEET = 9;
	public final static byte GROUPBY = 10;

	/** 操作命令 (select, delete, update, insert, conduct) */
	protected byte method;

	/**
	 * default
	 */
	public Compute() {
		super();
	}

	/**
	 * 定义实现类型
	 * 
	 * @param method
	 */
	public Compute(byte method) {
		this();
		this.setMethod(method);
	}

	/**
	 * @param object
	 */
	public Compute(Compute object) {
		this();
		this.setMethod(object.method);
	}

	/**
	 * 设置操作命令
	 * 
	 * @param b
	 */
	public void setMethod(byte b) {
		this.method = b;
	}

	/**
	 * 返回操作命令
	 * 
	 * @return
	 */
	public byte getMethod() {
		return this.method;
	}
}

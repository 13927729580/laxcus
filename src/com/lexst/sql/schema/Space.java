/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * user table name
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 2/2/2009
 * @see com.lexst.sql.schema
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.schema;

import java.io.*;
import java.util.regex.*;

/**
 * 数据库表名称，由数据库名和数据表名组成，忽略大小写。<br>
 * 数据库名和数据表名长度不能超过64字节。<br>
 * 数据库名和数据库表名由：ASCII字符、ASCII数字、下划线三部分组成。<br>
 */
public final class Space implements Serializable, Cloneable, Comparable<Space> {

	private static final long serialVersionUID = 9050428230331786877L;

	/** 数据表名的正则表达式 */
	private final static String REGEX = "^\\s*(\\w+)\\.(\\w+)\\s*$";

	/** 数据库和数据名长度限制 */
	public final static int MAX_SCHEMA_SIZE = 64;

	public final static int MAX_TABLE_SIZE = 64;

	/** 数据库名 **/
	private String schema;

	/** 数据表名 **/
	private String table;

	/** 哈希码 **/
	private int hash;

	/**
	 * 检查数据库名长度是否有效
	 * 
	 * @param size
	 * @return
	 */
	public static boolean isSchemaSize(int size) {
		return size > 0 && size <= Space.MAX_SCHEMA_SIZE;
	}

	/**
	 * 检查表名长度是否有效
	 * 
	 * @param size
	 * @return
	 */
	public static boolean isTableSize(int size) {
		return size > 0 && size <= Space.MAX_TABLE_SIZE;
	}

	/**
	 * 初始化数据库表名称
	 */
	protected Space() {
		super();
		this.hash = 0;
	}

	/**
	 * 设置数据库表名称
	 * 
	 * @param schema
	 * @param table
	 */
	public Space(String schema, String table) {
		this();
		this.set(schema, table);
	}

	/**
	 * 初始化并且复制数据库表名称
	 * 
	 * @param space
	 */
	public Space(Space space) {
		this();
		this.set(space);
	}

	/**
	 * 初始化解析数据库表名称结构，格式不正确抛出异常
	 * 
	 * @param input
	 * @throws SpaceFormatException
	 */
	public Space(String input) throws SpaceFormatException {
		this();
		this.resolve(input);
	}

	/**
	 * 设置数据库表名称
	 * 
	 * @param schema
	 * @param table
	 */
	public void set(String schema, String table) {
		this.schema = new String(schema);
		this.table = new String(table);
		this.hash = 0;
	}

	/**
	 * 设置数据库表名称
	 * 
	 * @param space
	 */
	public void set(Space space) {
		this.schema = space.schema;
		this.table = space.table;
		this.hash = space.hash;
	}

	/**
	 * 返回数据库名(数据库名称在集合中唯一)
	 * 
	 * @return
	 */
	public String getSchema() {
		return this.schema;
	}

	/**
	 * 返回数据库表名(表名在数据库名下唯一)
	 * 
	 * @return
	 */
	public String getTable() {
		return this.table;
	}

	/**
	 * 检查数据库名是否匹配
	 * 
	 * @param space
	 * @return
	 */
	public boolean matchSchema(Space space) {
		return schema != null && schema.equalsIgnoreCase(space.schema);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != Space.class) {
			return false;
		} else if (arg == this) {
			return true;
		}

		return this.compareTo((Space) arg) == 0;
	}

	/*
	 * 返回哈希码
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		if (this.hash == 0) {
			this.hash = schema.toLowerCase().hashCode() ^ table.toLowerCase().hashCode();
		}
		return this.hash;
	}

	/*
	 * Space的字符串格式
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s.%s", schema, table);
	}

	/*
	 * 复制Space对象
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Space(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Space space) {
		if (space == null) return -1;
		
		int ret = schema.compareToIgnoreCase(space.schema);
		if (ret == 0) {
			ret = table.compareToIgnoreCase(space.table);
		}
		return ret;
	}

	/**
	 * 解析数据库表名称
	 * 
	 * @param input
	 * @throws SpaceFormatException
	 */
	public void resolve(String input) throws SpaceFormatException {
		Pattern pattern = Pattern.compile(Space.REGEX);
		Matcher matcher = pattern.matcher(input);
		if (!matcher.matches()) {
			throw new SpaceFormatException(input);
		}
		this.set(matcher.group(1), matcher.group(2));
	}
}
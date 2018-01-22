/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * element perperty, this is root class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/6/2009
 * 
 * @see com.lexst.sql.column
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.column;

import java.io.*;

import com.lexst.sql.*;

/**
 * 列数据类型基础类。<br>
 *
 */
public abstract class Column implements Serializable, Cloneable, Comparable<Column> {
	
	private static final long serialVersionUID = 6355018286729659843L;

	/** 列标识号 **/
	private short id;

	/** 列值数据类型 **/
	private byte type;

	/** 空状态标记 **/
	private boolean nullable;

	/**
	 * default
	 */
	protected Column() {
		super();
		this.id = 0;
		this.type = 0;
		this.nullable = true;
	}

	/**
	 * clone it
	 * @param column
	 */
	protected Column(Column column) {
		super();
		this.id = column.id;
		this.nullable = column.nullable;
		this.type = column.type;
	}

	/**
	 * @param type
	 */
	public Column(byte type) {
		this();
		this.setType(type);
	}

	/**
	 * @param type
	 * @param id
	 */
	public Column(byte type, short id) {
		this(type);
		this.setId(id);
	}

	/**
	 * 设置列标识号 (1 - 0x8fff)
	 * @param i
	 */
	public void setId(short i) {
		this.id = i;
	}

	/**
	 * 返回列标识号
	 * @return
	 */
	public short getId() {
		return this.id;
	}

	/**
	 * set null status
	 * @param b
	 */
	public void setNull(boolean b) {
		this.nullable = b;
	}
	
	/**
	 * check null status
	 * @return
	 */
	public boolean isNull() {
		return this.nullable;
	}

	/**
	 * set column type
	 * @param b
	 */
	public void setType(byte b) {
		if (Type.RAW <= b && b <= Type.TIMESTAMP) {
			this.type = b;
		} else {
			throw new IllegalArgumentException("invalid type!");
		}
	}

	/**
	 * get column type
	 * @return
	 */
	public byte getType() {
		return this.type;
	}

	/**
	 * check column ttype
	 * @param b
	 * @return
	 */
	public boolean match_type(byte b) {
		return type == b;
	}
	
	/**
	 * build column tag
	 * @return
	 */
	protected byte build_tag() {
		return Type.buildState(isNull(), getType());
	}

	/**
	 * resolve column tag and check it
	 * @param tag
	 * @return
	 */
	protected boolean resolve_tag(byte tag) {
		setNull(Type.isNullable(tag));
		byte state = Type.parseType(tag);
		if (this.type != state) {
			throw new ColumnException("resolve error! not match type! %d - %d",
					type & 0xFF, state & 0xFF);
		}
		return true;
	}

	/**
	 * variable value
	 * 
	 * @return boolean
	 */
	public boolean isVariable() {
		return type == Type.RAW || type == Type.CHAR ||
			type == Type.SCHAR || type == Type.WCHAR;
	}
	
	/**
	 * character value
	 * 
	 * @return
	 */
	public boolean isWord() {
		return type == Type.CHAR || type == Type.SCHAR || type == Type.WCHAR;
	}

	public final boolean isRaw() {
		return this.type == Type.RAW;
	}

	public final boolean isChar() {
		return type == Type.CHAR;
	}

	public final boolean isSChar() {
		return type == Type.SCHAR;
	}

	public final boolean isWChar() {
		return type == Type.WCHAR;
	}

	public final boolean isShort() {
		return type == Type.SHORT;
	}

	public final boolean isInteger() {
		return type == Type.INTEGER;
	}

	public final boolean isLong() {
		return type == Type.LONG;
	}

	public final boolean isFloat() {
		return type == Type.FLOAT;
	}

	public final boolean isDouble() {
		return type == Type.DOUBLE;
	}

	public final boolean isDate() {
		return type == Type.DATE;
	}

	public final boolean isTime() {
		return type == Type.TIME;
	}

	public final boolean isTimestamp() {
		return type == Type.TIMESTAMP;
	}

	/*
	 * 按照列ID排序
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Column column) {
		return id < column.id ? -1 : (id == column.id ? 0 : 1);
	}

	/*
	 * 克隆列
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Column clone() {
		return duplicate();
	}
	
	/**
	 * 按照列的数值排序(数值型按照大小排序，可变长按照字符串排序)
	 * @param column
	 * @return
	 */
	public abstract int compare(Column column);

	/**
	 * 克隆子类实例
	 * 
	 * @return
	 */
	public abstract Column duplicate();
	
	/**
	 * 计算所占内存空间量(以字节为单位)
	 * @return
	 */
	public abstract int capacity();

	/**
	 * 生成数据流，并保存到缓存中
	 * @param buff
	 * @return
	 */
	public abstract int build(ByteArrayOutputStream buff);

	/**
	 * 解析数据流，返回解析的字节长度
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract int resolve(byte[] b, int off, int len);

}
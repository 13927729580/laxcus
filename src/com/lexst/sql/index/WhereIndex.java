/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * index column root class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 6/12/2009
 * 
 * @see com.lexst.sql.index
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;

/**
 * SQL WHERE 被检索参数
 * 
 */
public abstract class WhereIndex implements Serializable {
	private static final long serialVersionUID = 1L;

	/** 检索数据类型，参考Type类 中定义 **/
	protected byte type;

	/**
	 * @param type
	 */
	public WhereIndex(byte type) {
		super();
		this.setType(type);
	}

	/**
	 * @param index
	 */
	public WhereIndex(WhereIndex index) {
		super();
		this.type = index.type;
	}

	/**
	 * 检测类型定义
	 * @param type
	 * @return
	 */
	public boolean isType(byte type) {
		return Type.SHORT_INDEX <= type && type <= Type.SELECT_INDEX;
	}

	/**
	 * 设置索引类型
	 * @param i
	 */
	public void setType(byte i) {
		if (!isType(i)) {
			throw new IllegalArgumentException("invalid index type!");
		}
		this.type = i;
	}

	/**
	 * 返回类型定义
	 * @return
	 */
	public byte getType() {
		return type;
	}

	/**
	 * 列索引类型
	 * 
	 * @return
	 */
	public boolean isColumnType() {
		return Type.SHORT_INDEX <= type && type <= Type.DOUBLE_INDEX;
	}

	/**
	 * 判断嵌套SELECT类型
	 * @return
	 */
	public boolean isSelectType() {
		return type == Type.SELECT_INDEX;
	}

	/**
	 * 短整型类型
	 * @return
	 */
	public boolean isShortType() {
		return type == Type.SHORT_INDEX;
	}

	/**
	 * 整型类型
	 * @return
	 */
	public boolean isIntegerType() {
		return type == Type.INTEGER_INDEX;
	}

	/**
	 * 长整型类型
	 * @return
	 */
	public boolean isLongType() {
		return type == Type.LONG_INDEX;
	}

	/**
	 * 单浮点类型
	 * @return
	 */
	public boolean isFloatType() {
		return type == Type.FLOAT_INDEX;
	}

	/**
	 * 双浮点类型
	 * @return
	 */
	public boolean isDoubleType() {
		return type == Type.DOUBLE_INDEX;
	}

	/**
	 * 返回索引的数据流
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(256);
		build(buff);
		return buff.toByteArray();
	}
	
	/*
	 * 复制对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return duplicate();
	}
	
	/**
	 * 返回列ID
	 * 
	 * @return
	 */
	public abstract short getColumnId();

	/**
	 * 设置列ID
	 * 
	 * @param id
	 */
	public abstract void setColumnId(short id);

	/**
	 * 复制对象
	 * 
	 * @return
	 */
	public abstract WhereIndex duplicate();

	/**
	 * 生成索引的数据流
	 * 
	 * @param buff
	 */
	public abstract void build(ByteArrayOutputStream buff);

	/**
	 * 解析索引数据流，返回解析长度
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract int resolve(byte[] b, int off, int len);
}
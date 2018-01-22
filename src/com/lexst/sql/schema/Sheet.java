/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * Column attribute set 
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/8/2009
 * 
 * @see com.lexst.sql.schema
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.schema;

import java.io.*;

import java.util.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.util.Numeric;

/**
 * 以下标排列的列属性集合，区别与Table的按照列ID排列。下标开始为0
 */
public class Sheet implements Serializable, Cloneable {
	
	private static final long serialVersionUID = 8426075908866159319L;

	/** 列下标位置(不是列ID) -> 列属性  **/
	private Map<Integer, ColumnAttribute> attributes = new TreeMap<Integer, ColumnAttribute>();

	/**
	 * default
	 */
	public Sheet() {
		super();
	}
	
	/**
	 * 复制对象 
	 * @param sheet
	 */
	public Sheet(Sheet sheet) {
		this();
		this.attributes.putAll(sheet.attributes);
	}

	/**
	 * 保存列属性，下标从0开始
	 * 
	 * @param index
	 * @param attribute
	 * @return
	 */
	public boolean add(int index, ColumnAttribute attribute) {
		return attributes.put(index, attribute) == null;
	}

	/**
	 * 根据下标，返回对应的列属性
	 * 
	 * @param index
	 * @return
	 */
	public ColumnAttribute get(int index) {
		return attributes.get(index);
	}
	
	/**
	 * 根据列ID，查找对应的属性
	 * @param columnId
	 * @return
	 */
	public ColumnAttribute find(short columnId) {
		for (ColumnAttribute attribute : attributes.values()) {
			if (attribute.getColumnId() == columnId) {
				return attribute;
			}
		}
		return null;
	}

	/**
	 * 显示全部列属性
	 * 
	 * @return
	 */
	public Collection<ColumnAttribute> values() {
		return this.attributes.values();
	}

	/**
	 * 清除全部列属性
	 */
	public void clear() {
		this.attributes.clear();
	}

	/**
	 * 检查属性集合是否空
	 * @return
	 */
	public boolean isEmpty() {
		return this.attributes.isEmpty();
	}

	/**
	 * 属性集合空间容量
	 * @return
	 */
	public int size() {
		return this.attributes.size();
	}

	/*
	 * 克隆对象
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Sheet(this);
	}
	
	/**
	 * 生成数据流
	 * @return
	 */
	public byte[] build() {
		ArrayList<Integer> a = new ArrayList<Integer>(attributes.keySet());
		Collections.sort(a);
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		for (int offset : a) {
			ColumnAttribute attribute = attributes.get(offset);
			short columnId = attribute.getColumnId();
			String name = attribute.getName();
			byte[] b = Numeric.toBytes(columnId);
			buff.write(b, 0, b.length);
			b = name.getBytes();
			byte sz = (byte) b.length;
			buff.write(sz);
			buff.write(b, 0, b.length);
		}

		byte[] data = buff.toByteArray();

		ByteArrayOutputStream all = new ByteArrayOutputStream();
		short count = (short) attributes.size();
		byte[] b = Numeric.toBytes(count);
		all.write(b, 0, b.length);
		b = Numeric.toBytes(data.length);
		all.write(b, 0, b.length);
		all.write(data, 0, data.length);

		return all.toByteArray();
	}

	/**
	 * 解析Sheet,生成表结构
	 * 
	 * @param table
	 * @param b
	 * @param off
	 * @return
	 */
	public int resolve(Table table, byte[] b, int off) {
		int seek = off;

		short count = Numeric.toShort(b, seek, 2);
		seek += 2;
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;

		int index = 0;
		for (short i = 0; i < count; i++) {
			short columnId = Numeric.toShort(b, seek, 2);
			seek += 2;
			byte len = b[seek];
			seek += 1;
			byte[] byte_name = new byte[len];
			System.arraycopy(b, seek, byte_name, 0, byte_name.length);
			seek += byte_name.length;
			String name = new String(byte_name);
			// 检查列属性是否存在 
			ColumnAttribute attribute = table.find(columnId);
			if (attribute == null || !attribute.getName().equalsIgnoreCase(name)) {
				return -1;
			}
			// save data
			this.add(index, attribute);
			index++;
		}
		if(seek - 6 != size) {
			return -1;
		}
		return seek - off;
	}
}
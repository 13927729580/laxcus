/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.*;
import com.lexst.sql.column.attribute.*;

public class Raw extends Variable {
	private static final long serialVersionUID = 1L;

	/**
	 * default
	 */
	public Raw() {
		super(Type.RAW);
	}

	/**
	 * @param columnId
	 */
	public Raw(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Raw(short columnId, byte[] value) {
		this(columnId);
		super.setValue(value);
	}
	
	/**
	 * @param columnId
	 * @param value
	 * @param index
	 */
	public Raw(short columnId, byte[] value, byte[] index) {
		this(columnId, value);
		super.setIndex(index);
	}

	/**
	 * clone
	 * @param arg
	 */
	public Raw(Raw arg) {
		super(arg);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Raw(this);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != Raw.class) {
			return false;
		} else if (arg == this) {
			return true;
		}
		
		return super.equals((Raw)arg);
	}

	/**
	 * 返回二进制字节数组的字符串格式
	 * 
	 * @param limit - 限制字节长度
	 * @return
	 */
	public String toString(Packing packing, int limit) {
		if (isNull()) return "NULL";

		// 返回解包后的数据流
		byte[] b = super.getValue(packing);
		// 生成16进制字符流
		StringBuilder buff = new StringBuilder("0x");
		for (int i = 0; i < b.length; i++) {
			String s = String.format("%X", b[i] & 0xFF);
			if (s.length() == 1) buff.append('0');
			buff.append(s);
			if (limit > 0 && i + 1 == limit) break;
		}
		return buff.toString();
	}
	
	/**
	 * 返回二进制字节数组的字符串格式
	 * @param packing
	 * @return
	 */
	public String toString(Packing packing) {
		return toString(packing, -1);
	}
}
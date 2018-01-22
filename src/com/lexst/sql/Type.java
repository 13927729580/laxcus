/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * data type define
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/7/2009
 * @see com.lexst.sql
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql;

public class Type {
	
	/**
	 * 存储模型:storage model (适用C和JAVA接口)
	 * NSM: n-array storage model (row，行存储模型)
	 * DSM: depress storage model (column，列存储模型)
	 */
	public static final byte NSM = 1;
	public static final byte DSM = 2;

	/** 列数据类型(适用C和JAVA) **/
	public final static byte RAW = 1;
	public final static byte CHAR = 2;	/* 单字节，UTF8编码 (1 byte) */
	public final static byte VCHAR = 3;	/* LIKE CHAR，UTF8编码 */
	public final static byte SCHAR = 4; /* 双字节，UTF16编码 (2 byte, short character) */
	public final static byte VSCHAR = 5;
	public final static byte WCHAR = 6;	/* 宽字节，UTF32编码 (4 byte, wide character) */
	public final static byte VWCHAR = 7;
	public final static byte SHORT = 8;
	public final static byte INTEGER = 9;
	public final static byte LONG = 10;
	public final static byte FLOAT = 11;
	public final static byte DOUBLE = 12;
	public final static byte DATE = 13;
	public final static byte TIME = 14;
	public final static byte TIMESTAMP = 15;

	/** 列属性的索引KEY值，见 com.lexst.sql.column.attribute **/
	public final static byte NONE_KEY = 0;
	public final static byte PRIME_KEY = 1;
	public final static byte SLAVE_KEY = 2;

	/** SQL WHERE 检索时规定的KEY类型标识，见 com.lexst.sql.index  **/
	public final static byte SHORT_INDEX = 1;
	public final static byte INTEGER_INDEX = 2;
	public final static byte LONG_INDEX = 3;
	public final static byte FLOAT_INDEX = 4;
	public final static byte DOUBLE_INDEX = 5;
	public final static byte SELECT_INDEX = 6;

	/** 数据块状态(未完成和已经封闭状态) */
	public final static byte INCOMPLETE_CHUNK = 1;
	public final static byte COMPLETE_CHUNK = 2;

	/** 数据块级别(主块和从块) **/
	public final static byte PRIME_CHUNK = 1;
	public final static byte SLAVE_CHUNK = 2;

	
	/**
	 * variable value
	 * @return
	 */
	public static boolean isVariable(byte type) {
		return type == Type.RAW || type == Type.CHAR || type == Type.VCHAR
				|| type == Type.SCHAR || type == Type.VSCHAR
				|| type == Type.WCHAR || type == Type.VWCHAR;
	}

	public static boolean isWord(byte type) {
		return type == Type.CHAR || type == Type.VCHAR || type == Type.SCHAR
				|| type == Type.VSCHAR || type == Type.WCHAR
				|| type == Type.VWCHAR;
	}

	public static String showDataType(byte i) {
		String s = "Undefine";
		switch(i) {
		case Type.RAW:
			s = "Raw"; break;
		case Type.CHAR:
			s = "Char"; break;
		case Type.VCHAR:
			s = "RChar"; break;
		case Type.SCHAR:
			s = "NChar"; break;
		case Type.VSCHAR:
			s = "RNChar"; break;
		case Type.WCHAR:
			s = "WChar"; break;
		case Type.VWCHAR:
			s = "RWChar"; break;
		case Type.SHORT:
			s = "Short"; break;
		case Type.INTEGER:
			s = "Integer"; break;
		case Type.LONG:
			s = "Long"; break;
		case Type.FLOAT:
			s = "FLOAT"; break;
		case Type.DOUBLE:
			s = "Double"; break;
		case Type.DATE:
			s = "Date"; break;
		case Type.TIME:
			s = "Time"; break;
		case Type.TIMESTAMP:
			s = "Timestamp"; break;
		}
		return s;
	}

	public static String showIndexType(byte i) {
		switch(i) {
		case Type.PRIME_KEY:
			return "Prime Key";
		case Type.SLAVE_KEY:
			return "Slave Key";
		}
		return "None Index";
	}

	/**
	 * combin nullable and type, this is column state
	 * @param nullable
	 * @param type
	 * @return
	 */
	public static byte buildState(boolean nullable, byte type) {
		byte tag = (byte) (nullable ? 1 : 0);
		tag <<= 6;
		tag |= (byte) (type & 0x3F);
		return tag;
	}

	/**
	 * assert null status
	 * @param tag
	 * @return
	 */
	public static boolean isNullable(byte tag) {
		byte less = (byte) ((tag >>> 6) & 0x3);
		return less == 1;
	}

	/**
	 * resolve column type
	 * @param tag
	 * @return
	 */
	public static byte parseType(byte tag) {
		return (byte) (tag & 0x3F);
	}
}
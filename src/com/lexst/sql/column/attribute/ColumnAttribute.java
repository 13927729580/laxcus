/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * basic class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 4/23/2009
 * 
 * @see com.lexst.sql.column.attribute
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

/**
 * 列属性的基础类，定义所有与列相关的参数。<br>
 * 
 *
 */
public abstract class ColumnAttribute implements Serializable, Cloneable, Comparable<ColumnAttribute> {
	private static final long serialVersionUID = 1L;
	
	/** 列是否允许空值的状态  */
	protected final static byte UNSTATUS = -1;
	protected static final byte ALLOW_NULL = 0;
	protected static final byte NOT_NULL = 1;

	/** 列属性的基本性质: 数据类型、列编号、列名称 */
	private Spike spike = new Spike();
	
	/** 值状态，是否允许空置不定义 ( value status, default is undefine) **/
	private byte nullable;
	
	/** 键类型: 主键，从键，或者未定义(prime key, savle key, none key) **/
	protected byte key;

	/** SQL函数 (在生成默认列时使用)**/
	protected SQLFunction function;

	/**
	 * default constractor
	 */
	protected ColumnAttribute() {
		super();
		key = Type.NONE_KEY;		
		nullable = ColumnAttribute.UNSTATUS;
	}

	/**
	 * @param type
	 */
	protected ColumnAttribute(byte type) {
		this();
		this.setType(type);
	}

	/**
	 * @param type
	 * @param columnId
	 * @param name
	 */
	protected ColumnAttribute(byte type, short columnId, String name) {
		this(type);
		this.setColumnId(columnId);
		this.setName(name);
	}

	/**
	 * @param attribute
	 */
	protected ColumnAttribute(ColumnAttribute attribute) {
		super();
		this.spike.set(attribute.spike);
		this.key = attribute.key;
		this.nullable = attribute.nullable;
		this.function = attribute.function; // new DefaultFunction(param.function);
	}
	
	public Spike getSpike() {
		return this.spike;
	}

	/**
	 * set column identity
	 * @param id
	 */
	public void setColumnId(short id) {
		spike.setColumnId(id);
	}
	
	/**
	 * get column identity
	 * @return short
	 */
	public short getColumnId() {
		return spike.getColumnId();
	}

	/**
	 * set column name
	 * @param s
	 */
	public void setName(String s) {
		this.spike.setName(s);
	}
	
	/**
	 * set column name
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setName(byte[] b, int off, int len) {
		spike.setName(b, off, len);
	}
	
	/**
	 * get column name
	 * @return
	 */
	public String getName() {
		return spike.getName();
	}

	/**
	 * set column type
	 * @param b
	 */
	public void setType(byte b) {
		spike.setType(b);
	}
	
	/**
	 * get column type
	 * @return
	 */
	public byte getType() {
		return spike.getType();
	}

	/**
	 * 可变长字节数组，包括二进制字节和字符
	 * 
	 * @return
	 */
	public boolean isVariable() {
		return (isRaw() || isChar() || isSChar() || isWChar());
	}

	/**
	 * 可变长字符
	 * 
	 * @return
	 */
	public boolean isWord() {
		return (isChar() || isSChar() || isWChar());
	}

	/**
	 * 日期/时间类型
	 * @return
	 */
	public boolean isCalendar() {
		return (isDate() || isTime() || isTimestamp());
	}
	
	/**
	 * 固定长度无小数十进制数
	 * @return
	 */
	public boolean isIntegral() {
		return (isShort() || isInteger() || isLong());
	}
	
	/**
	 * 固定长度有小数十进制数
	 * @return
	 */
	public boolean isDecimal() {
		return (isFloat() || isDouble());
	}
	
	/**
	 * 十进制数字，包括浮点数和整数
	 * @return
	 */
	public boolean isNumber() {
		return (isShort() || isInteger() || isLong() || isFloat() || isDouble());
	}

	/**
	 * is raw type
	 * @return
	 */
	public boolean isRaw() {
		return spike.getType() == Type.RAW;
	}
	
	/**
	 * is char type
	 * 
	 * @return
	 */
	public boolean isChar() {
		return spike.getType() == Type.CHAR;
	}

	/**
	 * is nchar type
	 * 
	 * @return
	 */
	public boolean isSChar() {
		return spike.getType() == Type.SCHAR;
	}

	/**
	 * is wchar type
	 * 
	 * @return
	 */
	public boolean isWChar() {
		return spike.getType() == Type.WCHAR;
	}

	/**
	 * short type (2 bytes)
	 * @return
	 */
	public boolean isShort() {
		return spike.getType() == Type.SHORT;
	}

	/**
	 * integer type (4 bytes)
	 * @return
	 */
	public boolean isInteger() {
		return spike.getType() == Type.INTEGER;
	}

	/**
	 * long type (8 bytes)
	 * @return
	 */
	public boolean isLong() {
		return spike.getType() == Type.LONG;
	}

	/**
	 * float type (4 bytes)
	 * @return
	 */
	public boolean isFloat() {
		return spike.getType() == Type.FLOAT;
	}

	/**
	 * double type (8 bytes)
	 * @return
	 */
	public boolean isDouble() {
		return spike.getType() == Type.DOUBLE;
	}

	/**
	 * date type (4 bytes)
	 * @return
	 */
	public boolean isDate() {
		return spike.getType() == Type.DATE;
	}

	/**
	 * time type (4 bytes)
	 * @return
	 */
	public boolean isTime() {
		return spike.getType() == Type.TIME;
	}

	/**
	 * datetime type (8 bytes)
	 * @return
	 */
	public boolean isTimestamp() {
		return spike.getType() == Type.TIMESTAMP;
	}

	/**
	 * 设置键类型 (主键或者从键)
	 * @param b
	 */
	public void setKey(byte b) {
		this.key = b;
	}
	
	/**
	 * 返回键类型
	 * @return
	 */
	public byte getKey() {
		return this.key;
	}
	
	/**
	 * 主键或者从键
	 * @return
	 */
	public boolean isKey() {
		return this.isPrimeKey() || this.isSlaveKey();
	}

	/**
	 * 主键
	 * 
	 * @return
	 */
	public boolean isPrimeKey() {
		return key == Type.PRIME_KEY;
	}

	/**
	 * 从键
	 * 
	 * @return
	 */
	public boolean isSlaveKey() {
		return key == Type.SLAVE_KEY;
	}

	/**
	 * 此列的KEY未定义
	 * 
	 * @return
	 */
	public boolean isNoneKey() {
		return key == Type.NONE_KEY;
	}
	
	/**
	 * set default function
	 * @param def
	 */
	public void setFunction(SQLFunction def) {
		this.function = def;
	}

	/**
	 * get default function
	 * @return
	 */
	public SQLFunction getFunction() {
		return this.function;
	}

	/**
	 * set null status
	 * @param b
	 * @return
	 */
	public boolean setNull(boolean b) {
		if (b) {
			if (nullable == ColumnAttribute.UNSTATUS || nullable == ColumnAttribute.ALLOW_NULL) {
				nullable = ColumnAttribute.ALLOW_NULL;
				return true;
			}
		} else {
			if (nullable == ColumnAttribute.UNSTATUS || nullable == ColumnAttribute.NOT_NULL) {
				nullable = ColumnAttribute.NOT_NULL;
				return true;
			}
		}
		return false;
	}

	/**
	 * assert is null status
	 * @return
	 */
	public boolean isNullable() {
		return nullable == ColumnAttribute.ALLOW_NULL;
	}
	
	/**
	 * allow set value
	 * @return
	 */
	protected boolean isSetStatus() {
		return nullable == ColumnAttribute.UNSTATUS || nullable == ColumnAttribute.NOT_NULL;
	}

	/**
	 * generate null status identity
	 * @return
	 */
	protected byte buildNullable() {
		return (byte) ((nullable == ColumnAttribute.UNSTATUS || nullable == ColumnAttribute.ALLOW_NULL) ? 1 : 0);
	}

	/**
	 * analyse null status
	 * @param b
	 */
	protected void resolveNullable(byte b) {
		nullable = (b == 1 ? ColumnAttribute.ALLOW_NULL : ColumnAttribute.NOT_NULL);
	}

	/**
	 * 生成默认函数
	 * 
	 * @param buff
	 * @return
	 */
	protected int buildFunction(ByteArrayOutputStream buff) {
		// 0字节长度
		if (function == null) {
			byte[] b = Numeric.toBytes(0);
			buff.write(b, 0, b.length);
			return b.length;
		}

		// 串行化函数和写入字节长度
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		try {
			ObjectOutputStream writer = new ObjectOutputStream(out);
			writer.writeObject(function);
			writer.flush();
		} catch(IOException exp) {
//			throw new ColumnAttributeBuildException(exp);
		}

		byte[] s = out.toByteArray();
		byte[] b = Numeric.toBytes(s.length);
		
		buff.write(b, 0, b.length);
		buff.write(s, 0, s.length);

		return b.length + s.length;
	}
	
	/**
	 * 解析默认函数
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected int resolveFunction(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		// 串行化参数字节长度
		if (seek + 4 > end) {
			throw new ColumnAttributeResolveException("function sizeout!");
		}
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;

		// 串行化函数
		if (size > 0) {
			if (seek + size > end) {
				throw new ColumnAttributeResolveException("function sizeout!");
			}
			try {
				ByteArrayInputStream in = new ByteArrayInputStream(b, seek, size);
				ObjectInputStream reader = new ObjectInputStream(in);
				function = (SQLFunction) reader.readObject();
				seek += size;
			} catch (IOException e) {
				throw new ColumnAttributeResolveException(e);
			} catch (ClassNotFoundException e) {
				throw new ColumnAttributeResolveException(e);
			}
		}
		
		return seek - off;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(ColumnAttribute attribute) {
		return spike.compareTo(attribute.spike);
	}

	/*
	 * 克隆对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return duplicate();
	}

	/**
	 * 克隆子类实例
	 * 
	 * @return
	 */
	public abstract ColumnAttribute duplicate();

	/**
	 * 定义一个默认列
	 * @return
	 */
	public abstract Column getDefault(short columnId);

	/**
	 * exchange column attribute to stream
	 * @return
	 */
	public abstract byte[] build();

	/**
	 * resolve column attribute
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract int resolve(byte[] b, int off, int len);

}
/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;

import com.lexst.util.*;

/**
 * 自定义分布式计算参数值。<br>
 * circulate value
 *
 */
public abstract class CValue implements Serializable, Cloneable {

	private static final long serialVersionUID = 8033820113323670539L;

	/** 分布计算数据类型  **/
	public final static byte BOOLEAN = 1;
	public final static byte RAW = 2;
	public final static byte STRING = 3;
	
	public final static byte SHORT = 4;
	public final static byte INTEGER = 5;
	public final static byte LONG = 5;
	
	public final static byte FLOAT = 6;
	public final static byte DOUBLE = 7;

	public final static byte DATE = 8;
	public final static byte TIME = 9;
	public final static byte TIMESTAMP = 10;
	
	public final static byte OBJECT = 11;

	/** 数据类型 */
	protected byte type;

	/** 参数名称 */
	protected String name;

	/**
	 * default
	 */
	protected CValue() {
		super();
	}

	/**
	 * 
	 * @param type
	 */
	protected CValue(byte type) {
		this();
		this.setType(type);
	}
	
	/**
	 * @param value
	 */
	public CValue(CValue value) {
		this();
		this.setType(value.type);
		this.setName(value.name);
	}

	/**
	 * naming
	 * @param s
	 */
	public void setName(String s) {
		this.name = s;
	}
	
	/**
	 * 
	 * @return String
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return byte
	 */
	public byte getType() {
		return this.type;
	}

	/**
	 * @param b
	 */
	protected void setType(byte b) {
		this.type = b;
	}
	
	public boolean isBoolean() {
		return type == CValue.BOOLEAN;
	}
	
	public boolean isRaw() {
		return type == CValue.RAW;
	}

	public boolean isString() {
		return type == CValue.STRING;
	}

	public boolean isFloat() {
		return type == CValue.FLOAT;
	}

	public boolean isDouble() {
		return type == CValue.DOUBLE;
	}
	
	public boolean isShort() {
		return type == CValue.SHORT;
	}
	
	public boolean isInteger() {
		return type == CValue.INTEGER;
	}

	public boolean isLong() {
		return type == CValue.LONG;
	}
	
	public boolean isDate() {
		return type == CValue.DATE;
	}
	
	public boolean isTime() {
		return type == CValue.TIME;
	}
	
	public boolean isTimestamp() {
		return type == CValue.TIMESTAMP;
	}
	
	public boolean isObject() {
		return type == CValue.OBJECT;
	}
	
	/**
	 * 生成标记
	 * 
	 * @param buff
	 */
	protected void buildTag(ByteArrayOutputStream buff) {
		buff.write(this.type);
		byte[] code = new com.lexst.sql.charset.UTF8().encode(name);
		byte[] b = Numeric.toBytes(code.length);
		buff.write(b, 0, b.length);
		buff.write(code, 0, code.length);
	}

	/**
	 * 解析标记
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected int resolveTag(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		if (seek + 5 > end) {
			throw new IndexOutOfBoundsException("tag indexout");
		}
		type = b[seek];
		seek += 1;
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (seek + size > end) {
			throw new IndexOutOfBoundsException("tag name indexout");
		}
		name = new com.lexst.sql.charset.UTF8().decode(b, seek, size);
		seek += size;

		return seek - off;
	}
	
	/*
	 * 克隆参数
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return duplicate();
	}
	
	/**
	 * 复制
	 * @return
	 */
	public abstract CValue duplicate();
	
	/**
	 * 构造数据流
	 * @return
	 */
	public abstract byte[] build();

	/**
	 * 解析数据流
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract int resolve(byte[] b, int off, int len);

}
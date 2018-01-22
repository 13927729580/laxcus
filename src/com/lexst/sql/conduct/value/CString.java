/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;

import com.lexst.util.*;

/**
 * 字符串变量值
 * 
 */
public class CString extends CValue {

	private static final long serialVersionUID = 1L;

	/** 字符串变量 */
	private String value;

	/**
	 * default
	 */
	public CString() {
		super(CValue.STRING);
	}

	/**
	 * @param param
	 */
	public CString(CString param) {
		super(param);
		this.setValue(param.value);
	}

	/**
	 * @param name
	 * @param value
	 */
	public CString(String name, String value) {
		this();
		super.setName(name);
		this.setValue(value);
	}

	/**
	 * 字符串值
	 * @param s
	 */
	public void setValue(String s) {
		this.value = s;
	}

	public String getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CString(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.statement.dc.CParameter#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		super.buildTag(buff);
		byte[] s = new com.lexst.sql.charset.UTF8().encode(value);
		byte[] b = Numeric.toBytes(s.length);
		buff.write(b, 0, b.length);
		buff.write(s, 0, s.length);
		return buff.toByteArray();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.statement.dc.CParameter#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		int size = super.resolveTag(b, seek, len);
		seek += size;

		if (seek + 4 > end) {
			throw new IndexOutOfBoundsException("string indexout");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;

		if (seek + size > end) {
			throw new IndexOutOfBoundsException("string indexout");
		}
		value = new com.lexst.sql.charset.UTF8().decode(b, seek, size);
		seek += size;

		return seek - off;
	}

}

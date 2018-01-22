/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;

/**
 * 布尔值变量
 * 
 */
public class CBool extends CValue {

	private static final long serialVersionUID = 1L;
	
	private boolean value;

	/**
	 * default
	 */
	public CBool() {
		super(CValue.BOOLEAN);
	}

	/**
	 * @param param
	 */
	public CBool(CBool param) {
		super(param);
		this.value = param.value;
	}

	/**
	 * @param type
	 * @param name
	 */
	public CBool(String name) {
		this();
		super.setName(name);
	}
	
	/**
	 * @param name
	 * @param value
	 */
	public CBool(String name, boolean value) {
		this(name);
		this.setValue(value);
	}
	
	public void setValue(boolean b) {
		this.value = b;
	}

	public boolean getValue() {
		return this.value;
	}

	public boolean isTrue() {
		return this.value;
	}

	public boolean isFalse() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CBool(this);
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
		buff.write((byte) (value ? 1 : 0));
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
		
		int size = super.resolveTag(b, seek, len);
		seek += size;
		if (seek + 1 > off + len) {
			throw new IndexOutOfBoundsException("bool value indexout");
		}
		value = (b[seek] == 1 ? true : false);
		seek += 1;
		
		return seek - off;
	}

}

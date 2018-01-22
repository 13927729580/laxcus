/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;
import java.util.*;

import com.lexst.util.*;

/**
 * @author scott.liang
 * 
 */
public class CRaw extends CValue {

	private static final long serialVersionUID = 1L;
	
	private byte[] value;

	/**
	 * defalut
	 */
	public CRaw() {
		super(CValue.RAW);
	}

	/**
	 * @param param
	 */
	public CRaw(CRaw param) {
		super(param);
		this.setValue(param.value);
	}

	/**
	 * @param name
	 * @param value
	 */
	public CRaw(String name, byte[] value) {
		this();
		super.setName(name);
		this.setValue(value);
	}

	/**
	 * @param b
	 */
	public void setValue(byte[] b) {
		if (b == null) {
			value = null;
		} else {
			value = new byte[b.length];
			System.arraycopy(b, 0, value, 0, b.length);
		}
	}

	/**
	 * @return
	 */
	public byte[] getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CRaw(this);
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
		byte[] b = Numeric.toBytes(value.length);
		buff.write(b, 0, b.length);
		buff.write(value, 0, value.length);
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
			throw new IndexOutOfBoundsException("raw indexout");
		}
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;

		if (seek + size > end) {
			throw new IndexOutOfBoundsException("raw indexout");
		}
		value = Arrays.copyOfRange(b, seek, seek + size);
		seek += size;

		return seek - off;		
	}

}
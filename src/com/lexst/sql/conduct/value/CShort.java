/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;

import com.lexst.util.*;

/**
 * @author scott.liang
 *
 */
public class CShort extends CValue {

	private static final long serialVersionUID = 1L;
	
	private short value;
	
	/**
	 * default
	 */
	public CShort() {
		super(CValue.SHORT);
	}

	/**
	 * @param param
	 */
	public CShort(CShort param) {
		super(param);
		this.value = param.value;
	}

	/**
	 * @param name
	 * @param value
	 */
	public CShort(String name, short value) {
		this();
		this.setName(name);
		this.setValue(value);
	}
	
	/**
	 * 设置参数
	 * @param i
	 */
	public void setValue(short i) {
		this.value = i;
	}
	
	/**
	 * 返回参数
	 * @return
	 */
	public short getValue() {
		return this.value;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CShort(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		super.buildTag(buff);
		byte[] b = Numeric.toBytes(value);
		buff.write(b, 0, b.length);
		return buff.toByteArray();
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		int size = resolveTag(b, seek, len);
		seek += size;
		if (seek + 2 > end) {
			throw new IndexOutOfBoundsException("short indexout");
		}
		value = Numeric.toShort(b, seek, 2);
		seek += 2;

		return seek - off;
	}

}

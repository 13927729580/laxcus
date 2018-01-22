/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;

import com.lexst.util.*;

/**
 * 时间值
 *
 */
public class CTime extends CValue {

	private static final long serialVersionUID = 1L;
	
	private int value;
	
	/**
	 * 
	 */
	public CTime() {
		super(CValue.TIME);
	}

	/**
	 * @param param
	 */
	public CTime(CTime param) {
		super(param);
		this.value = param.value;
	}

	public CTime(String name, int value) {
		this();
		super.setName(name);
		this.setValue(value);
	}
	
	public void setValue(int i) {
		this.value = i;
	}
	public int getValue() {
		return this.value;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CTime(this);
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
		if (seek + 4 > end) {
			throw new IndexOutOfBoundsException("time indexout");
		}
		value = Numeric.toShort(b, seek, 4);
		seek += 4;

		return seek - off;
	}

}

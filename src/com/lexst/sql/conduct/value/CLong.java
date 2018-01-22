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
public class CLong extends CValue {

	private static final long serialVersionUID = 1L;
	
	private long value;
	
	/**
	 * 
	 */
	public CLong() {
		super(CValue.LONG);
	}

	/**
	 * @param param
	 */
	public CLong(CLong param) {
		super(param);
		this.value = param.value;
	}
	
	public CLong(String name, long value) {
		this();
		this.setName(name);
		this.setValue(value);
	}
	
	public void setValue(long i) {
		this.value = i;
	}
	
	public long getValue() {
		return this.value;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CLong(this);
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
		if (seek + 8 > end) {
			throw new IndexOutOfBoundsException("long indexout");
		}
		value = Numeric.toLong(b, seek, 8);
		seek += 8;
		return seek - off;
	}

}

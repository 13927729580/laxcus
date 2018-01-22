/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct.value;

import java.io.*;

import com.lexst.util.*;

/**
 * circulate timestamp
 *
 */
public class CTimestamp extends CValue {

	private static final long serialVersionUID = 1L;
	
	private long value;
	
	/**
	 * default
	 */
	public CTimestamp() {
		super(CValue.TIMESTAMP);
	}

	/**
	 * @param param
	 */
	public CTimestamp(CTimestamp param) {
		super(param);
		this.value = param.value;
	}
	
	public CTimestamp(String name, long value) {
		this();
		super.setName(name);
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
		return new CTimestamp(this);
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
			throw new IndexOutOfBoundsException("timestamp indexout");
		}
		value = Numeric.toShort(b, seek, 8);
		seek += 8;

		return seek - off;
	}

}
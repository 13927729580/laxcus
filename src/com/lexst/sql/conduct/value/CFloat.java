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
public class CFloat extends CValue {

	private static final long serialVersionUID = 1L;
	
	private float value;
	
	/**
	 * default
	 */
	public CFloat() {
		super(CValue.FLOAT);
	}

	/**
	 * @param param
	 */
	public CFloat(CFloat param) {
		super(param);
		this.value = param.value;
	}
	
	/**
	 * @param name
	 * @param value
	 */
	public CFloat(String name, float value) {
		this();
		super.setName(name);
		this.setValue(value);
	}
	
	/**
	 * 设置单浮点值
	 * @param i
	 */
	public void setValue(float i) {
		this.value = i;
	}

	/**
	 * 单浮点值
	 * @return
	 */
	public float getValue() {
		return this.value;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CFloat(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		super.buildTag(buff);
		byte[] b = Numeric.toBytes(Float.floatToIntBits(value));
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
			throw new IndexOutOfBoundsException("float indexout");
		}
		value = java.lang.Float.intBitsToFloat(Numeric.toShort(b, seek, 4));
		seek += 4;

		return seek - off;
	}

}

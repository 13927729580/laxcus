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
public class CDouble extends CValue {

	private static final long serialVersionUID = 1L;
	
	private double value;
	
	/**
	 * 
	 */
	public CDouble() {
		super(CValue.DOUBLE);
	}

	/**
	 * @param param
	 */
	public CDouble(CDouble param) {
		super(param);
		this.value = param.value;
	}
	
	public CDouble(String name, double value) {
		this();
		super.setName(name);
		this.setValue(value);
	}
	
	/**
	 * 设置双浮点值
	 * @param i
	 */
	public void setValue(double i) {
		this.value = i;
	}
	
	/**
	 * 取双浮点值
	 * @return
	 */
	public double getValue() {
		return this.value;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#duplicate()
	 */
	@Override
	public CValue duplicate() {
		return new CDouble(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.dc.CParameter#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		super.buildTag(buff);
		byte[] b = Numeric.toBytes(java.lang.Double.doubleToLongBits(value));
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
			throw new IndexOutOfBoundsException("double indexout");
		}
		value = java.lang.Double.longBitsToDouble(Numeric.toShort(b, seek, 8));
		seek += 8;

		return seek - off;
	}

}

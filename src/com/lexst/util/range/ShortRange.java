/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * short range
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 7/12/2009
 * 
 * @see com.lexst.util.range
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.util.range;

import java.io.Serializable;
import java.math.*;

public class ShortRange implements NumberRange, Serializable, Cloneable, Comparable<ShortRange> {
	
	private static final long serialVersionUID = 3385327664602778339L;
	
	/** SHORT 范围 **/
	private short begin, end;

	/**
	 * default
	 */
	public ShortRange() {
		super();
		begin = end = 0;
	}

	/**
	 * @param begin
	 * @param end
	 */
	public ShortRange(short begin, short end) {
		this();
		this.set(begin, end);
	}

	/**
	 * 
	 * @param range
	 */
	public ShortRange(ShortRange range) {
		this(range.begin, range.end);
	}

	/**
	 * 设置范围下标位置
	 * @param begin
	 * @param end
	 */
	public void set(short begin, short end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid short range!");
		}
		this.begin = begin;
		this.end = end;
	}

	/**
	 * 返回开始位置
	 * @return
	 */
	public short getBegin() {
		return begin;
	}

	/**
	 * 返回结果位置
	 * @return
	 */
	public short getEnd() {
		return end;
	}
	
	/**
	 * 将一组分片，平均划分为多组
	 * 
	 * @param blocks
	 * @return
	 */
	public ShortRange[] split(int blocks) {
		if (this.begin == 0 && this.end == 0) {
			return null;
		}
		// 最小分块是1,<1即出错
		if (blocks < 1) {
			throw new IllegalArgumentException("illegal blocks:" + blocks);
		}

		BigInteger min = BigInteger.valueOf(this.begin);
		BigInteger max = BigInteger.valueOf(this.end);

		BigIntegerRange range = new BigIntegerRange(min, max);
		BigIntegerRange[] result = range.split(blocks);
		ShortRange[] ranges = new ShortRange[result.length];
		for (int i = 0; i < result.length; i++) {
			ranges[i] = new ShortRange(result[i].begin().shortValue(),
					result[i].end().shortValue());
		}
		return ranges;
	}

	/**
	 * 是否包含
	 * @param value
	 * @return
	 */
	public boolean inside(short value) {
		return begin <= value && value <= end;
	}

	/**
	 * 比较两个范围值是否一致
	 * 
	 * @param object
	 * @return boolean
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != ShortRange.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((ShortRange) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return begin ^ end;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%d - %d", this.begin, this.end);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ShortRange(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(ShortRange range) {
		if (range == null) {
			return 1;
		} else if (begin == range.begin) {
			return end < range.end ? -1 : (end == range.end ? 0 : 1);
		}
		return begin < range.begin ? -1 : 1;
	}

}
/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * double range
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

public class DoubleRange implements NumberRange, Serializable, Cloneable, Comparable<DoubleRange> {

	private static final long serialVersionUID = -7767269544311384581L;

	/** 双浮点数范围 **/
	private double begin, end;

	/**
	 * default
	 */
	public DoubleRange() {
		super();
		begin = end = 0.0f;
	}

	/**
	 * @param begin
	 * @param end
	 */
	public DoubleRange(double begin, double end) {
		this();
		this.set(begin, end);
	}

	/**
	 * 
	 * @param range
	 */
	public DoubleRange(DoubleRange range) {
		this(range.begin, range.end);
	}

	/**
	 * 设置范围
	 * 
	 * @param begin
	 * @param end
	 */
	public void set(double begin, double end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid double range!");
		}
		this.begin = begin;
		this.end = end;
	}

	public double getBegin() {
		return begin;
	}

	public double getEnd() {
		return end;
	}

	public boolean inside(double value) {
		return begin <= value && value <= end;
	}

	/*
	 * 比较是否一致
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != DoubleRange.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return compareTo((DoubleRange) object) == 0;
	}

	/*
	 * 字符描述
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%f - %f", this.begin, this.end);
	}

	/*
	 * 散列码
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (Double.doubleToLongBits(begin) ^ Double.doubleToLongBits(end));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new DoubleRange(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DoubleRange range) {
		if (range == null) {
			return 1;
		} else if (begin == range.begin) {
			return end < range.end ? -1 : (end == range.end ? 0 : 1);
		}
		return begin < range.begin ? -1 : 1;
	}

}

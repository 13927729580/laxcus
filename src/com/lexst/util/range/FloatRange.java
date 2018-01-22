/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * float range
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

public class FloatRange implements NumberRange, Serializable, Cloneable, Comparable<FloatRange> {

	private static final long serialVersionUID = -6019819686953817086L;
	
	/** 单浮点值范围 */
	private float begin, end;

	/**
	 * default
	 */
	public FloatRange() {
		super();
		begin = end = 0.0f;
	}

	/**
	 * 单浮点范围
	 * 
	 * @param begin
	 * @param end
	 */
	public FloatRange(float begin, float end) {
		this();
		this.setRange(begin, end);
	}

	/**
	 * 
	 * @param range
	 */
	public FloatRange(FloatRange range) {
		this(range.begin, range.end);
	}

	/**
	 * @param begin
	 * @param end
	 */
	public void setRange(float begin, float end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid float range!");
		}
		this.begin = begin;
		this.end = end;
	}

	public float getBegin() {
		return this.begin;
	}

	public float getEnd() {
		return this.end;
	}

	public boolean inside(float value) {
		return begin <= value && value <= end;
	}

	/**
	 * 比较前后两个结点阵是否一致
	 * 
	 * @param object
	 * @return boolean
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != FloatRange.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((FloatRange) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (Float.floatToIntBits(begin) ^ Float.floatToIntBits(end));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%f - %f", this.begin, this.end);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new FloatRange(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FloatRange range) {
		if (range == null) {
			return 1;
		} else if (begin == range.begin) {
			return end < range.end ? -1 : (end == range.end ? 0 : 1);
		}
		return begin < range.begin ? -1 : 1;
	}

}

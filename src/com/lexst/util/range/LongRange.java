/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * long range
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
import java.math.BigInteger;

public final class LongRange implements NumberRange, Serializable, Cloneable, Comparable<LongRange> {

	private static final long serialVersionUID = -581500664017721280L;
	
	/** 数据范围  **/
	private long begin, end;

	/**
	 * default
	 */
	public LongRange() {
		super();
		begin = end = 0L;
	}

	/**
	 * @param begin
	 * @param end
	 */
	public LongRange(long begin, long end) {
		this();
		this.set(begin, end);
	}

	/**
	 * 
	 * @param range
	 */
	public LongRange(LongRange range) {
		this(range.begin, range.end);
	}

	public long size() {
		return end - begin + 1;
	}

	public void set(long begin, long end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid long range!");
		}
		this.begin = begin;
		this.end = end;
	}

	/**
	 * @param LongRange
	 */
	public void set(LongRange range) {
		if (range != null) {
			set(range.getBegin(), range.getEnd());
		}
	}

	/**
	 * @param blocks
	 * @return LongRange[]
	 */
	public LongRange[] split(int blocks) {
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
		LongRange[] ranges = new LongRange[result.length];
		for (int i = 0; i < result.length; i++) {
			ranges[i] = new LongRange(result[i].begin().longValue(),
					result[i].end().longValue());
		}
		return ranges;
	}

	public boolean inside(long value) {
		return (begin <= value && value <= end);
	}

	public boolean inside(LongRange range) {
		return (begin <= range.begin && range.end <= end);
	}

	/**
	 * 开始点否小于指定的值
	 * 
	 * @return boolean
	 */
	public boolean beginLessBy(long value) {
		return this.begin < value;
	}

	/**
	 * 开始点是否等于指定的值
	 * 
	 * @return boolean
	 */
	public boolean beginEqualsBy(long value) {
		return this.begin == value;
	}

	/**
	 * 开始点是否大于指定的值
	 * 
	 * @return boolean
	 */
	public boolean beginGreatBy(long value) {
		return this.begin > value;
	}

	/**
	 * 结束点是否小于被比较值
	 * 
	 * @return boolean
	 */
	public boolean endLessBy(long value) {
		return this.end < value;
	}

	/**
	 * 是否等于结束点值
	 * 
	 * @return boolean
	 */
	public boolean endEqualsBy(long value) {
		return this.end == value;
	}

	/**
	 * 是否大于结束点值
	 * 
	 * @return boolean
	 */
	public boolean endGreatBy(long value) {
		return this.end > value;
	}

	/**
	 * 判断当前LongRange结尾与另一个LongRange开始是否衔接
	 * 
	 * @param before
	 * @return boolean
	 */
	public boolean isLinkupByAfter(LongRange after) {
		return this.end + 1 == after.getBegin();
	}

	/**
	 * 判断另一个LongRange的结尾与当前LongRange开始是否衔接
	 * 
	 * @param before
	 * @return boolean
	 */
	public boolean isLinkupByBefore(LongRange before) {
		return before.getEnd() + 1 == this.begin;
	}

	/**
	 * 合并两个LongRange对象. 成功返回一个合并后的新对象,不成功,返回NULL
	 * 
	 * @param previous
	 * @param next
	 * @return LongRange
	 */
	public static LongRange incorporate(LongRange after, LongRange before) {
		// 比较两个对象是否衔
		if (!after.isLinkupByAfter(before))
			return null;
		// 组成一个合并后的新对象
		return new LongRange(after.getBegin(), before.getEnd());
	}

	/**
	 * 返回开始点
	 * 
	 * @return long
	 */
	public long getBegin() {
		return this.begin;
	}

	/**
	 * 返回结束点
	 * 
	 * @return long
	 */
	public long getEnd() {
		return this.end;
	}

	public void copy(LongRange range) {
		this.set(range);
	}

	public boolean isValid() {
		return begin != -1 && end != -1 && begin <= end;
	}

	/*
	 * 比较范围是否一致
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != LongRange.class) {
			return false;
		} else if (object == this) {
			return true;
		}

		return this.compareTo((LongRange) object) == 0;
	}

	/*
	 * 散列码
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (begin ^ end);
	}

	/*
	 * 字符串描述
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%d - %d", this.begin, this.end);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new LongRange(this);
	}

	/*
	 * 比较排列
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(LongRange range) {
		if (range == null) {
			return 1;
		} else if (begin == range.begin) {
			return end < range.end ? -1 : (end == range.end ? 0 : 1);
		}
		return begin < range.begin ? -1 : 1;
	}

}
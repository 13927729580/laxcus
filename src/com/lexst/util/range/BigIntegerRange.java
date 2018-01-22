/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * basic range
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

import java.io.*;
import java.math.*;
import java.util.*;

public class BigIntegerRange implements Serializable, Cloneable,
		Comparable<BigIntegerRange> {

	private static final long serialVersionUID = 1L;

	/** 范围 */
	private BigInteger begin, end;

	/**
	 * construct method
	 */
	public BigIntegerRange() {
		super();
	}

	/**
	 * construct method
	 * 
	 * @param begin
	 * @param end
	 */
	public BigIntegerRange(BigInteger begin, BigInteger end) {
		this();
		this.setRange(begin, end);
	}

	/**
	 * construct method
	 * 
	 * @param begin
	 * @param end
	 * @param radix
	 */
	public BigIntegerRange(String begin, String end, int radix) {
		this();
		this.setRange(begin, end, radix);
	}

	/**
	 * @param signum
	 * @param begin
	 * @param end
	 */
	public BigIntegerRange(int signum, byte[] begin, byte[] end) {
		this();
		this.setRange(signum, begin, end);
	}
	
	/**
	 * 复制范围
	 * 
	 * @param range
	 */
	public BigIntegerRange(BigIntegerRange range) {
		this();
		this.setRange(range);
	}

	/**
	 * 返回开始点
	 * 
	 * @return BigInteger
	 */
	public BigInteger begin() {
		return this.begin;
	}

	/**
	 * 返回结束点
	 * 
	 * @return BigInteger
	 */
	public BigInteger end() {
		return this.end;
	}

	/**
	 * 设置范围
	 * 
	 * @param begin
	 * @param end
	 */
	public void setRange(BigInteger begin, BigInteger end) {
		// begin必须小于等于end, 否则认为出错
		if (begin.compareTo(end) > 0) {
			throw new IllegalArgumentException("invalid range!");
		}

		this.begin = new BigInteger(begin.signum(), begin.toByteArray());
		this.end = new BigInteger(end.signum(), end.toByteArray());
	}

	/**
	 * 设置范围
	 * 
	 * @param range
	 */
	public void setRange(BigIntegerRange range) {
		this.setRange(range.begin, range.end);
	}

	/**
	 * 设置范围
	 * 
	 * @param begin
	 * @param end
	 * @param signum
	 */
	public void setRange(int signum, byte[] begin, byte[] end) {
		this.setRange(new BigInteger(signum, begin), new BigInteger(signum, end));
	}

	/**
	 * 设置码范围
	 * 
	 * @param begin
	 * @param end
	 * @param radix
	 */
	public void setRange(String begin, String end, int radix) {
		this.setRange(new BigInteger(begin, radix), new BigInteger(end, radix));
	}

	/**
	 * 返回范围尺寸
	 * @return
	 */
	public BigInteger size() {
		return end.subtract(begin).add(BigInteger.ONE);
	}

	/**
	 * 是否在范围内
	 * 
	 * @return boolean
	 */
	public boolean isInside(BigInteger value) {
		return (begin.compareTo(value) <= 0 && value.compareTo(end) <= 0);
	}

	/**
	 * 是否在范围内
	 * 
	 * @param range
	 * @return
	 */
	public boolean isInside(BigIntegerRange range) {
		return begin.compareTo(range.begin) <= 0 && range.end.compareTo(end) <= 0;
	}

	/**
	 * 按指定的块数分割成多个范围
	 * 
	 * @param blocks
	 * @return
	 */
	public BigIntegerRange[] split(final int blocks) {
		if (this.begin == null || this.end == null) {
			throw new NullPointerException("undefine range!");
		}
		// 最小分块是1，<1即出错
		if (blocks < 1) {
			throw new IllegalArgumentException("illegal blocks define:" + blocks);
		}

		// 存储集
		ArrayList<BigIntegerRange> array = new ArrayList<BigIntegerRange>();
		// 确定一个BLOCK的范围
		BigInteger biBlocks = BigInteger.valueOf(blocks);
		BigInteger sect = end.subtract(begin).add(BigInteger.ONE);
		BigInteger field = sect.divide(biBlocks);
		if (sect.remainder(biBlocks).compareTo(BigInteger.ZERO) != 0) {
			field = field.add(BigInteger.ONE);
		}
		// 分块开始
		BigInteger previous = this.begin;
		while (true) {
			BigInteger next = previous.add(field);
			if (next.compareTo(this.end) >= 0) {
				next = this.end;
				array.add(new BigIntegerRange(previous, next));
				break;
			} else {
				if (next.compareTo(previous) > 0) {
					next = next.subtract(BigInteger.ONE);
				}
				array.add(new BigIntegerRange(previous, next));
				previous = next.add(BigInteger.ONE);
			}
		}

		// 保存数组
		BigIntegerRange[] ranges = new BigIntegerRange[array.size()];
		return array.toArray(ranges);
	}

	/**
	 * @param min
	 * @param max
	 * @param blocks
	 * @return
	 */
	public static BigIntegerRange[] split(BigInteger min, BigInteger max, int blocks) {
		BigInteger count = new BigInteger(String.valueOf(blocks));

		BigInteger size = max.subtract(min).add(BigInteger.ONE); // end - begin + 1

		BigInteger gap = size.divide(count); // size / blocks
		if (BigInteger.ZERO.compareTo(size.remainder(gap)) != 0)
			gap = gap.add(BigInteger.ONE); // size % gap

		List<BigIntegerRange> array = new ArrayList<BigIntegerRange>();
		BigInteger begin = min;
		while (true) {
			BigInteger end = begin.add(gap);
			if (end.compareTo(max) > 0)
				end = max; // end > max

			array.add(new BigIntegerRange(begin, end));
			if (end.compareTo(max) >= 0)
				break; // last >= max
			begin = end.add(BigInteger.ONE);
		}

		BigIntegerRange[] s = new BigIntegerRange[array.size()];
		return array.toArray(s);
	}

	/**
	 * 开始点否小于指定的值
	 * 
	 * @return boolean
	 */
	public boolean beginLessBy(BigInteger value) {
		return begin != null && begin.compareTo(value) < 0;
	}

	/**
	 * 开始点是否等于指定的值
	 * 
	 * @return boolean
	 */
	public boolean beginEqualsBy(BigInteger value) {
		return begin != null && begin.compareTo(value) == 0;
	}

	/**
	 * 开始点是否大于指定的值
	 * 
	 * @return boolean
	 */
	public boolean beginGreatBy(BigInteger value) {
		return begin != null && begin.compareTo(value) > 0;
	}

	/**
	 * 结束点是否小于被比较值
	 * 
	 * @return boolean
	 */
	public boolean endLessBy(BigInteger value) {
		return end != null && end.compareTo(value) < 0;
	}

	/**
	 * 是否等于结束点值
	 * 
	 * @return boolean
	 */
	public boolean endEqualsBy(BigInteger value) {
		return end != null && end.compareTo(value) == 0;
	}

	/**
	 * 是否大于结束点值
	 * 
	 * @return boolean
	 */
	public boolean endGreatBy(BigInteger value) {
		return end != null && end.compareTo(value) > 0;
	}

	/**
	 * 判断当前Range结尾与另一个Range开始是否衔接
	 * 
	 * @param before
	 * @return boolean
	 */
	public boolean isLinkupByAfter(BigIntegerRange after) {
		// if(this.end==null || after==null || after.getBegin()==null) return
		// false;
		return end != null
				&& end.add(BigInteger.ONE).compareTo(after.begin()) == 0;
	}

	/**
	 * 判断另一个Range的结尾与当前Range开始是否衔接
	 * 
	 * @param before
	 * @return boolean
	 */
	public boolean isLinkupByBefore(BigIntegerRange before) {
		// if(begin==null || before==null || before.getEnd()==null) return
		// false;
		return begin != null
				&& before.end().add(BigInteger.ONE).compareTo(this.begin) == 0;
	}

	/**
	 * 合并两个Range对象. 成功返回一个合并后的新对象,不成功,返回NULL
	 * 
	 * @param previous
	 * @param next
	 * @return Range
	 */
	public static BigIntegerRange incorporate(BigIntegerRange after, BigIntegerRange before) {
		// 比较两个对象是否衔
		if (!after.isLinkupByAfter(before)) return null;
		// 组成一个合并后的新对象
		return new BigIntegerRange(after.begin, before.end);
	}

	/**
	 * 判断是否有效
	 * @return
	 */
	public boolean isValid() {
		return begin != null && end != null && begin.compareTo(end) <= 0;
	}

	/**
	 * 复制
	 * 
	 * @param range
	 */
	public void set(BigIntegerRange range) {
		this.setRange(range.begin(), range.end());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(BigIntegerRange range) {
		int ret = begin.compareTo(range.begin);
		if (ret == 0) {
			ret = end.compareTo(range.end);
		}
		return ret;

		// // if(arg==this) return 0;
		// // if(!(arg instanceof Range)) return -1;
		// // Range range = (Range)arg;
		//
		//
		//
		// if (range.getBegin().compareTo(this.getBegin()) == 0) {
		// if (range.getEnd().compareTo(this.getEnd()) == 0)
		// return 0;
		// else if (range.getEnd().compareTo(this.getEnd()) < 1)
		// return 1;
		// else
		// return -1;
		// } else if (range.getBegin().compareTo(this.getBegin()) < 0) {
		// return 1;
		// } else {
		// return -1;
		// }
	}


	/*
	 * 比较前后两个结点阵是否一致
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != BigIntegerRange.class) {
			return false;
		} else if (arg == this) {
			return true;
		}

		return this.compareTo((BigIntegerRange) arg) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (begin != null && end != null) {
			return begin.hashCode() ^ end.hashCode();
		}
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new BigIntegerRange(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s - %s", begin.toString(16), end.toString(16));

		// if (begin == null)
		// return "begin not define!";
		// if (end == null)
		// return "end not define!";
		//
		// String b = begin.toString(16);
		// String e = end.toString(16);
		// if (!b.equals("0") && b.length() % 2 == 1)
		// b = "0" + b;
		// if (!e.equals("0") && e.length() % 2 == 1)
		// e = "0" + e;
		//
		// String str = String.format("%s - %s", b, e);
		// return str;
	}

}
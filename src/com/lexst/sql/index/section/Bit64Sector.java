/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import java.util.*;
import java.util.regex.*;

import com.lexst.util.range.*;

/**
 * BIT64长整型的分片范围
 */
public abstract class Bit64Sector extends ColumnSector {

	private static final long serialVersionUID = -9213242853231466750L;

	/** 正则表达式语法定义 **/
	private final static String SYNTAX = "^\\s*([0-9a-fA-F]{1,16}),([0-9a-fA-F]{1,16})\\s*$";
	
	/** LONG范围集合 **/
	protected Set<LongRange> ranges = new TreeSet<LongRange>();

	/**
	 * default
	 */
	protected Bit64Sector() {
		super();
	}

	public boolean add(LongRange range){
		 return ranges.add(range);
	}
	
	public boolean remove(LongRange range) {
		return ranges.remove(range);
	}
	
	public java.util.Collection<LongRange> list() {
		return this.ranges;
	}
	
	public void clear() {
		this.ranges.clear();
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 */
	protected int indexOf(long value) {
		// 无范围定义是错误
		if (ranges.isEmpty()) return -1;

		// 找到对应的范围下标位置
		int index = 0;
		for (LongRange range : ranges) {
			if (value < range.getBegin() || range.inside(value)) return index;
			index++;
		}
		return index - 1;
	}

//	/**
//	 * cut size, save to memory
//	 * 
//	 * @param begin
//	 * @param end
//	 * @param elements
//	 * @return
//	 */
//	public int splitting(long begin, long end, int elements) {
//		BigInteger min = new BigInteger(String.valueOf(begin));
//		BigInteger max = new BigInteger(String.valueOf(end));
//		BigInteger count = new BigInteger(String.valueOf(elements));
//
//		BigInteger size = max.subtract(min).add(BigInteger.ONE); // end - begin + 1
//		BigInteger gap = size.divide(count);
//		if (BigInteger.ZERO.compareTo(size.remainder(gap)) != 0) gap = gap.add(BigInteger.ONE); // size % gap 
//
//		int pos = ranges.size();
//		BigInteger seek = min;
//		while (true) {
//			BigInteger last = seek.add(gap);
//			if (last.compareTo(max) > 0) last = max;
//			
//			ranges.add(new LongRange(seek.longValue(), last.longValue()));
//
//			if (last.compareTo(max) >= 0) break; // last >= max
//			seek = last.add(BigInteger.ONE);
//		}
//
//		return ranges.size() - pos;
//	}

	/**
	 * 转换成long类型
	 * @param s
	 * @return
	 */
	protected long toLong(String s) {
		long value = 0L;
		int shift = 0;
		int end = s.length();
		while (end > 0) {
			int gap = (end - 2 >= 0 ? 2 : 1);
			String sub = s.substring(end - gap, end);
			long num = java.lang.Long.valueOf(sub, 16) & 0xFF;
			value |= (num << shift);
			shift += 8;
			end -= gap;
		}
		return value;
	}
	
	/*
	 * 解析字符串，返回解析的成员数
	 * @see com.lexst.sql.index.section.ColumnSector#split(java.lang.String)
	 */
	@Override
	protected int split(String s) {
		Pattern pattern = Pattern.compile(Bit64Sector.SYNTAX);
		String[] elements = s.split(";");
		int size = ranges.size();
		for (String element : elements) {
			Matcher matcher = pattern.matcher(element);
			if (!matcher.matches()) {
				throw new IllegalArgumentException("invalid long sector syntax! " + element);
			}
			
			long begin = toLong(matcher.group(1));
			long end = toLong(matcher.group(2));
			ranges.add(new LongRange(begin, end));
		}

		return ranges.size() - size;
	}

	/*
	 * 生成BIT64的数据流
	 * @see com.lexst.sql.index.section.ColumnSector#assemble()
	 */
	@Override
	protected String assemble() {
		StringBuilder buff = new StringBuilder();
		for (LongRange range : ranges) {
			if (buff.length() > 0) buff.append(';');
			buff.append(String.format("%X,%X", range.getBegin(), range.getEnd()));
		}
		return buff.toString();
	}

}
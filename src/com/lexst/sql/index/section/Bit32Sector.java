/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import java.util.*;
import java.util.regex.*;

import com.lexst.util.range.*;

/**
 * BIT32整型的分片范围
 *
 */
public abstract class Bit32Sector extends ColumnSector {

	private static final long serialVersionUID = 894074173965669610L;

	/** 正则表达式语法 **/
	protected final static String SYNTAX = "^\\s*([0-9a-fA-F]{1,8}),([0-9a-fA-F]{1,8})\\s*$";

	/** 范围定义 **/
	protected Set<IntegerRange> ranges = new TreeSet<IntegerRange>();

	/**
	 * default
	 */
	protected Bit32Sector() {
		super();
	}

	/**
	 * @param range
	 * @return
	 */
	public boolean add(IntegerRange range) {
		return ranges.add(range);
	}

	/**
	 * @param begin
	 * @param end
	 * @return
	 */
	public boolean add(int begin, int end) {
		return add(new IntegerRange(begin, end));
	}
	
	public boolean remove(IntegerRange range) {
		return ranges.remove(range);
	}
	
	public java.util.Collection<IntegerRange> list() {
		return this.ranges;
	}
	
	public void clear() {
		this.ranges.clear();
	}
	
	/**
	 * 查找某个值所在的范围
	 * 
	 * @param value
	 * @return
	 */
	public IntegerRange inside(int value) {
		for (IntegerRange range : ranges) {
			if (range.inside(value)) {
				return range;
			}
		}
		return null;
	}

	/**
	 * 
	 * @param value
	 * @return
	 */
	protected int indexOf(int value) {
		// 无范围定义是错误
		if (ranges.isEmpty()) return -1;

		int index = 0;
		for (IntegerRange range : ranges) {
			if (value < range.getBegin() || range.inside(value)) return index;
			index++;
		}
		return index - 1;
	}

	/**
	 * 转换成int类型
	 * @param s
	 * @return
	 */
	protected int toInteger(String s) {
		int value = 0;
		int shift = 0;
		int end = s.length();
		while (end > 0) {
			int gap = (end - 2 >= 0 ? 2 : 1);
			String sub = s.substring(end - gap, end);
			int num = java.lang.Integer.valueOf(sub, 16) & 0xFF;
			value |= (num << shift);
			shift += 8;
			end -= gap;
		}
		return value;
	}
	
	/*
	 * 解析32位的整数范围，返回成员数
	 * @see com.lexst.sql.index.section.ColumnSector#split(java.lang.String)
	 */
	@Override
	protected int split(String s) {
		Pattern pattern = Pattern.compile(Bit32Sector.SYNTAX);
		int size = ranges.size();
		String[] elements = s.split(";");
		for (String element : elements) {
			Matcher matcher = pattern.matcher(element);
			if (!matcher.matches()) {
				throw new IllegalArgumentException("invalid integer sector syntax! " + element);
			}

			int begin = toInteger(matcher.group(1));
			int end = toInteger(matcher.group(2));
			ranges.add(new IntegerRange(begin, end));
		}

		return ranges.size() - size;
	}

	/*
	 * 生成BIT32的数据流
	 * @see com.lexst.sql.index.section.ColumnSector#assemble()
	 */
	@Override
	protected String assemble() {
		StringBuilder buff = new StringBuilder();
		for (IntegerRange range : ranges) {
			if (buff.length() > 0) buff.append(';');
			buff.append(String.format("%X,%X", range.getBegin(), range.getEnd()));
		}
		return buff.toString();
	}

}
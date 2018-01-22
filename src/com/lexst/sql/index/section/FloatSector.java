/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import java.math.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.*;
import com.lexst.util.range.FloatRange;

/**
 * @author scott.liang
 *
 */
public class FloatSector extends ColumnSector {

	private static final long serialVersionUID = 2895972430814827554L;

	/* float syntax */
	private final static String RANGE = "^\\s*([0-9a-fA-F]{1,8}),([0-9a-fA-F]{1,8})\\s*$";

	/* float range set */
	private Set<FloatRange> ranges = new TreeSet<FloatRange>();
	
	/**
	 * default
	 */
	public FloatSector() {
		super();
	}

	public boolean add(FloatRange range){
		 return ranges.add(range);
	}
	
	public boolean remove(FloatRange range) {
		return ranges.remove(range);
	}
	
	public java.util.Collection<FloatRange> list() {
		return this.ranges;
	}

	public void clear() {
		ranges.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "float_section";
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Float.class) {
			throw new ClassCastException("this is not float");
		}
		
		return indexOf((com.lexst.sql.column.Float)column);
	}
	
	/**
	 * @param column
	 * @return
	 */
	public int indexOf(com.lexst.sql.column.Float column) {
		// 没有范围定义是错误
		if (ranges.isEmpty()) return -1;
		// 如果是NULL状态，返回0下标位置
		if (column.isNull()) return 0;

		float value = column.getValue();
		int index = 0;
		for (FloatRange range : ranges) {
			if (value < range.getBegin() || range.inside(value)) return index;
			index++;
		}
		return index - 1;
	}

	/**
	 * 
	 * @param begin
	 * @param end
	 * @param elements
	 * @return
	 */
	public int splitting(float begin, float end, int elements) {
		BigDecimal min = new BigDecimal(String.valueOf(begin));
		BigDecimal max = new BigDecimal(String.valueOf(end));
		BigDecimal count = new BigDecimal(String.valueOf(elements));

		BigDecimal size = max.subtract(min).add(BigDecimal.ONE); // end - begin + 1
		BigDecimal gap = size.divide(count);

		int pos = ranges.size();
		BigDecimal seek = min;
		for (int i = 0; i < elements; i++) {
			BigDecimal last = seek.add(gap);
			if (last.compareTo(max) > 0 || i + 1 == elements) last = max;
			
			FloatRange range = new FloatRange(seek.floatValue(), last.floatValue());
			ranges.add(range);
			
			if (last.compareTo(max) >= 0) break; // last >= max
			seek = last;
		}

		return ranges.size() - pos;
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
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.ColumnSector#split(java.lang.String)
	 */
	protected int split(String s) {
		Pattern pattern = Pattern.compile(FloatSector.RANGE);
		int size = ranges.size();
		String[] elements = s.split(";");
		for (String element : elements) {
			Matcher matcher = pattern.matcher(element);
			if (!matcher.matches()) {
				throw new IllegalArgumentException("invalid long sector syntax! " + element);
			}
			
			float begin = java.lang.Float.intBitsToFloat(toInteger(matcher.group(1)));
			float end = java.lang.Float.intBitsToFloat(toInteger(matcher.group(2)));
			ranges.add(new FloatRange(begin, end));
		}

		return ranges.size() - size;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.ColumnSector#assemble()
	 */
	@Override
	protected String assemble() {
		StringBuilder buff = new StringBuilder();
		for (FloatRange range : ranges) {
			if (buff.length() > 0) buff.append(';');
			int min = java.lang.Float.floatToIntBits(range.getBegin());
			int max = java.lang.Float.floatToIntBits(range.getEnd());
			buff.append(String.format("%X,%X", min, max)); 
		}
		return buff.toString();
	}
}
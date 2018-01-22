/**
 * 
 */
package com.lexst.sql.index.section;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.*;
import com.lexst.util.range.*;

/**
 * 短整型分片记录器
 * 
 */
public class ShortSector extends ColumnSector {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4239363674777396388L;

	/** SHORT范围正则表达式语法定义 **/
	private final static String REGEX = "^\\s*([0-9a-fA-F]{1,4}),([0-9a-fA-F]{1,4})\\s*$";

	/** SHORT范围集合 */
	private Set<ShortRange> ranges = new TreeSet<ShortRange>();

	/**
	 * default
	 */
	public ShortSector() {
		super();
	}

	public boolean add(ShortRange range){
		 return ranges.add(range);
	}
	
	public boolean remove(ShortRange range) {
		return ranges.remove(range);
	}
	
	public java.util.Collection<ShortRange> list() {
		return this.ranges;
	}
	
	public void clear() {
		this.ranges.clear();
	}
	
	public int size() {
		return ranges.size();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "short_section";
	}

	/**
	 * @param column
	 * @return
	 */
	public int indexOf(com.lexst.sql.column.Short column) {
		// 没有范围定义是错误
		if(ranges.isEmpty()) return -1;
		// 如果SHORT列是NULL状态，返回0下标
		if (column.isNull()) return 0;

		// 找到分片范围
		short value = column.getValue();
		int index = 0;
		for (ShortRange range : ranges) {
			if (value < range.getBegin() || range.inside(value)) return index;
			index++;
		}
		return index - 1;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Short.class) {
			throw new ClassCastException("this is not Short");
		}
		return indexOf((com.lexst.sql.column.Short)column);
	}

	/**
	 * 转换成short类型
	 * @param s
	 * @return
	 */
	public short toShort(String s) {
		short value = 0;
		int shift = 0;
		int end = s.length();
		while (end > 0) {
			int gap = (end - 2 >= 0 ? 2 : 1);
			String sub = s.substring(end - gap, end);
			short num = java.lang.Short.valueOf(sub, 16);
			value |= (num << shift);
			shift += 8;
			end -= gap;
		}
		return value;
	}
	
	/*
	 * 解析SHORT范围，返回解析的范围数量
	 * @see com.lexst.sql.index.section.ColumnSector#split(java.lang.String)
	 */
	@Override
	protected int split(String s) {
		Pattern pattern = Pattern.compile(ShortSector.REGEX);

		int size = ranges.size();
		String[] elements = s.split(";");
		for (String element : elements) {
			Matcher matcher = pattern.matcher(element);
			if (!matcher.matches()) {
				throw new IllegalArgumentException("invalid short sector syntax! " + element);
			}

			short begin = this.toShort(matcher.group(1));
			short end = this.toShort(matcher.group(2));
			ranges.add(new ShortRange(begin, end));
		}
		return ranges.size() - size;
	}
	
	/*
	 * 生成Short范围数据流
	 * @see com.lexst.sql.index.section.ColumnSector#assemble()
	 */
	@Override
	protected String assemble() {
		StringBuilder buff = new StringBuilder();
		for (ShortRange range : ranges) {
			if (buff.length() > 0) buff.append(';');
			buff.append(String.format("%X,%X", range.getBegin(), range.getEnd()));
		}
		return buff.toString();
	}
}
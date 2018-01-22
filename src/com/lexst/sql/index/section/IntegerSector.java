/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import com.lexst.sql.column.*;

/**
 * @author scott.liang
 *
 */
public class IntegerSector extends Bit32Sector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4951055397040177527L;

	/**
	 * default
	 */
	public IntegerSector() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "int_section";
	}

	/*
	 * 查找列值所在的区域的下标位置
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Integer.class) {
			throw new ClassCastException("this is not Integer");
		}
		// 如果没有定义，返回-1下标，即无效
		if (ranges.isEmpty()) return -1;
		// 如果是空值，必须在0下标
		if (column.isNull()) return 0;
		return indexOf(((com.lexst.sql.column.Integer) column).getValue());
	}
}
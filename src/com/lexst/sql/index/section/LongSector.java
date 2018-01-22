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
public class LongSector extends Bit64Sector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4018486945756441269L;

	/**
	 * default
	 */
	public LongSector() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "long_section";
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Long.class) {
			throw new ClassCastException("this is not Long");
		}
		if (ranges.isEmpty()) return -1;
		if (column.isNull()) return 0;
		return indexOf(((com.lexst.sql.column.Long) column).getValue());
	}

}
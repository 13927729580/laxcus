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
public class TimestampSector extends Bit64Sector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7578540353415695179L;

	/**
	 * default
	 */
	public TimestampSector() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "timestamp_section";
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Timestamp.class) {
			throw new ClassCastException("this is not timestamp");
		}

		if(ranges.isEmpty()) return -1;
		if(column.isNull()) return 0;
		return indexOf(((com.lexst.sql.column.Timestamp)column).getValue());
	}

}
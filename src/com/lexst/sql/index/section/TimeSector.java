/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import com.lexst.sql.column.*;

/**
 * 时间分片记录器
 *
 */
public class TimeSector extends Bit32Sector {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1612300982775980566L;

	/**
	 * default
	 */
	public TimeSector() {
		super();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "time_section";
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Time.class) {
			throw new ClassCastException("this is not Time");
		}

		if(ranges.isEmpty()) return -1;
		if(column.isNull()) return 0;
		return indexOf(((com.lexst.sql.column.Time)column).getValue());
	}

}
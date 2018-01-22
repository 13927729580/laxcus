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
public class DateSector extends Bit32Sector {

	private static final long serialVersionUID = 5583747581998259941L;

	/**
	 * default
	 */
	public DateSector() {
		super();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "date_section";
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Date.class) {
			throw new ClassCastException("this is not Date");
		}

		if(ranges.isEmpty()) return -1;
		if (column.isNull()) return 0;
		return indexOf(((com.lexst.sql.column.Date)column).getValue());
	}

}
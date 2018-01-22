/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;

/**
 * @author scott.liang
 *
 */
public class SCharSector extends WordSector {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6968614780616929341L;

	/**
	 * default
	 */
	public SCharSector() {
		super(new UTF16());
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "schar_section";
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.SChar.class) {
			throw new ClassCastException("this is not schar");
		}
		if (ranges.isEmpty()) return -1;
		if (column.isNull()) return 0;
		return this.indexOf(((SChar)column).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.Bit32Sector#split(java.lang.String)
	 */
	@Override
	protected int split(String s) {
		return split(new UTF16(), s);
	}
}
/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;

/**
 * WChar数据分片器
 *
 */
public class WCharSector extends WordSector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4953927693910256804L;

	/**
	 * default
	 */
	public WCharSector() {
		super(new UTF32());
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "wchar_section";
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.WChar.class) {
			throw new ClassCastException("this is not wchar");
		}
		if (ranges.isEmpty()) return -1;
		if (column.isNull()) return 0;
		return super.indexOf(((WChar) column).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.Bit32Sector#split(java.lang.String)
	 */
	@Override
	protected int split(String s) {
		return split(new UTF32(), s);
	}
}
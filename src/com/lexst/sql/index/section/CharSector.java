/**
 * 
 */
package com.lexst.sql.index.section;

import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;

/**
 * UTF8编码的字符分割器
 * 
 */
public class CharSector extends WordSector {

	private static final long serialVersionUID = 6797997713178510532L;

	/**
	 * default constractor
	 */
	public CharSector() {
		super(new UTF8());
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnSector#getTag()
	 */
	@Override
	public String getTag() {
		return "char_section";
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.Section#indexOf(com.lexst.sql.column.Column)
	 */
	@Override
	public int indexOf(Column column) {
		if (column == null || column.getClass() != com.lexst.sql.column.Char.class) {
			throw new ClassCastException("this is not char");
		}
		if (ranges.isEmpty()) return -1;
		if (column.isNull()) return 0;
		return this.indexOf(((Char)column).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.Bit32Sector#split(java.lang.String)
	 */
	@Override
	protected int split(String s) {
		return split(new UTF8(), s);
	}
}
/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.sort;

import com.lexst.sql.column.*;

/**
 * 短字符(UTF16)列比较器
 */
public class SCharComparator extends WordComparator {

	/**
	 * default
	 */
	public SCharComparator() {
		super(new com.lexst.sql.charset.UTF16());
	}

	/**
	 * @param columnId
	 */
	public SCharComparator(short columnId) {
		this();
		super.setColumnId(columnId);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnComparator#compare(com.lexst.sql.column.Column, com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column o1, Column o2) {
		return super.compare(((SChar)o1).getValue(), ((SChar)o2).getValue());
	}

}
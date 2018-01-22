/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.sort;

import com.lexst.sql.column.*;

/**
 * 宽字符(UTF32)列比较器
 */
public class WCharComparator extends WordComparator {

	/**
	 * default
	 */
	public WCharComparator() {
		super(new com.lexst.sql.charset.UTF32());
	}

	/**
	 * @param columnId
	 * @param charset
	 */
	public WCharComparator(short columnId) {
		this();
		super.setColumnId(columnId);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnComparator#compare(com.lexst.sql.column.Column, com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column o1, Column o2) {
		return super.compare(((WChar)o1).getValue(), ((WChar)o2).getValue());
	}

}
/**
 * 
 */
package com.lexst.sql.statement.sort;

import com.lexst.sql.column.*;

public class CharComparator extends WordComparator { 

	/**
	 * default
	 */
	public CharComparator() {
		super(new com.lexst.sql.charset.UTF8());
	}

	/**
	 * @param columnId
	 * @param charset
	 */
	public CharComparator(short columnId) {
		this();
		super.setColumnId(columnId);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.order.ColumnComparator#compare(com.lexst.sql.column.Column, com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column o1, Column o2) {
		return super.compare(((Char)o1).getValue(), ((Char)o2).getValue());
	}

}
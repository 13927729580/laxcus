/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function.value;

import com.lexst.sql.column.*;
import com.lexst.sql.function.*;

/**
 * @author scott.liang
 *
 */
public class SQLDate extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private int value;

	/**
	 * @param type
	 */
	public SQLDate() {
		super(SQLValue.DATE);
		value = 0;
	}
	
	/**
	 * @param date
	 */
	public SQLDate(java.util.Date date) {
		this(com.lexst.util.datetime.SimpleDate.format(date));
	}
	
	public SQLDate(int value) {
		this();
		this.setValue(value);
	}
	
	public SQLDate(SQLDate param) {
		this();
		this.value = param.value;
	}
	
	public void setValue(int i) {
		this.value = i;
	}
	
	public int getValue() {
		return this.value;
	}
	
	public java.util.Date getDate() {
		return com.lexst.util.datetime.SimpleDate.format(value);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLDate(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLDate s = (SQLDate)param;
		return value < s.value ? -1 : (value > s.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Date(columnId, value);
	}

}

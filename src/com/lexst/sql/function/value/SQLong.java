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
public class SQLong extends SQLValue {

	private static final long serialVersionUID = 1L;

	private long value;
	
	/**
	 * @param type
	 */
	public SQLong() {
		super(SQLValue.LONG);
		value = 0L;
	}
	
	public SQLong(long value) {
		this();
		this.setValue(value);
	}
	
	public SQLong(SQLong param) {
		this();
		this.setValue(param.value);
	}
	
	public void setValue(long i) {
		this.value = i;
	}
	
	public long getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLong(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLong s = (SQLong)param;
		return value < s.value ? -1 : (value > s.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Long(columnId, value);
	}
}
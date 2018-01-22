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
public class SQLTime extends SQLValue {

	private static final long serialVersionUID = 1L;

	private int value;

	/**
	 * default
	 */
	public SQLTime() {
		super(SQLValue.TIME);
		value = 0;
	}

	/**
	 * @param value
	 */
	public SQLTime(int value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param time
	 */
	public SQLTime(java.util.Date time) {
		this(com.lexst.util.datetime.SimpleTime.format(time));
	}

	public SQLTime(SQLTime param) {
		this();
		this.value = param.value;
	}

	public void setValue(int i) {
		this.value = i;
	}

	public int getValue() {
		return this.value;
	}

	/**
	 * @return
	 */
	public java.util.Date getTime() {
		return com.lexst.util.datetime.SimpleTime.format(value);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLTime(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLTime s = (SQLTime) param;
		return (value < s.value ? -1 : (value > s.value ? 1 : 0));
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Time(columnId, value);
	}

}
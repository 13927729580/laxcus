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
public class SQLTimestamp extends SQLValue {

	private static final long serialVersionUID = 1L;

	private long value;

	/**
	 * default
	 */
	public SQLTimestamp() {
		super(SQLValue.LONG);
		value = 0L;
	}

	/**
	 * @param date
	 */
	public SQLTimestamp(java.util.Date date) {
		this(com.lexst.util.datetime.SimpleTimestamp.format(date));
	}

	/**
	 * @param value
	 */
	public SQLTimestamp(long value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param param
	 */
	public SQLTimestamp(SQLTimestamp param) {
		this();
		this.setValue(param.value);
	}

	public void setValue(long i) {
		this.value = i;
	}

	public long getValue() {
		return this.value;
	}

//	public java.util.Date getTimestamp() {
//		return com.lexst.util.datetime.SimpleTimestamp.format(value);
//	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLTimestamp(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLTimestamp s = (SQLTimestamp)param;
		return value < s.value ? -1 : (value > s.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Timestamp(columnId, value);
	}
}

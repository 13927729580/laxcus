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
public class SQLInteger extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private int value;

	/**
	 * @param type
	 */
	public SQLInteger() {
		super(SQLValue.INTEGER);
		value = 0;
	}
	
	public SQLInteger(int value) {
		this();
		this.setValue(value);
	}
	
	public SQLInteger(SQLInteger param) {
		this();
		this.value = param.value;
	}
	
	public void setValue(int i) {
		this.value = i;
	}
	
	public int getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLInteger(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLInteger s = (SQLInteger)param;
		return value < s.value ? -1 : (value > s.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Integer(columnId, value);
	}
}
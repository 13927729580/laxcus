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
public class SQLDouble extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private double value;

	/**
	 * @param type
	 */
	public SQLDouble() {
		super(SQLValue.DOUBLE);
		value = 0.0f;
	}
	
	/**
	 * @param value
	 */
	public SQLDouble(float value) {
		this();
		this.setValue(value);
	}
	
	/**
	 * @param param
	 */
	public SQLDouble(SQLDouble param) {
		this();
		this.value = param.value;
	}
	
	public void setValue(double i) {
		this.value = i;
	}
	
	public double getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLDouble(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLDouble s = (SQLDouble)param;
		return value < s.value ? -1 : (value > s.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Double(columnId, value);
	}
}
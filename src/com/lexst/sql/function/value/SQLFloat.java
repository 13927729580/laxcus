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
public class SQLFloat extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private float value;

	/**
	 * @param type
	 */
	public SQLFloat() {
		super(SQLValue.FLOAT);
		value = 0.0f;
	}
	
	/**
	 * @param value
	 */
	public SQLFloat(float value) {
		this();
		this.setValue(value);
	}
	
	/**
	 * @param param
	 */
	public SQLFloat(SQLFloat param) {
		this();
		this.value = param.value;
	}
	
	public void setValue(float i) {
		this.value = i;
	}
	
	public float getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLFloat(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLFloat s = (SQLFloat)param;
		return value < s.value ? -1 : (value > s.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Float(columnId, value);
	}
}
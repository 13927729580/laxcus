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
public class SQLShort extends SQLValue {
	
	private static final long serialVersionUID = 1L;
	
	private short value;

	/**
	 * @param type
	 */
	public SQLShort() {
		super(SQLValue.SHORT);
	}
	
	/**
	 * @param value
	 */
	public SQLShort(short value) {
		this();
		this.setValue(value);
	}
	
	/**
	 * @param param
	 */
	public SQLShort(SQLShort param) {
		this();
		this.value = param.value;
	}
	
	public void setValue(short i) {
		this.value = i;
	}
	
	public short getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#duplicate()
	 */
	@Override
	public SQLValue duplicate() {
		return new SQLShort(this);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#compareIt(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public int compareIt(SQLValue param) {
		SQLShort sht = (SQLShort) param;
		return value < sht.value ? -1 : (value > sht.value ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLValue#toColumn(short)
	 */
	@Override
	public Column toColumn(short columnId) {
		return new com.lexst.sql.column.Short(columnId, value);
	}

}
/**
 * 
 */
package com.lexst.sql.column;

import com.lexst.sql.*;
import com.lexst.util.*;

public class Short extends Number {

	private static final long serialVersionUID = 1L;

	private short value;

	/**
	 * default
	 */
	public Short() {
		super(Type.SHORT);
	}

	/**
	 * 
	 * @param columnId
	 */
	public Short(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Short(short columnId, short value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * clone it
	 * @param arg
	 */
	public Short(Short arg) {
		super(arg);
		this.value = arg.value;
	}

	/**
	 * set short
	 * @param i
	 */
	public void setValue(short i) {
		this.value = i;
		this.setNull(false);
	}

	/**
	 * get short
	 * @return
	 */
	public short getValue() {
		return this.value;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#setNumber(byte[], int, int)
	 */
	public void setNumber(byte[] b, int off, int len) {
		if (len != 2) {
			throw new IllegalArgumentException("value size is 8!");
		}
		setValue(Numeric.toShort(b, off, len));
	}

	/*
	 * S(non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#getNumber()
	 */
	public byte[] getNumber() {
		return Numeric.toBytes(value);
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.sql.column.Column#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		if (isNull() && column.isNull()) return 0;
		else if (isNull()) return -1;
		else if (column.isNull()) return 1;

		Short num = (Short) column;
		return (value < num.value ? -1 : (value > num.value ? 1 : 0));
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Short(this);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#capacity()
	 */
	@Override
	public int capacity() {
		if(isNull()) return 1;
		return 3;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != com.lexst.sql.column.Short.class) {
			return false;
		} else if (arg == this) {
			return true;
		}
		
		Short num = (Short) arg;
		if (!num.isNull() && !this.isNull()) {
			return value == num.value;
		}
		return this.isNull() == num.isNull();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		if(isNull()) return 0;
		return value;
	}

	/*
	 * 返回字符串
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (isNull()) return "NULL";
		return String.format("%d", value);
	}
}
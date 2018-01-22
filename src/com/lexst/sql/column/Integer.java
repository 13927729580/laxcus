/**
 * 
 */
package com.lexst.sql.column;

import com.lexst.sql.*;
import com.lexst.util.*;

public class Integer extends Number {

	private static final long serialVersionUID = 1L;

	private int value;

	/**
	 * default constructor
	 */
	public Integer() {
		super(Type.INTEGER);
	}
	
	/**
	 * * @param id
	 */
	public Integer(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Integer(short columnId, int value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * clone
	 * @param arg
	 */
	public Integer(Integer arg) {
		super(arg);
		this.value = arg.value;
	}

	/**
	 * set int value
	 * @param num
	 */
	public void setValue(int num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get int value
	 * @return
	 */
	public int getValue() {
		return this.value;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#setNumber(byte[], int, int)
	 */
	public void setNumber(byte[] b, int off, int len) {
		if (len != 4) {
			throw new IllegalArgumentException("value size is 8!");
		}
		setValue(Numeric.toInteger(b, off, len));
	}

	/*
	 * (non-Javadoc)
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

		Integer num = (Integer)column;
		return (value < num.value ? -1 : (value > num.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Integer(this);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#capacity()
	 */
	@Override
	public int capacity() {
		if(isNull()) return 1;
		return 5;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != com.lexst.sql.column.Integer.class) {
			return false;
		} else if (arg == this) {
			return true;
		}

		Integer num = (Integer) arg;
		if (!num.isNull() && isNull()) {
			return value == num.value;
		}
		return isNull() == num.isNull();
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
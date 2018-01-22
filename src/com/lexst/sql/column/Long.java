/**
 * 
 */
package com.lexst.sql.column;

import com.lexst.sql.*;
import com.lexst.util.*;

public class Long extends Number {

	private static final long serialVersionUID = 1L;

	private long value;

	/**
	 * default
	 */
	public Long() {
		super(Type.LONG);
	}
	
	/**
	 * @param columnId
	 */
	public Long(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Long(short columnId, long value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * clone
	 * @param arg
	 */
	public Long(Long arg) {
		super(arg);
		this.value = arg.value;
	}

	/**
	 * set long value
	 * @param num
	 */
	public void setValue(long num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get long value
	 * @return
	 */
	public long getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#setNumber(byte[], int, int)
	 */
	public void setNumber(byte[] b, int off, int len) {
		if (len != 8) {
			throw new IllegalArgumentException("value size is 8!");
		}
		setValue(Numeric.toLong(b, off, len));
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

		Long num = (Long)column;
		return (value < num.value ? -1 : (value > num.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Long(this);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#capacity()
	 */
	@Override
	public int capacity() {
		if(isNull()) return 1;
		return 9;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != com.lexst.sql.column.Long.class) {
			return false;
		} else if(arg == this) {
			return true;
		}

		Long num = (Long) arg;
		if (!num.isNull() && !isNull()) {
			return value == num.value;
		}
		return num.isNull() == isNull();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		if(isNull()) return 0;
		return (int) (value >>> 32 ^ value);
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
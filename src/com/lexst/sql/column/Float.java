/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.*;
import com.lexst.util.*;

public class Float extends Number {
	private static final long serialVersionUID = 1L;

	private float value;

	/**
	 * default
	 */
	public Float() {
		super(Type.FLOAT);
	}
	
	/**
	 * @param columnId
	 */
	public Float(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Float(short columnId, float value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * clone
	 * @param arg
	 */
	public Float(Float arg) {
		super(arg);
		this.value = arg.value;
	}
	
	/**
	 * set float value
	 * @param num
	 */
	public void setValue(float num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get float value
	 * @return
	 */
	public float getValue() {
		return this.value;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#setNumber(byte[], int, int)
	 */
	public void setNumber(byte[] b, int off, int len) {
		if (len != 4) {
			throw new IllegalArgumentException("value size is 4!");
		}
		int num = Numeric.toInteger(b, off, len);
		setValue(java.lang.Float.intBitsToFloat(num));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#getNumber()
	 */
	public byte[] getNumber() {
		int num = java.lang.Float.floatToIntBits(value);
		return Numeric.toBytes(num);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.column.Column#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		if (isNull() && column.isNull()) return 0;
		else if (isNull()) return -1;
		else if (column.isNull()) return 1;

		Float real = (Float)column;
		return (value < real.value ? -1 : (value > real.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Float(this);
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
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != com.lexst.sql.column.Float.class) {
			return false;
		} else if (arg == this) {
			return true;
		}

		Float num = (Float) arg;
		if (!this.isNull() && !num.isNull()) {
			return value == num.value;
		}
		return num.isNull() == this.isNull();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		if (isNull()) return 0;
		return (int) (value);
	}
	
	/*
	 * 返回字符串
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (isNull()) return "NULL";
		return String.format("%.5f", value);
	}
}
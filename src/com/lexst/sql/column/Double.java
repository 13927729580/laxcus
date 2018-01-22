/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.*;
import com.lexst.util.*;

public class Double extends Number {
	private static final long serialVersionUID = 1L;

	private double value;

	/**
	 * default constractor
	 */
	public Double() {
		super(Type.DOUBLE);
	}
	
	/**
	 * @param columnId
	 */
	public Double(short columnId) {
		this();
		this.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Double(short columnId, double value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * clone
	 * 
	 * @param arg
	 */
	public Double(Double arg) {
		super(arg);
		this.value = arg.value;
	}

	/**
	 * set double
	 * @param num
	 */
	public void setValue(double num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get double
	 * @return
	 */
	public double getValue() {
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
		long num = Numeric.toLong(b, off, len);
		setValue(java.lang.Double.longBitsToDouble(num));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Number#getNumber()
	 */
	public byte[] getNumber() {
		long num = java.lang.Double.doubleToLongBits(value);
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

		Double num = (Double)column;
		return (value < num.value ? -1 : (value > num.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Double(this); 
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
		if (arg == null || arg.getClass() != com.lexst.sql.column.Double.class) {
			return false;
		} else if(arg == this) {
			return true;
		}
		
		Double num = (Double) arg;
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
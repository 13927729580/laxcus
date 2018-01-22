/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * date, standard style: yyyy-MM-dd
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/20/2009
 * 
 * @see com.lexst.sql.column
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.column;

import java.text.*;

import com.lexst.sql.*;
import com.lexst.util.*;
import com.lexst.util.datetime.*;

public class Date extends Number {
	private static final long serialVersionUID = 1L;

	private int value;
	
	/**
	 * default
	 */
	public Date() {
		super(Type.DATE);
	}

	/**
	 * @param columnId
	 */
	public Date(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Date(short columnId, int value) {
		this(columnId);
		this.setValue(value);
	}
	
	/**
	 * clone 
	 * @param date
	 */
	public Date(Date date) {
		super(date);
		value = date.value;
	}
	
	/**
	 * set date value
	 * @param num
	 */
	public void setValue(int num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get date value
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
			throw new IllegalArgumentException("value size is 4!");
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

		Date date = (Date)column;
		return (value < date.value ? -1 : (value > date.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Date(this);
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
		if (arg == null || arg.getClass() != com.lexst.sql.column.Date.class) {
			return false;
		} else if (arg == this) {
			return true;
		}

		Date date = (Date) arg;
		if (!date.isNull() && !isNull()) {
			return value == date.value;
		}
		return date.isNull() == this.isNull();
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
		java.util.Date date = SimpleDate.format(value);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(date);
	}
}
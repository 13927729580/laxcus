/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * time, standard style: hh:mm:ss SSS
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

public class Time extends Number {
	private static final long serialVersionUID = 1L;

	private int value;

	/**
	 * default
	 */
	public Time() {
		super(Type.TIME);
	}
	
	/**
	 * @param columnId
	 */
	public Time(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Time(short columnId, int value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * 保存副本
	 * 
	 * @param arg
	 */
	public Time(Time arg) {
		super(arg);
		this.value = arg.value;
	}
	
	/**
	 * set time value
	 * @param num
	 */
	public void setValue(int num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get time value
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

		Time num = (Time)column;
		return (value < num.value ? -1 : (value > num.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Time(this);
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
		if (arg == null || arg.getClass() != com.lexst.sql.column.Time.class) {
			return false;
		} else if (arg == this) {
			return true;
		}
		
		Time time = (Time) arg;
		if (!time.isNull() && !isNull()) {
			return value == time.value;
		}
		return isNull() == time.isNull();
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
		java.util.Date date = SimpleTime.format(value);
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss SSS");
		return sdf.format(date);
	}
}
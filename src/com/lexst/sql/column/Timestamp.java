/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * datetime, standard style: yyyy-MM-dd hh:mm:ss SSS
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

public class Timestamp extends Number {
	private static final long serialVersionUID = 1L;

	private long value;

	/**
	 * default
	 */
	public Timestamp() {
		super(Type.TIMESTAMP);
	}
	
	/**
	 * @param columnId
	 */
	public Timestamp(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Timestamp(short columnId, long value) {
		this(columnId);
		this.setValue(value);
	}

	/**
	 * clone
	 * @param arg
	 */
	public Timestamp(Timestamp arg) {
		super(arg);
		this.value = arg.value;
	}

	/**
	 * set timestamp value
	 * @param num
	 */
	public void setValue(long num) {
		this.value = num;
		this.setNull(false);
	}

	/**
	 * get timestamp value
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

		Timestamp stamp = (Timestamp)column;
		return (value < stamp.value ? -1 : (value > stamp.value ? 1 : 0));
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Timestamp(this);
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
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != com.lexst.sql.column.Timestamp.class) {
			return false;
		} else if (arg == this) {
			return true;
		}
		
		Timestamp stamp = (Timestamp) arg;
		if (!stamp.isNull() && !isNull()) {
			return value == stamp.value;
		}
		return isNull() == stamp.isNull();
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
		java.util.Date date = SimpleTimestamp.format(value);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		return sdf.format(date);
	}
}
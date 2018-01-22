/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

public class DoubleAttribute extends NumberAttribute {
	// default serial number
	private static final long serialVersionUID = 1L;
	// default value
	private double value;

	/**
	 * default
	 */
	public DoubleAttribute() {
		super(Type.DOUBLE);
		value = 0.0f;
	}

	/**
	 * @param attribute
	 */
	public DoubleAttribute(DoubleAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 * 
	 * @param value
	 */
	public DoubleAttribute(double value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public DoubleAttribute(short columnId, double value) {
		this();
		super.setColumnId(columnId);
		this.setValue(value);
	}
	
	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public DoubleAttribute(short columnId, String name, double value) {
		this(columnId, value);
		this.setName(name);
	}

	/**
	 * set default value
	 * @param num
	 */
	public boolean setValue(double num) {
		if(!isSetStatus()) return false;

		this.value = num;
		this.setNull(false);
		return true;
	}

	/**
	 * get default value
	 * @return
	 */
	public double getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Double column = new com.lexst.sql.column.Double(columnId);
		if (isNullable()) return column;

		if (function != null) {
			column.setValue(SQLFunctionComputer.toDouble(function, null));
		} else {
			column.setValue(value);
		}
		return column;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#build()
	 */
	@Override
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(128);
		super.buildPrefix(buff);
		// defalut value
		long num = Double.doubleToLongBits(value);
		byte[] b = Numeric.toBytes(num);
		buff.write(b, 0, b.length);

		return buff.toByteArray();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		// 解析前缀参数
		int size = super.resolvePrefix(b, seek, end - seek);
		seek += size;
		// 默认值
		if (seek + 8 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,8,%d", seek, end);
		}
		long num = Numeric.toLong(b, seek, 8);
		seek += 8;
		value = Double.longBitsToDouble(num);

		return seek - off;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new DoubleAttribute(this);
	}

}
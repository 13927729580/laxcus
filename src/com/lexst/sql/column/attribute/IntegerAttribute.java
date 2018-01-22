/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

public class IntegerAttribute extends NumberAttribute {

	private static final long serialVersionUID = -10264971394614701L;
	
	// default int value
	private int value;

	/**
	 * default
	 */
	public IntegerAttribute() {
		super(Type.INTEGER);
		this.value = 0; //default
	}
	
	/**
	 * 复制配置，保存副本
	 * @param attribute
	 */
	public IntegerAttribute(IntegerAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 *
	 * @param value (default)
	 */
	public IntegerAttribute(int value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columId
	 * @param value
	 */
	public IntegerAttribute(short columId, int value) {
		this(value);
		super.setColumnId(columId);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public IntegerAttribute(short columnId, String name, int value) {
		this(columnId, value);
		super.setName(name);
	}

	/**
	 * default value
	 * @param num
	 */
	public boolean setValue(int num) {
		if(!isSetStatus()) return false;
		this.value = num;
		this.setNull(false);
		return true;
	}

	/**
	 * get default value
	 * @return
	 */
	public int getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Integer sht = new com.lexst.sql.column.Integer(columnId);
		if (isNullable()) return sht;

		if (function != null) {
			sht.setValue(SQLFunctionComputer.toInteger(function, null));
		} else {
			sht.setValue(value);
		}
		return sht;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#build()
	 */
	@Override
	public byte[] build() {		
		ByteArrayOutputStream buff = new ByteArrayOutputStream(128);
		super.buildPrefix(buff);
		// default value
		byte[] b = Numeric.toBytes(value);
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
		// default value
		if (seek + 4 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,4,%d", seek, end);
		}
		this.value = Numeric.toInteger(b, seek, 4);
		seek += 4;

		return seek - off;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new IntegerAttribute(this);
	}
}
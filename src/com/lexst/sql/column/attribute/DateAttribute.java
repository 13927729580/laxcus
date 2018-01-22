/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

/**
 * @author scott.liang
 *
 */
public class DateAttribute extends NumberAttribute {
	private static final long serialVersionUID = 1L;

	// default value
	private int value;

	/**
	 * default
	 */
	public DateAttribute() {
		super(Type.DATE);
		this.value = 0;
	}

	/**
	 * @param attribute
	 */
	public DateAttribute(DateAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 * @param value (default value)
	 */
	public DateAttribute(int value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public DateAttribute(short columnId, int value) {
		this(value);
		super.setColumnId(columnId);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public DateAttribute(short columnId, String name, int value) {
		this(columnId, value);
		super.setName(name);
	}
	
	/**
	 * set default value
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
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Date column = new com.lexst.sql.column.Date(columnId);
		if (isNullable()) return column;

		if (function != null) {
			column.setValue(SQLFunctionComputer.toDate(function, null));
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
		// default value
		byte[] b = Numeric.toBytes(value);
		buff.write(b, 0, b.length);

		return buff.toByteArray();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 解析前缀参数
		int size = super.resolvePrefix(b, seek, end - seek);
		seek += size;
		// default value
		if(seek + 4 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,4,%d", seek, end);			
		}
		value = Numeric.toInteger(b, seek, 4);
		seek += 4;

		return seek - off;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new DateAttribute(this);
	}
}
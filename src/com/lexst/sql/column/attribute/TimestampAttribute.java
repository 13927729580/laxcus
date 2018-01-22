/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

public class TimestampAttribute extends NumberAttribute {
	// default serial number
	private static final long serialVersionUID = 1L;

	// default value
	private long value;

	/**
	 * default
	 */
	public TimestampAttribute() {
		super(Type.TIMESTAMP);
		value = 0L;
	}

	/**
	 * @param attribute
	 */
	public TimestampAttribute(TimestampAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 * @param value (default)
	 */
	public TimestampAttribute(long value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public TimestampAttribute(short columnId, long value) {
		this(value);
		super.setColumnId(columnId);
	}

	/**
	 * 
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public TimestampAttribute(short columnId, String name, long value) {
		this(columnId, value);
		super.setName(name);
	}

	/**
	 * set default value
	 * @param num
	 */
	public boolean setValue(long num) {
		if(!isSetStatus()) return false;
		this.value = num;
		this.setNull(false);
		return true;
	}

	/**
	 * get default value
	 * @return
	 */
	public long getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Timestamp column = new com.lexst.sql.column.Timestamp(columnId);
		if (isNullable()) return column;

		if (function != null) {
			column.setValue(SQLFunctionComputer.toTimestamp(function, null));
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
		if (seek + 8 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,8,%d", seek, end);
		}
		value = Numeric.toLong(b, seek, 8);
		seek += 8;

		return seek - off;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new TimestampAttribute(this);
	}
}
/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

public class LongAttribute extends NumberAttribute {
	private static final long serialVersionUID = 1L;

	// default value
	private long value;

	/**
	 * default function
	 */
	public LongAttribute() {
		super(Type.LONG);
		this.value = 0L;
	}
	
	/**
	 * @param attribute
	 */
	public LongAttribute(LongAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 * @param name
	 */
	public LongAttribute(String name) {
		this();
		this.setName(name);
	}

	/**
	 * @param value (default)
	 */
	public LongAttribute(long value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public LongAttribute(short columnId, long value) {
		this(value);
		this.setColumnId(columnId);
	}
	
	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public LongAttribute(short columnId, String name, long value) {
		this(columnId, value);
		this.setName(name);
	}

	/**
	 * @param name
	 * @param value (default value)
	 */
	public LongAttribute(String name, long value) {
		this(value);
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
	public Column getDefault(short columnId) {
		com.lexst.sql.column.Long sht = new com.lexst.sql.column.Long(columnId);
		if (isNullable()) return sht;

		if (function != null) {
			sht.setValue(SQLFunctionComputer.toLong(function, null));
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
		// 取前缀数据
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
		// defalut value
		if (seek + 8 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,8,%d", seek, end);
		}
		this.value = Numeric.toLong(b, seek, 8);
		seek += 8;

		return seek - off;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new LongAttribute(this);
	}
}
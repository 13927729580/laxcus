/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;

public class ShortAttribute extends NumberAttribute {
	
	private static final long serialVersionUID = -8158974154761816427L;
	
	// default value
	private short value;

	/**
	 * default
	 */
	public ShortAttribute() {
		super(Type.SHORT);
		value = 0;
	}

	/**
	 * @param attribute
	 */
	public ShortAttribute(ShortAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 * @param value (default)
	 */
	public ShortAttribute(short value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param value (default)
	 */
	public ShortAttribute(short columnId, short value) {
		this(value);
		super.setColumnId(columnId);
	}
	
	/**
	 *
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public ShortAttribute(short columnId, String name, short value) {
		this(columnId, value);
		this.setName(name);
	}

	/**
	 * set default value
	 * @param num
	 */
	public boolean setValue(short num) {
		if (!isSetStatus()) return false;

		this.value = num;
		this.setNull(false);
		return true;
	}

	/**
	 * get default value
	 * @return
	 */
	public short getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Short column = new com.lexst.sql.column.Short(columnId);
		if (isNullable()) return column;
		
		if (function != null) {
			column.setValue(SQLFunctionComputer.toShort(function, null));
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
		// defalut value
		if (seek + 2 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,2,%d", seek, end);
		}
		this.value = Numeric.toShort(b, seek, 2);
		seek += 2;

		return seek - off;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new ShortAttribute(this);
	}
}
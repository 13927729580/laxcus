/**
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.function.*;
import com.lexst.util.*;


public class FloatAttribute extends NumberAttribute {
	// default serial number
	private static final long serialVersionUID = 1L;
	// default value
	private float value;

	/**
	 * default
	 */
	public FloatAttribute() {
		super(Type.FLOAT);
		value = 0.0f;
	}

	/**
	 * @param attribute
	 */
	public FloatAttribute(FloatAttribute attribute) {
		super(attribute);
		this.value = attribute.value;
	}

	/**
	 * @param type
	 */
	public FloatAttribute(float value) {
		this();
		this.setValue(value);
	}
	
	/**
	 * @param columnId
	 * @param value
	 */
	public FloatAttribute(short columnId, float value) {
		this(value);
		super.setColumnId(columnId);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public FloatAttribute(short columnId, String name, float value) {
		this(columnId, value);
		this.setName(name);
	}

	/**
	 * set default float
	 * @param value
	 */
	public boolean setValue(float num) {
		if(!isSetStatus()) return false;
		this.value = num;
		this.setNull(false);
		return true;
	}

	/**
	 * get default float
	 * @return
	 */
	public float getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Float column = new com.lexst.sql.column.Float(columnId);
		if (isNullable()) return column;

		if (function != null) {
			column.setValue(SQLFunctionComputer.toFloat(function, null));
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
		int num = Float.floatToIntBits(value);
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
		if (seek + 4 > end) {
			throw new ColumnAttributeResolveException("float sizeout! %d,4,%d", seek, end);
		}
		int num = Numeric.toInteger(b, seek, 4);
		seek += 4;
		value = Float.intBitsToFloat(num);

		return seek - off;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new FloatAttribute(this);
	}
}
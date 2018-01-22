/**
 *
 */
package com.lexst.sql.column.attribute;

import com.lexst.sql.*;
import com.lexst.sql.function.*;

public class RawAttribute extends VariableAttribute {

	private static final long serialVersionUID = -8675476580713351068L;

	/**
	 * default
	 */
	public RawAttribute() {
		super(Type.RAW);
	}
	
	/**
	 * @param attribute
	 */
	public RawAttribute(RawAttribute attribute) {
		super(attribute);
	}

	/**
	 * @param value
	 */
	public RawAttribute(byte[] value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columnId
	 * @param b
	 */
	public RawAttribute(short columnId, byte[] b) {
		this(b);
		super.setColumnId(columnId);
	}
	
	/**
	 * @param columnId
	 * @param name
	 */
	public RawAttribute(short columnId, String name) {
		this();
		super.setColumnId(columnId);
		super.setName(name);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param b
	 */
	public RawAttribute(short columnId, String name, byte[] b) {
		this(columnId, name);
		setValue(b);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Raw raw = new com.lexst.sql.column.Raw(columnId);
		if (isNullable()) return raw;

		if (function != null) {
			byte[] b = SQLFunctionComputer.toRaw(function, null);
			raw.setValue(b);
		} else {
			raw.setValue(value);
			raw.setIndex(index);
		}
		return raw;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new RawAttribute(this);
	}
}
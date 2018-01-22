/**
 *
 */
package com.lexst.sql.column.attribute;

import com.lexst.sql.*;
import com.lexst.sql.function.*;

public class SCharAttribute extends WordAttribute {
	// serial number
	private static final long serialVersionUID = 1L;

	/**
	 * default
	 */
	public SCharAttribute() {
		super(Type.SCHAR);
	}
	
	/**
	 * @param attribute
	 */
	public SCharAttribute(SCharAttribute attribute) {
		super(attribute);
	}

	/**
	 * @param value
	 */
	public SCharAttribute(byte[] value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public SCharAttribute(short columnId, String name, byte[] value) {
		this();
		super.setColumnId(columnId);
		super.setName(name);
		super.setValue(value);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.property.ColumnProperty#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.SChar column = new com.lexst.sql.column.SChar(columnId);
		if (isNullable()) return column;

		if (function != null) {
			byte[] b = SQLFunctionComputer.toSChar(function, null);
			column.setValue(b);
		} else {
			column.setValue(value);
			column.setIndex(index);
			column.addVWords(vagues);
		}
		return column;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#duplicate()
	 */
	@Override
	public ColumnAttribute duplicate() {
		return new SCharAttribute(this);
	}
}
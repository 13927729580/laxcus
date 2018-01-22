/**
 *
 */
package com.lexst.sql.column.attribute;

import com.lexst.sql.*;
import com.lexst.sql.function.*;

public class WCharAttribute extends WordAttribute {

	private static final long serialVersionUID = 891331381730452889L;

	/**
	 * default
	 */
	public WCharAttribute() {
		super(Type.WCHAR);
	}

	/**
	 * @param attribute
	 */
	public WCharAttribute(WCharAttribute attribute) {
		super(attribute);
	}

	/**
	 *
	 * @param value
	 */
	public WCharAttribute(byte[] value) {
		this();
		super.setValue(value);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public WCharAttribute(short columnId, String name, byte[] value) {
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
		com.lexst.sql.column.WChar column = new com.lexst.sql.column.WChar(columnId);
		if (isNullable()) return column;

		if (function != null) {
			byte[] b = SQLFunctionComputer.toWChar(function, null);
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
		return new WCharAttribute(this);
	}
}
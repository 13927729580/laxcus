/**
 *
 */
package com.lexst.sql.column.attribute;

import com.lexst.sql.*;
import com.lexst.sql.function.*;

public class CharAttribute extends WordAttribute {
	
	private static final long serialVersionUID = -8201893643409903116L;

	/**
	 * default
	 */
	public CharAttribute() {
		super(Type.CHAR);
	}

	/**
	 * @param attribute
	 */
	public CharAttribute(CharAttribute attribute) {
		super(attribute);
	}

	/**
	 * @param columnId
	 * @param name
	 */
	public CharAttribute(short columnId, String name) {
		this();
		this.setColumnId(columnId);
		this.setName(name);
	}

	/**
	 * @param columnId
	 * @param name
	 * @param value
	 */
	public CharAttribute(short columnId, String name, byte[] value) {
		this(columnId, name);
		this.setValue(value);
	}

	/**
	 *
	 * @param value
	 */
	public CharAttribute(byte[] value) {
		this();
		this.setValue(value);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.attribute.ColumnAttribute#getDefault(short)
	 */
	@Override
	public com.lexst.sql.column.Column getDefault(short columnId) {
		com.lexst.sql.column.Char column = new com.lexst.sql.column.Char(columnId);
		// is null status
		if (isNullable()) return column;
		
		if (function != null) {
			byte[] b = SQLFunctionComputer.toChar(function, null);
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
		return new CharAttribute(this);
	}

}
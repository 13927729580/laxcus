/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.Type;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.attribute.*;

public class Char extends Word {

	private static final long serialVersionUID = -3390491452743182642L;

	/**
	 * default constractor
	 */
	public Char() {
		super(Type.CHAR);
	}

	/**
	 * 复制对象
	 * 
	 * @param object
	 */
	public Char(Char object) {
		super(object);
	}

	/**
	 * @param columnId
	 */
	public Char(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public Char(short columnId, byte[] value) {
		this(columnId);
		super.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 * @param index
	 */
	public Char(short columnId, byte[] value, byte[] index) {
		this(columnId, value);
		super.setIndex(index);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new Char(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Variable#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		return super.compare(new UTF8(), column);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != Char.class) {
			return false;
		} else if (arg == this) {
			return true;
		}

		return super.equals((Char) arg);
	}

	/**
	 * 返回字符串
	 * 
	 * @param packing
	 * @return
	 */
	public String toString(Packing packing) {
		return toString(packing, new UTF8(), -1);
	}
}
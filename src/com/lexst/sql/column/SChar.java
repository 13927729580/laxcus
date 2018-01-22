/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.Type;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.attribute.*;

public class SChar extends Word {

	private static final long serialVersionUID = 6086319022936974746L;

	/**
	 * default
	 */
	public SChar() {
		super(Type.SCHAR);
	}

	/**
	 * 完整复制对象
	 * 
	 * @param object
	 */
	public SChar(SChar object) {
		super(object);
	}

	/**
	 * @param columnId
	 */
	public SChar(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public SChar(short columnId, byte[] value) {
		this(columnId);
		super.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 * @param index
	 */
	public SChar(short columnId, byte[] value, byte[] index) {
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
		return new SChar(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.column.Variable#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		return super.compare(new UTF16(), column);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object object) {
		if (object == null || object.getClass() != SChar.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return super.equals((SChar) object);
	}

	/**
	 * 返回字符串
	 * 
	 * @param packing
	 * @return
	 */
	public String toString(Packing packing) {
		return super.toString(packing, new UTF16(), -1);
	}
}
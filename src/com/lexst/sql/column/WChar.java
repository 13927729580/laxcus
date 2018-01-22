/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.Type;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.attribute.*;

public class WChar extends Word {
	
	private static final long serialVersionUID = 3811321563950929598L;

	/**
	 * default
	 */
	public WChar() {
		super(Type.WCHAR);
	}

	/**
	 * 复制全部参数
	 * @param object
	 */
	public WChar(WChar object) {
		super(object);
	}

	/**
	 * @param columnId
	 */
	public WChar(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param value
	 */
	public WChar(short columnId, byte[] value) {
		this(columnId);
		super.setValue(value);
	}

	/**
	 * @param columnId
	 * @param value
	 * @param index
	 */
	public WChar(short columnId, byte[] value, byte[] index){
		this(columnId, value);
		super.setIndex(index);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new WChar(this);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Variable#compare(com.lexst.sql.column.Column)
	 */
	@Override
	public int compare(Column column) {
		return super.compare(new UTF32(), column);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != WChar.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return super.equals((WChar) object);
	}
	
	/**
	 * 返回字符串
	 * @param packing
	 * @return
	 */
	public String toString(Packing packing) {
		return toString(packing, new UTF32(), -1);
	}
}
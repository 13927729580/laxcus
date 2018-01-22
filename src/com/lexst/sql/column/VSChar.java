/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.Type;

public class VSChar extends VWord {
	private static final long serialVersionUID = 1L;

	/**
	 * default
	 */
	public VSChar() {
		super(Type.VSCHAR);
	}
	
	/**
	 * 
	 * @param arg
	 */
	public VSChar(VSChar arg) {
		super(arg);
	}

	/**
	 * @param columnId
	 */
	public VSChar(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param index
	 */
	public VSChar(short columnId, byte[] index) {
		this(columnId);
		this.setIndex(index);
	}

	/**
	 * @param columnId
	 * @param left
	 * @param right
	 * @param index
	 */
	public VSChar(short columnId, short left, short right, byte[] index) {
		this(columnId, index);
		super.setRange(left, right);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new VSChar(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != VSChar.class) {
			return false;
		}
		return super.equals((VSChar) arg);
	}
	
}
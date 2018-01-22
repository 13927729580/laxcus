/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.Type;

public class VChar extends VWord {

	private static final long serialVersionUID = 1L;

	/**
	 * default constructor
	 */
	public VChar() {
		super(Type.VCHAR);
	}
	
	/**
	 * clone
	 * @param arg
	 */
	public VChar(VChar arg) {
		super(arg);
	}

	/**
	 * @param columnId
	 */
	public VChar(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param index
	 */
	public VChar(short columnId, byte[] index) {
		this(columnId);
		this.setIndex(index);
	}

	/**
	 * @param columnId
	 * @param left
	 * @param right
	 * @param index
	 */
	public VChar(short columnId, short left, short right, byte[] index) {
		this(columnId, index);
		this.setRange(left, right);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new VChar(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != VChar.class) {
			return false;
		}
		// checking...
		return super.equals((VChar) arg);
	}

}
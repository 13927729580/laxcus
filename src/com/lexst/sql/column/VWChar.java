/**
 *
 */
package com.lexst.sql.column;

import com.lexst.sql.*;

/**
 * 
 * @author scott.liang
 *
 */
public class VWChar extends VWord {

	private static final long serialVersionUID = 1L;
	
	/**
	 * default
	 */
	public VWChar() {
		super(Type.VWCHAR);
	}
	
	/**
	 * @param arg
	 */
	public VWChar(VWChar arg) {
		super(arg);
	}

	/**
	 * @param columnId
	 */
	public VWChar(short columnId) {
		this();
		super.setId(columnId);
	}

	/**
	 * @param columnId
	 * @param index
	 */
	public VWChar(short columnId, byte[] index) {
		this(columnId);
		this.setIndex(index);
	}

	/**
	 * @param columnId
	 * @param left
	 * @param right
	 * @param index
	 */
	public VWChar(short columnId, short left, short right, byte[] index) {
		this(columnId, index);
		super.setRange(left, right);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.column.Column#duplicate()
	 */
	@Override
	public Column duplicate() {
		return new VWChar(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != VWChar.class) {
			return false;
		}
		return super.equals((VWChar) arg);
	}

}
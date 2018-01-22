/**
 *
 */
package com.lexst.sql.index.range;

import com.lexst.sql.*;

public final class IntegerIndexRange extends IndexRange {

	private static final long serialVersionUID = 2894798468670385258L;

	private int begin, end;

	/**
	 *
	 */
	public IntegerIndexRange() {
		super(Type.INTEGER_INDEX);
		begin = end = 0;
	}

	/**
	 * @param index
	 */
	public IntegerIndexRange(IntegerIndexRange index) {
		super(index);
		this.begin = index.begin;
		this.end = index.end;
	}

	/**
	 * 
	 * @param chunkId
	 * @param columnId
	 */
	public IntegerIndexRange(long chunkId, short columnId) {
		super(Type.INTEGER_INDEX, chunkId, columnId);
		begin = end = 0;
	}

	/**
	 * @param chunkId
	 * @param columnId
	 * @param begin
	 * @param end
	 */
	public IntegerIndexRange(long chunkId, short columnId, int begin, int end) {
		this(chunkId, columnId);
		this.setRange(begin, end);
	}

	public void setRange(int begin, int end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid int range!");
		}
		this.begin = begin;
		this.end = end;
	}

	public int getBegin() {
		return this.begin;
	}

	public int getEnd() {
		return this.end;
	}

	public boolean inside(int value) {
		return begin <= value && value <= end;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new IntegerIndexRange(this);
	}

}

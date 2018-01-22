/**
 *
 */
package com.lexst.sql.index.range;

import com.lexst.sql.*;

public final class FloatIndexRange extends IndexRange {

	private static final long serialVersionUID = 2008279337746646249L;

	private float begin, end;

	/**
	 *
	 */
	public FloatIndexRange() {
		super(Type.FLOAT_INDEX);
		begin = end = 0f;
	}

	/**
	 * @param range
	 */
	public FloatIndexRange(FloatIndexRange range) {
		super(range);
		this.begin = range.begin;
		this.end = range.end;
	}

	/**
	 * 
	 * @param chunkId
	 * @param columnId
	 */
	public FloatIndexRange(long chunkId, short columnId) {
		super(Type.FLOAT_INDEX, chunkId, columnId);
		begin = end = 0f;
	}

	/**
	 * @param chunkId
	 * @param columnId
	 * @param begin
	 * @param end
	 */
	public FloatIndexRange(long chunkId, short columnId, float begin, float end) {
		this(chunkId, columnId);
		this.setRange(begin, end);
	}

	public void setRange(float begin, float end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid float range!");
		}
		this.begin = begin;
		this.end = end;
	}

	public float getBegin() {
		return this.begin;
	}

	public float getEnd() {
		return this.end;
	}

	public boolean inside(float value) {
		return begin <= value && value <= end;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new FloatIndexRange(this);
	}
}
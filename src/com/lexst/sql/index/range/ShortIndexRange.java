/**
 *
 */
package com.lexst.sql.index.range;

import com.lexst.sql.*;

public final class ShortIndexRange extends IndexRange {
	
	private static final long serialVersionUID = -3040280334342752481L;
	private short begin, end;

	/**
	 *
	 */
	public ShortIndexRange() {
		super(Type.SHORT_INDEX);
		begin = end = 0;
	}

	/**
	 * @param index
	 */
	public ShortIndexRange(ShortIndexRange index) {
		super(index);
		this.begin = index.begin;
		this.end = index.end;
	}

	/**
	 * @param chunkId
	 * @param columnId
	 */
	public ShortIndexRange(long chunkId, short columnId) {
		super(Type.SHORT_INDEX, chunkId, columnId);
		begin = end = 0;
	}

	/**
	 * @param chunkId
	 * @param columnId
	 * @param begin
	 * @param end
	 */
	public ShortIndexRange(long chunkId, short columnId, short begin, short end) {
		this(chunkId, columnId);
		this.setRange(begin, end);
	}

	public void setRange(short begin, short end) {
		if (begin > end) {
			throw new IllegalArgumentException("invalid small range!");
		}
		this.begin = begin;
		this.end = end;
	}
	public short getBegin() {
		return this.begin;
	}
	public short getEnd() {
		return this.end;
	}

	public boolean inside(short value) {
		return begin <= value && value <= end;
	}

	/*
	 * 克隆ShortIndexRange
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ShortIndexRange(this);
	}
}
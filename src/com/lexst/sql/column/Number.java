/**
 * 
 */
package com.lexst.sql.column;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.util.*;

public abstract class Number extends Column {

	private static final long serialVersionUID = 1L;

	/**
	 * @param type
	 */
	protected Number(byte type) {
		super(type);
	}

	/**
	 * @param arg
	 */
	protected Number(Number arg) {
		super(arg);
	}

	/*
	 * generate number stream
	 * 
	 * @see com.lexst.sql.column.Column#build(java.io.ByteArrayOutputStream)
	 */
	@Override
	public int build(ByteArrayOutputStream stream) {
		byte tag = build_tag();
		stream.write(tag);

		int size = 1;
		if (!isNull()) {
			byte[] b = getNumber();
			stream.write(b, 0, b.length);
			size += b.length;
		}
		return size;
	}

	/*
	 * resolve number column
	 * 
	 * @see com.lexst.sql.column.Column#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		resolve_tag(b[seek++]);
		if (isNull()) return 1;

		int size = -1;
		switch (getType()) {
		case Type.SHORT:
			size = 2; break;
		case Type.INTEGER:
		case Type.FLOAT:
		case Type.DATE:
		case Type.TIME:
			size = 4; break;
		case Type.LONG:
		case Type.DOUBLE:
		case Type.TIMESTAMP:
			size = 8; break;
		}

		if (size == -1) {
			throw new ColumnException("invalid type: %d", getType());
		}
		if (seek + size > end) {
			throw new SizeOutOfBoundsException("value sizeout!");
		}
		setNumber(b, seek, size);
		seek += size;

		return seek - off;
	}

	/**
	 * set number
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	abstract void setNumber(byte[] b, int off, int len);

	/**
	 * get number stream
	 * 
	 * @return
	 */
	abstract byte[] getNumber();

}
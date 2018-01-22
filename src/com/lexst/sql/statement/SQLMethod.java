/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * sql standard object 
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/5/2009
 * 
 * @see com.lexst.sql.statement
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.statement;

import java.io.*;

import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * 标准SQL语句的基础类
 * 
 */
public class SQLMethod extends Compute {

	private static final long serialVersionUID = -8528043483750503483L;
	
	/** SQL操作表名 */
	protected Space space;

	/**
	 * default
	 */
	protected SQLMethod() {
		super();
	}

	/**
	 * 
	 * @param method
	 */
	public SQLMethod(byte method) {
		super(method);
	}

	/**
	 * 复制对象
	 * 
	 * @param object
	 */
	public SQLMethod(SQLMethod object) {
		super(object);
		this.setSpace(object.space);
	}

	/**
	 * 设置表名
	 * 
	 * @param s
	 */
	public void setSpace(Space s) {
		if (s != null) {
			this.space = new Space(s);
		}
	}

	/**
	 * 返回表名
	 * 
	 * @return
	 */
	public Space getSpace() {
		return this.space;
	}

	protected byte[] buildMethod(byte method, byte[] data) {
		int size = 5 + data.length;
		byte[] b = Numeric.toBytes(size);

		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		out.write(b, 0, b.length);
		out.write(method);
		out.write(data, 0, data.length);
		return out.toByteArray();
	}

	protected byte[] buildField(byte id, byte[] data) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		int size = (data == null ? 0 : data.length);
		// write identity
		out.write(id);
		// write data size
		byte[] b = Numeric.toBytes(size);
		out.write(b, 0, b.length);
		// write data
		if (size > 0) {
			out.write(data, 0, size);
		}
		return out.toByteArray();
	}

	protected Body splitField(byte[] data, int off, int len) {
		int seek = off;
		int end = off + len;

		if (seek + 5 > end) {
			throw new IllegalArgumentException("error field head");
		}
		// read identity
		byte id = data[seek++];
		// read data size
		int size = Numeric.toInteger(data, seek, 4);
		seek += 4;

		if (seek + size > end) {
			throw new IllegalArgumentException("error field");
		}
		// read data
		byte[] bytes = new byte[size];
		System.arraycopy(data, seek, bytes, 0, size);
		seek += size;

		// get body
		Body body = new Body(id, size, bytes);
		body.setLength(seek - off);
		return body;
	}

	protected byte[] buildSpace() {
		// space field
		byte[] db = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		// db size and table size
		buff.write((byte) db.length);
		buff.write((byte) table.length);
		// database and table name
		buff.write(db, 0, db.length);
		buff.write(table, 0, table.length);
		byte[] data = buff.toByteArray();
		return buildField(Compute.SPACE, data);
	}

	protected int splitSpace(byte[] data, int offset, int len) {
		int off = offset;
		// db size and table size
		int db_sz = data[off++] & 0xff;
		int table_sz = data[off++] & 0xff;
		if (!Space.isSchemaSize(db_sz)) {
			throw new IllegalArgumentException("invalid space db size");
		}
		if (!Space.isTableSize(table_sz)) {
			throw new IllegalArgumentException("invalid space table size");
		}
		if (off + db_sz + table_sz > data.length) {
			throw new IllegalArgumentException("invalid space table size");
		}

		// db name
		String db = new String(data, off, db_sz);
		off += db_sz;
		// table name
		String table = new String(data, off, table_sz);
		off += table_sz;
		// set space
		this.setSpace(new Space(db, table));
		return off - offset;
	}

}
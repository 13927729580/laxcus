/**
 *
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.util.*;

public class LongIndex extends ColumnIndex {

	private static final long serialVersionUID = 1L;

	private long value;

	/**
	 * default
	 */
	public LongIndex() {
		super(Type.LONG_INDEX);
		value = 0L;
	}

	/**
	 * 复制LongIndex
	 * @param index
	 */
	public LongIndex(LongIndex index) {
		super(index);
		this.value = index.value;
	}

	/**
	 * @param value
	 */
	public LongIndex(long value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param value
	 * @param column
	 */
	public LongIndex(long value, Column column) {
		this();
		this.setValue(value);
		this.setColumn(column);
	}

	public void setValue(long num) {
		this.value = num;
	}

	public long getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#duplicate()
	 */
	@Override
	public WhereIndex duplicate() {
		return new LongIndex(this);
	}
	
	/*
	 * 输出长整型索引数据流
	 * @see com.lexst.sql.index.WhereIndex#build(java.io.ByteArrayOutputStream)
	 */
	@Override
	public void build(ByteArrayOutputStream buff) {
		// 类型定义
		buff.write(super.getType());
		// 索引值
		byte[] b = Numeric.toBytes(this.value);
		buff.write(b, 0, b.length);
		// 列标识(column identity)
		b = Numeric.toBytes(this.getColumnId());
		buff.write(b, 0, b.length);
		// 列数据(输出数据到缓存)
		this.getColumn().build(buff);
	}
	
	/*
	 * 解析长整形索引数据流
	 * @see com.lexst.sql.index.WhereIndex#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 检查尺寸
		if (seek + 12 > end) {
			throw new SizeOutOfBoundsException("long index sizeout!");
		}
		super.setType(b[seek]);
		seek += 1;
		this.value = Numeric.toInteger(b, seek, 8);
		seek += 8;
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		// 长整型索引包括以下类型
		switch (b[seek]) {
		case Type.LONG:
			column = new com.lexst.sql.column.Long(); break;
		case Type.TIMESTAMP:
			column = new com.lexst.sql.column.Timestamp(); break;
		case Type.RAW:
			column = new com.lexst.sql.column.Raw(); break;
		case Type.CHAR:
			column = new com.lexst.sql.column.Char(); break;
		case Type.SCHAR:
			column = new com.lexst.sql.column.SChar(); break;
		case Type.WCHAR:
			column = new com.lexst.sql.column.WChar(); break;
		case Type.VCHAR:
			column = new com.lexst.sql.column.VChar(); break;
		case Type.VSCHAR:
			column = new com.lexst.sql.column.VSChar(); break;
		case Type.VWCHAR:
			column = new com.lexst.sql.column.VWChar(); break;
		default:
			throw new ColumnException("cannot support column type: %d!", b[seek]);
		}
		// 解析列
		column.setId(columnId);
		int size = column.resolve(b, seek, end - seek);
		seek += size;

		return seek - off;
	}
}
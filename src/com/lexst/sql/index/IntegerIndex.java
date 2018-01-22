/**
 *
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.util.*;

public class IntegerIndex extends ColumnIndex {

	private static final long serialVersionUID = 1L;

	/** 索引值，用于定位数据区域 */
	private int value;

	/**
	 * default
	 */
	public IntegerIndex() {
		super(Type.INTEGER_INDEX);
		value = 0;
	}

	/**
	 * @param object
	 */
	public IntegerIndex(IntegerIndex object) {
		super(object);
		this.value = object.value;
	}

	/**
	 * @param value
	 */
	public IntegerIndex(int value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param value
	 * @param column
	 */
	public IntegerIndex(int value, Column column) {
		this();
		this.setValue(value);
		this.setColumn(column);
	}

	/**
	 * 设置索引值
	 * @param num
	 */
	public void setValue(int num) {
		this.value = num;
	}

	/**
	 * 返回索引值
	 * @return
	 */
	public int getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#duplicate()
	 */
	@Override
	public WhereIndex duplicate() {
		return new IntegerIndex(this);
	}
	
	/*
	 * 输出整型索引数据流
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
	 * 解析整形索引数据流
	 * @see com.lexst.sql.index.WhereIndex#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 检查尺寸
		if (seek + 8 > end) {
			throw new SizeOutOfBoundsException("integer index sizeout!");
		}
		super.setType(b[seek]);
		seek += 1;
		this.value = Numeric.toInteger(b, seek, 4);
		seek += 4;
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		// 限制三种类型
		switch(b[seek]) {
		case Type.INTEGER:
			column = new com.lexst.sql.column.Integer(); break;
		case Type.DATE:
			column = new com.lexst.sql.column.Date(); break;
		case Type.TIME:
			column = new com.lexst.sql.column.Time(); break;
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
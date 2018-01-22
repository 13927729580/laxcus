/**
 *
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.util.*;

public class ShortIndex extends ColumnIndex {

	private static final long serialVersionUID = 1L;

	/** 短整形索引值，用于定位数据区域 */
	private short value;

	/**
	 * default
	 */
	public ShortIndex() {
		super(Type.SHORT_INDEX);
		value = 0;
	}

	/**
	 * clone it
	 * @param param
	 */
	public ShortIndex(ShortIndex param) {
		super(param);
		this.value = param.value;
	}

	/**
	 * @param value
	 */
	public ShortIndex(short value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param value
	 * @param column
	 */
	public ShortIndex(short value, Column column) {
		this();
		this.setValue(value);
		this.setColumn(column);
	}

	/**
	 * set index value
	 * @param num
	 */
	public void setValue(short num) {
		this.value = num;
	}

	/**
	 * get index value
	 * @return
	 */
	public short getValue() {
		return this.value;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#duplicate()
	 */
	@Override
	public WhereIndex duplicate() {
		return new ShortIndex(this);
	}
	
	/*
	 * 输出短整型索引数据流到缓存
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
	 * 解析数据流，返回解析字节长度
	 * @see com.lexst.sql.index.WhereIndex#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 检查尺寸
		if (seek + 6 > end) {
			throw new SizeOutOfBoundsException("short index sizeout!");
		}
		super.setType(b[seek]);
		seek += 1;
		this.value = Numeric.toShort(b, seek, 2);
		seek += 2;
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		// 只有SHORT列
		if (b[seek] != Type.SHORT) {
			throw new ColumnException("cannot support column type: %d!", b[seek]);
		}
		// 解析列
		column = new com.lexst.sql.column.Short();
		column.setId(columnId);
		int size = column.resolve(b, seek, end - seek);
		seek += size;

		return seek - off;
	}
}
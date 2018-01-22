/**
 *
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.util.*;

public class DoubleIndex extends ColumnIndex {

	private static final long serialVersionUID = 1L;

	private double value;

	/**
	 * default constrctor
	 */
	public DoubleIndex() {
		super(Type.DOUBLE_INDEX);
		value = 0.0f;
	}

	/**
	 * @param object
	 */
	public DoubleIndex(DoubleIndex object) {
		super(object);
		this.value = object.value;
	}

	/**
	 * @param value
	 */
	public DoubleIndex(double value) {
		this();
		this.setValue(value);
	}

	/**
	 * @param value
	 * @param column
	 */
	public DoubleIndex(double value, Column column) {
		this();
		this.setValue(value);
		this.setColumn(column);
	}

	public void setValue(double num) {
		this.value = num;
	}

	public double getValue() {
		return this.value;
	}

	/*
	 * 复制对象
	 * @see com.lexst.sql.index.WhereIndex#duplicate()
	 */
	@Override
	public WhereIndex duplicate() {
		return new DoubleIndex(this);
	}
	
	/*
	 * 输出双浮点索引数据流到缓存
	 * @see com.lexst.sql.index.WhereIndex#build(java.io.ByteArrayOutputStream)
	 */
	@Override
	public void build(ByteArrayOutputStream buff) {
		// 类型定义
		buff.write(super.getType());
		// 索引值
		byte[] b = Numeric.toBytes(java.lang.Double.doubleToLongBits(this.value));
		buff.write(b, 0, b.length);
		// 列标识(column identity)
		b = Numeric.toBytes(this.getColumnId());
		buff.write(b, 0, b.length);
		// 列数据(输出数据到缓存)
		this.getColumn().build(buff);
	}
	
	/*
	 * 解析双浮点数据流，返回解析字节长度
	 * @see com.lexst.sql.index.WhereIndex#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 检查尺寸
		if (seek + 12 > end) {
			throw new SizeOutOfBoundsException("float index sizeout!");
		}
		super.setType(b[seek]);
		seek += 1;
		long num = Numeric.toLong(b, seek, 8);
		seek += 8;
		this.value = java.lang.Double.longBitsToDouble(num);		
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		// 只定义双浮点列
		if (b[seek] != Type.DOUBLE) {
			throw new ColumnException("cannot support column type: %d!", b[seek]);
		}
		// 解析列
		column = new com.lexst.sql.column.Double();
		column.setId(columnId);
		int size = column.resolve(b, seek, end - seek);
		seek += size;

		return seek - off;
	}

}
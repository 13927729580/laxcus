/**
 *
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.util.*;


public class FloatIndex extends ColumnIndex {

	private static final long serialVersionUID = 1L;

	private float value;

	/**
	 * default
	 */
	public FloatIndex() {
		super(Type.FLOAT_INDEX);
		value = 0.0f;
	}

	/**
	 * @param object
	 */
	public FloatIndex(FloatIndex object) {
		super(object);
		this.value = object.value;
	}

	/**
	 * @param num
	 */
	public FloatIndex(float num) {
		this();
		this.setValue(num);
	}

	/**
	 * @param num
	 * @param column
	 */
	public FloatIndex(float num, com.lexst.sql.column.Float column) {
		this();
		this.setValue(num);
		this.setColumn(column);
	}

	/**
	 * 设置单浮点索引值
	 * @param num
	 */
	public void setValue(float num) {
		this.value = num;
	}

	/**
	 * 返回单浮点索引值
	 * @return
	 */
	public float getValue() {
		return this.value;
	}
	
	/*
	 * 复制对象
	 * @see com.lexst.sql.index.WhereIndex#duplicate()
	 */
	@Override
	public WhereIndex duplicate() {
		return new FloatIndex(this);
	}
	
	/*
	 * 输出单浮点索引数据流到缓存
	 * @see com.lexst.sql.index.WhereIndex#build(java.io.ByteArrayOutputStream)
	 */
	@Override
	public void build(ByteArrayOutputStream buff) {
		// 类型定义
		buff.write(super.getType());
		// 索引值
		byte[] b = Numeric.toBytes(java.lang.Float.floatToIntBits(this.value));
		buff.write(b, 0, b.length);
		// 列标识(column identity)
		b = Numeric.toBytes(this.getColumnId());
		buff.write(b, 0, b.length);
		// 列数据(输出数据到缓存)
		this.getColumn().build(buff);
	}
	
	/*
	 * 解析单浮点数据流，返回解析字节长度
	 * @see com.lexst.sql.index.WhereIndex#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 检查尺寸
		if (seek + 8 > end) {
			throw new SizeOutOfBoundsException("float index sizeout!");
		}
		super.setType(b[seek]);
		seek += 1;
		int num = Numeric.toInteger(b, seek, 4);
		seek += 4;
		this.value = java.lang.Float.intBitsToFloat(num);		
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		// 只定义单浮点列
		if (b[seek] != Type.FLOAT) {
			throw new ColumnException("cannot support column type: %d!", b[seek]);
		}
		// 解析列
		column = new com.lexst.sql.column.Float();
		column.setId(columnId);
		int size = column.resolve(b, seek, end - seek);
		seek += size;

		return seek - off;
	}

}
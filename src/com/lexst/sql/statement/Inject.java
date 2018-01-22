/**
 * 
 */
package com.lexst.sql.statement;

import java.io.*;
import java.util.*;

import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * 
 * SQL INSERT的多记录类 
 */
public class Inject extends DefaultInsert {

	private static final long serialVersionUID = 5050386004244544715L;

	/** 记录数组 */
	private ArrayList<Row> array = new ArrayList<Row>();
	
	/**
	 * default
	 */
	public Inject() {
		super();
	}
	
	/**
	 * 指定记录空间尺寸
	 * @param capacity
	 */
	public Inject(int capacity) {
		this();
		this.ensure(capacity);
	}
	
	/**
	 * @param table
	 */
	public Inject(Table table) {
		this();
		setTable(table);
	}
	
	/**
	 * @param table
	 * @param capacity
	 */
	public Inject(Table table, int capacity) {
		this();
		super.setTable(table);
		this.ensure(capacity);
	}

	/**
	 * 将记录数组空间调整为指定的尺寸
	 * 
	 * @param capacity - 指定的尺寸
	 */
	public void ensure(int capacity) {
		array.ensureCapacity(capacity);
	}

	/**
	 * 将记录数组空间调整为实际大小
	 */
	public void trim() {
		this.array.trimToSize();
	}

	/**
	 * 增加一条记录
	 * @param row
	 */
	public void add(Row row) {
		this.array.add(row);
	}
	
	public List<Row> list() {
		return this.array;
	}
	
	public void clear() {
		this.array.clear();
	}
	
	public boolean isEmpty() {
		return this.array.isEmpty();
	}
	
	public int size() {
		return this.array.size();
	}
		
	/**
	 * 将多行记录生成数据流并输出
	 * @return
	 */
	private byte[] buildRows() {
		// 预定义数据流总长度和单行记录最大长度
		int capacity = 0;
		int maxlen = 0;
		for (Row row : array) {
			int size = row.capacity();
			if(size > maxlen) maxlen = size;
			capacity += size;
		}
		int left = capacity % 32;
		if (left > 0) capacity = capacity - left + 32;
		left = maxlen % 32;
		if (left > 0) maxlen = maxlen - left + 32;

		// 输出数据到缓存(需要产生校验码)
		ByteArrayOutputStream buff = new ByteArrayOutputStream(capacity);
		for(Row row : array) {
			row.build(maxlen, true, buff);
		}

		// 输出数据流
		byte[] data = buff.toByteArray();
		// 重置缓存
		buff.reset();
		
		// 写入数据流长度和行记录数
		byte[] b = Numeric.toBytes(data.length);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(array.size());
		buff.write(b, 0, b.length);
		// 写记录并且输出
		buff.write(data, 0, data.length);
		return buff.toByteArray();
	}

	/**
	 * 输出数据流
	 * @return
	 */
	public byte[] build() {
		// 输出数据域
		byte[] data = buildRows();
		// 标识域和数据域合并输出
		return super.build(data);
	}

}
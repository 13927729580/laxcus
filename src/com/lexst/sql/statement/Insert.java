/**
 *
 */
package com.lexst.sql.statement;

import java.io.*;
import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * SQL INSERT的单记录类
 */
public class Insert extends DefaultInsert  {

	private static final long serialVersionUID = 2720725288904568512L;

	/** 一条记录 **/
	protected Row row = new Row();

	/**
	 * insert method
	 */
	public Insert() {
		super();
	}
	
	/**
	 * @param table
	 */
	public Insert(Table table) {
		super(table);
	}

	/**
	 * @param table
	 * @param row
	 */
	public Insert(Table table, Row row) {
		this(table);
		this.setRow(row);
	}

	/**
	 * 返回一行记录
	 * @return
	 */
	public Row getRow() {
		return this.row;
	}

	/**
	 * 设置一行记录
	 * @param row
	 */
	public void setRow(Row row) {
		this.row = (Row) row.clone();
	}

	public boolean add(Column value) {
		return row.add(value);
	}

	public Collection<Column> list() {
		return row.list();
	}

	public void clear() {
		row.clear();
	}

	public boolean isEmpty() {
		return row.isEmpty();
	}

	public int size() {
		return row.size();
	}

	/**
	 * 构造生成数据域
	 * @return
	 */
	private byte[] buildRow() {
		// 输出一行记录的字节流
		byte[] data = row.build();
		ByteArrayOutputStream buff = new ByteArrayOutputStream(8 + data.length);
		// 数据流长度
		byte[] b = Numeric.toBytes(data.length);
		buff.write(b, 0, b.length);
		// 行记录统计(总是1)
		int count = 1;
		b = Numeric.toBytes(count);
		buff.write(b, 0, b.length);
		// 写记录
		buff.write(data, 0, data.length);
		// 输出
		return buff.toByteArray();
	}
	
	/**
	 * 构造生成数据流
	 * @return
	 */
	public byte[] build() {
		// 输出数据域
		byte[] data = buildRow();
		// 合并输出全部
		return super.build(data);
	}

}
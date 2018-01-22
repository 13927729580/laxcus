/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * row class
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/26/2009
 * @see com.lexst.sql.row
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.row;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * 记录(行)的类表现
 * 
 * @author Administrator
 *
 */
public class Row implements Serializable {
	
	private static final long serialVersionUID = -3224196864274612931L;
	
	public final static byte VALID = 1;
	public final static byte DELETE = 2;

	/** 记录(行)头标记 **/
	private byte status;	//状态，有效或删除
	private int checksum;	//CRC32校验和 (计算从记录长度及以下的列数据流) 
	private int checklen;	//记录长度（包括头标记和列生成流)
	private short columns;	//列成员总数

	/** 列成员数组(按照列ID顺序存储) **/
	private ArrayList<Column> array = new ArrayList<Column>(5);

	/**
	 * default constractor
	 */
	public Row() {
		super();
	}
	
	/**
	 * 复制对象
	 * 
	 * @param row
	 */
	public Row(Row row) {
		this();
		this.status = row.status;
		this.checksum = row.checksum;
		this.checklen = row.checklen;
		this.columns = row.columns;
		array.addAll(row.array);
		this.trim();
	}

	/**
	 * 指定列数组空间尺寸
	 * 
	 * @param capacity - 基本列空间尺寸
	 */
	public Row(int capacity) {
		this();
		array.ensureCapacity(capacity);
	}
	
	/**
	 * 记录头标记长度(11字节)
	 * 
	 * @return
	 */
	private final int presize() {
		return 11;
	}

	/**
	 * 返回"行"CRC32校验和
	 * @return
	 */
	public int getChecksum() {
		return this.checksum;
	}
	
	/**
	 * 返回"行"数据长度
	 * @return
	 */
	public int getChecklen() {
		return this.checklen;
	}
	
	/**
	 * 将数组空间调整为实际大小(删除剩余空间)
	 */
	public void trim() {
		this.array.trimToSize();
	}

	/**
	 * 判断当前是否空状态
	 * @return
	 */
	public boolean isEmpty() {
		return this.array.isEmpty();
	}

	/**
	 * 返回列成员数
	 * @return
	 */
	public int size() {
		return array.size();
	}
	
	/**
	 * 本"行"的字节长度（包括头标记11字节和列的字节长度)
	 * 
	 * @return
	 */
	public int capacity() {
		int size = 0;
		for (Column column : array) {
			size += column.capacity();
		}
		return presize() + size;
	}
	
	/**
	 * 返回列ID集合
	 * @return
	 */
	public Set<java.lang.Short> keySet() {
		TreeSet<java.lang.Short> set = new TreeSet<java.lang.Short>();
		for(Column column : array) {
			set.add(column.getId());
		}
		return set;
	}
	
	/**
	 * 根据列ID查找对应的列
	 * 
	 * @param columnId
	 * @return
	 */
	public Column find(short columnId) {
		// 列ID是从1开始
		if (columnId > 0 && columnId <= array.size()) {
			Column column = array.get(columnId - 1);
			if (column.getId() == columnId) {
				return column;
			}
		}
		for (int index = 0; index < array.size(); index++) {
			Column column = array.get(index);
			if (column.getId() == columnId) {
				return column;
			}
		}
		return null;
	}
	
	/**
	 * 返回指定下标的列
	 * 
	 * @param index
	 * @return
	 */
	public Column get(int index) {
		if (index < 0 || index >= array.size()) {
			return null;
		}
		return array.get(index);
	}
	
	/**
	 * 保存一列数据
	 * @param column
	 * @return
	 */
	public boolean add(Column column) {
		short columnId = column.getId();
		if (columnId > array.size()) {
			array.add(column);
		} else {
			array.add(columnId - 1, column);
		}
		return true;
	}
	
	/**
	 * 根据列ID删除对应"列"
	 * 
	 * @param columnId
	 * @return
	 */
	public boolean remove(short columnId) {
		if (columnId <= array.size()) {
			Column column = array.get(columnId - 1);
			if (column.getId() == columnId) {
				return array.remove(columnId - 1) != null;
			}
		}
		for (int index = 0; index < array.size(); index++) {
			Column column = array.get(index);
			if (column.getId() == columnId) {
				return array.remove(index) != null;
			}
		}
		return false;
	}

	/**
	 * 替换一列
	 * @param column
	 * @return
	 */
	public boolean replace(Column column) {
		short columnId = column.getId();
		if(remove(columnId)) {
			return this.add(column);
		}
		return false;
	}
	
	/**
	 * 返回全部列集合
	 * @return
	 */
	public Collection<Column> list() {
		return this.array;
	}
	
	/**
	 * 清除全部列
	 */
	public void clear() {
		array.clear();
	}
	
	/*
	 * 克隆对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Row(this);
	}

	/**
	 * 指定数据空间长度和校验码，生成"行"数据流
	 * 
	 * @param capacity
	 * @param crc32
	 * @param buff
	 * @return
	 */
	public int build(int capacity, boolean crc32, ByteArrayOutputStream buff) {
		// 如果未定义数据流空间长度时
		if(capacity < 1) {
			capacity = capacity();
			int left = capacity % 32;
			if (left > 0) capacity = capacity - left + 32;
		}

		ByteArrayOutputStream body = new ByteArrayOutputStream(capacity);
		// 全部列转成数据流
		for (Column column : array) {
			column.build(body);
		}
		// 取出数据流
		byte[] data = body.toByteArray();

		// 状态位(有效)
		this.status = Row.VALID;
		// 列成员数
		this.columns = (short) array.size();
		// 一行记录的长度(包括头信息)
		this.checklen = presize() + data.length;
		
		// 如果要求生成CRC32校验码
		if(crc32) {
			CRC32 sum = new CRC32();
			byte[] b = Numeric.toBytes(checklen);
			sum.update(b, 0, b.length);
			b = Numeric.toBytes(columns);
			sum.update(b, 0, b.length);
			sum.update(data, 0, data.length);
			checksum = (int)sum.getValue();
		} else {
			checksum = 0; // 校验码默认0，不设置
		}

		// 生成头标记
		buff.write(this.status);
		byte[] b = Numeric.toBytes(this.checksum);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(this.checklen);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(this.columns);
		buff.write(b, 0, b.length);
		// 列数据流写入缓存
		buff.write(data, 0, data.length);
		
		return checklen;
	}

	/**
	 * 生成"行"数据流并且输出到缓存
	 * 
	 * @param buff
	 * @return
	 */
	public int build(ByteArrayOutputStream buff) {
		return this.build(0, true, buff);
	}

	/**
	 * 生成"行"数据流，返回字节数据
	 * @return
	 */
	public byte[] build() {
		int deflen = capacity();
		int left = deflen % 32;
		if(left > 0) deflen = deflen - left + 32;
		
		ByteArrayOutputStream buff = new ByteArrayOutputStream(deflen);
		this.build(deflen, true, buff);
		return buff.toByteArray();
	}

	/**
	 * 解析行头标记
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public final int resolveTag(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		if(seek + presize() > end) {
			throw new RowParseException("row tag sizeout!");
		}
		
		// 状态(有效或者无效)
		status = b[seek];
		seek += 1;
		// CRC32校验码
		checksum = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 一行记录的长度(包括头标记11个字节)
		checklen = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 列成员总数
		columns = Numeric.toShort(b, seek, 2);
		seek += 2;
		
		return seek - off;
	}

	/**
	 * 根据Sheet中的列排列顺序，依次解析数据流
	 * 成功，返回解析流长度；失败，弹出错误
	 * 
	 * @param sheet
	 * @param b
	 * @param off
	 * @return
	 */
	public int resolve(Sheet sheet, byte[] b, int off, int len) {
		int seek = off;
		// 解析并返回头标记
		int size = resolveTag(b, seek, len);
		// 如果一行记录长度超过最大时...
		if (checklen <= presize()) {
			throw new RowParseException("row stream indexout!");
		} else if (seek + this.checklen > off + len) {
			throw new RowParseException("row stream sizeout!");
		}
		// 跨过头标记
		seek += size;
		// 行结束下标
		int end = off + checklen;
		
		// 解析列
		for (int index = 0; seek < end; index++) {
			ColumnAttribute attribute = sheet.get(index);
			if(attribute == null) {
				throw new RowParseException("cannot find attribute!");
			}

			// 列数据类型
			byte type = Type.parseType(b[seek]);			
			// 解析"列"并返回解析长度
			Column column = com.lexst.sql.column.ColumnCreator.create(type);
			if (column == null) {
				throw new RowParseException("unknown column type:%d", type & 0xFF);
			}
			size = column.resolve(b, seek, end - seek);
			seek += size;
			//设置列ID
			column.setId(attribute.getColumnId());
			// 保存
			this.add(column);
		}
		// 释放多余空间
		this.trim();
		return seek - off;
	}

	/**
	 * 根据Table的列排列解析数据流
	 * 成功，返回解析的字节长度；失败弹出异常
	 * 
	 * @param table
	 * @param b
	 * @param off
	 * @return
	 */
	public int resolve(Table table, byte[] b, int off, int len) {
		// 解析头标记
		int seek = off;
		int size = resolveTag(b, seek, len);
		if (checklen <= presize()) {
			throw new RowParseException("row stream indexout!");
		} else if (seek + checklen > off + len) {
			throw new RowParseException("row stream sizeout!");
		}
		// 跨过头标记
		seek += size;
		// 本行结束下标
		int end = off + this.checklen;
		
		short elements = (short)table.size();
		
		for (short columnId = 1; columnId <= elements; columnId++) {
			if(seek > end) {
				throw new RowParseException("row stream sizeout!");
			}
			// 查找匹配的属性
			ColumnAttribute attribute = table.find(columnId);
			if(attribute == null) {
				throw new RowParseException("cannot find column: %d",columnId);
			}
			
			// 解析列数据类型
			byte type = Type.parseType(b[seek]);
			if (type != attribute.getType()) {
				throw new RowParseException("column not match! %d - %d", type, attribute.getType());
			}
			Column column = com.lexst.sql.column.ColumnCreator.create(type);
			if(column == null) {
				throw new RowParseException("unknown column type:%d", type & 0xFF);
			}
			size = column.resolve(b, seek, end - seek);
			seek += size;
			// 设置列ID并且保存
			column.setId(attribute.getColumnId());
			this.add(column);
		}
		// 释放多余的空间
		this.trim();
		// 被解析的数据流长度
		return seek - off;
	}

}
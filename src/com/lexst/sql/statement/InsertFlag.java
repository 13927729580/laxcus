/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.statement;

import java.io.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * SQL INSERT的数据报头标识
 */
public class InsertFlag implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/** 解析字节流的长度 **/
	private int flagSize;

	/** 数据标识域和数据域的字节总长度(包括自身4字节) **/
	private int totalSize;

	/** INSERT版本号 **/
	private int version;

	/** 所属数据库表名 **/
	private Space space;
	
	private Sheet sheet;
	
	/**
	 * default
	 */
	protected InsertFlag() {
		super();
	}
	
	/**
	 * 被解析的字节流长度
	 * @return
	 */
	public int getFlagSize() {
		return this.flagSize;
	}
	
	/**
	 * 返回一次INSERT数据总长度
	 * @return
	 */
	public int getTotalSize() {
		return this.totalSize;
	}
	
	/**
	 * 返回版本号
	 * @return
	 */
	public int getVersion() {
		return this.version;
	}

	/**
	 * 返回数据库表名称
	 * @return
	 */
	public Space getSpace() {
		return this.space;
	}
	
	/**
	 * 列顺序排列记录
	 * @return
	 */
	public Sheet getShee() {
		return this.sheet;
	}

	/**
	 * 解析SQL INSERT的数据标识头，返回解析长度
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 尺寸不足
		if (seek + 10 > end) {
			throw new SizeOutOfBoundsException("insert flag size missing!");
		}
		// 数据总长度，包括它自己的4字节
		this.totalSize = Numeric.toInteger(b, seek, 4);
		seek += 4;

		// 版本号
		this.version = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (this.version != DefaultInsert.VERSION) {
			throw new InvalidTypeException("insert version not match!");
		}

		// 数据库表名长度
		int schemaSize = b[seek++] & 0xFF;
		int tableSize = b[seek++] & 0xFF;

		// 尺寸不足
		if (seek + schemaSize + tableSize > end) {
			throw new SizeOutOfBoundsException("insert flag size missing!");
		}

		String schema = new String(b, seek, schemaSize);
		seek += schemaSize;
		String table = new String(b, seek, tableSize);
		seek += tableSize;
		this.space = new Space(schema, table);
		
		// 解析数据表
		int sheetSize = this.resolveSheet(b, seek, end - seek);
		seek += sheetSize;

		// 记录被解析的字节流长度并且返回
		return (this.flagSize = seek - off);
	}
	
	/**
	 * 解析数据列排列顺序
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	private int resolveSheet(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		sheet = new Sheet();

		if (seek + 6 > end) {
			throw new SizeOutOfBoundsException("resovle sheet size missing!");
		}
		short elements = Numeric.toShort(b, seek, 2);
		seek += 2;
		int sheetSize = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(seek + sheetSize > end) {
			throw new SizeOutOfBoundsException("resovle sheet size missing!");
		}

		for (short index = 0; index < elements; index++) {
			if (seek + 4 > end) {
				throw new SizeOutOfBoundsException("resovle sheet size missing!");
			}
			// 列属性类型
			byte type = b[seek];
			seek += 1;
			// 列标识号
			short columnId = Numeric.toShort(b, seek, 2);
			seek += 2;

			// 列名长度
			int nameSize = b[seek] & 0xFF;
			seek += 1;
			// 列名
			if (seek + nameSize > end) {
				throw new SizeOutOfBoundsException("resovle sheet size missing!");
			}
			String name = new String(b, seek, nameSize);
			seek += nameSize;

			// 保存参数
			ColumnAttribute attribute = ColumnAttributeCreator.create(type);
			attribute.setColumnId(columnId);
			attribute.setName(name);
			sheet.add(index, attribute);
		}

		return seek - off;
	}
	
	/**
	 * 输出数据表名字节流
	 * @return
	 */
	private byte[] buildSpace() {
		byte[] schema = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();
		// 数据库名长度和数据库表名长度
		ByteArrayOutputStream buff = new ByteArrayOutputStream(128);
		buff.write((byte) (schema.length & 0xFF));
		buff.write((byte) (table.length & 0xFF));
		// 保存数据库表信息
		buff.write(schema, 0, schema.length);
		buff.write(table, 0, table.length);
		return buff.toByteArray();
	}

	/**
	 * 输出列排序字节流
	 * @param table
	 * @return
	 */
	private byte[] buildSheet(Table table) {
		sheet = new Sheet();
		short index = 0;
		for (short columnId : table.idSet()) {
			ColumnAttribute attribute = table.find(columnId);
			sheet.add(index++, attribute);
		}
		
		index = 0;
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		while (index < sheet.size()) {
			ColumnAttribute attribute = sheet.get(index++);
			// 数据类型
			buff.write(attribute.getType());
			// 列标识号
			byte[] b = Numeric.toBytes(attribute.getColumnId());
			buff.write(b, 0, b.length);
			// 列名长度
			b = attribute.getName().getBytes();
			buff.write((byte) (b.length & 0xFF));
			// 列名
			buff.write(b, 0, b.length);
		}
		
		byte[] fields = buff.toByteArray();
		
		// 重置
		buff.reset();
		byte[] b = Numeric.toBytes((short) sheet.size());
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(fields.length);
		buff.write(b, 0, b.length);
		buff.write(fields, 0, fields.length);
		
		return buff.toByteArray();
	}
	
	/**
	 * @param table
	 * @param contentSize
	 * @return
	 */
	public byte[] build(Table table, int contentSize) {
		this.version = DefaultInsert.VERSION;
		this.space = new Space(table.getSpace());

		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		
		// 总长度默认为0(只为占据4个字节空间)
		byte[] b = Numeric.toBytes(this.totalSize = 0);
		buff.write(b, 0, b.length);
		// 版本号
		b = Numeric.toBytes(version);
		buff.write(b, 0, b.length);
		// 数据库长名总长度
		b = buildSpace();
		buff.write(b, 0, b.length);
		
		// 列顺序表
		b = buildSheet(table);
		buff.write(b, 0, b.length);
		
		// INSERT标记长度
		this.flagSize = buff.size();

		// 输出INSERT标记字节流
		byte[] data = buff.toByteArray();
		
		// 改写INSERT总长度
		this.totalSize = this.flagSize + contentSize;
		b = Numeric.toBytes(this.totalSize);
		System.arraycopy(b, 0, data, 0, b.length);
		return data;
	}
	
//	public byte[] build2(Table table, int contentSize) {
//		this.version = DefaultInsert.VERSION;
//		this.space = new Space(table.getSpace());
//
//		byte[] b_version = Numeric.toBytes(version);
//		byte[] b_space = buildSpace();
//		int tagSize = 4 + b_version.length + b_space.length;
//		// create table
//		byte[] b_sheet = buildSheet(table);
//		// create row set
//		int headSize = tagSize + b_sheet.length;
//
//		// byte[] data = buildRow();
//		this.size = headSize + contentSize; // data.length;
//		byte[] b_allsize = Numeric.toBytes(this.size);
//
//		// build all
//		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
//		buff.write(b_allsize, 0, b_allsize.length);
//		buff.write(b_version, 0, b_version.length);
//		buff.write(b_space, 0, b_space.length);
//		buff.write(b_sheet, 0, b_sheet.length);
//
//		return buff.toByteArray();
//	}
	
//	protected void setSize(int i) {
//		this.size = i;
//	}
//	
//	protected void setVersion(int i) {
//		this.version = i;
//	}
//	
//	protected void setSpace(String schema, String table) {
//		this.space = new Space(schema, table);
//	}
}

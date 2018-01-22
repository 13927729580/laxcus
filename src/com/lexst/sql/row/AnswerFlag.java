/**
 * 
 */
package com.lexst.sql.row;

import java.io.*;

import com.lexst.sql.Type;
import com.lexst.sql.charset.*;
import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * SQL检索(SELECT、DELETE)结果的前缀信息
 * 
 * 
 */
public final class AnswerFlag {

	/** 检索数据流长度(不包括AnswerFlag自身的尺寸) **/
	private long size;
	/** 记录长度 */
	private int rows;
	/** 每一条记录的列成员数 */
	private short columns;
	/** 存储模式 ，见 "com.lexst.sql.Type" **/
	private byte sm;
	/** 数据库表名称(数据库名和表名构成) **/
	private String schema;
	private String table;

	/**
	 * default
	 */
	public AnswerFlag() {
		size = 0L;
		rows = 0;
		columns = 0;
		sm = 0;
	}
	
	/**
	 * 
	 * @param space
	 */
	public AnswerFlag(Space space) {
		this();
		this.setSpace(space);
	}
	
	/**
	 * 前缀字节长度
	 * @return
	 */
	private final int presize() {
		return 17;
	}
	
	/**
	 * 设置检索结果数据流长度
	 * @param i
	 */
	public void setSize(long i) {
		this.size = i;
	}
	
	/**
	 * 检索数据流长度
	 * @return
	 */
	public long getSize() {
		return this.size;
	}
	
	/**
	 * 行记录统计
	 * @param i
	 */
	public void setRows(int i){
		this.rows = i;
	}
	
	public int getRows() {
		return this.rows;
	}
	
	/**
	 * 每一行记录的列成员数
	 * @param i
	 */
	public void setColumns(short i) {
		this.columns = i;
	}
	
	public short getColumns() {
		return this.columns;
	}
	
	/**
	 * 设置存储模型 (NSM, DSM)
	 * @param b
	 */
	public void setStorage(byte b) {
		if (b != Type.DSM && b != Type.NSM) {
			throw new IllegalArgumentException("invalid storage model!");
		}
		this.sm = b;
	}

	/**
	 * 返回存储模型 (NSM, DSM)
	 * @return
	 */
	public byte getStorage() {
		return this.sm;
	}

	/**
	 * is NSM(row storage model)
	 * @return
	 */
	public boolean isNSM() {
		return this.sm == Type.NSM;
	}

	/**
	 * is DSM (column storage model)
	 * @return
	 */
	public boolean isDSM() {
		return this.sm == Type.DSM;
	}

	/**
	 * 设置数据表名
	 * 
	 * @param schema
	 * @param table
	 */
	public void setSpace(String schema, String table) {
		this.schema = schema;
		this.table = table;
	}

	/**
	 * 设置数据表名
	 * 
	 * @param s
	 */
	public void setSpace(Space s) {
		this.setSpace(s.getSchema(), s.getTable());
	}

	/**
	 * 返回数据表名称
	 * 
	 * @return
	 */
	public Space getSpace() {
		return new Space(schema, table);
	}
	
	/**
	 * 生成数据流
	 * 
	 * @return
	 */
	public byte[] build() {		
		ByteArrayOutputStream buff = new ByteArrayOutputStream(128);
	
		// 后续的数据流长度
		byte[] b = Numeric.toBytes(this.size);
		buff.write(b, 0, b.length);		
		// 行记录总数
		b = Numeric.toBytes(this.rows);
		buff.write(b, 0, b.length);
		// 每行的列成员数
		b = Numeric.toBytes(this.columns);
		buff.write(b, 0, b.length);
		// 存储模式
		buff.write(this.sm);
		
		// 数据表尺寸和名称
		byte[] s1 = new UTF8().encode(schema);
		byte[] s2 = new UTF8().encode(table);
		
		buff.write(s1.length & 0xFF);
		buff.write(s2.length & 0xFF);
		buff.write(s1, 0, s1.length);
		buff.write(s2, 0, s2.length);

		return buff.toByteArray();
	}
	
	/**
	 * 解析数据流
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		if (seek + presize() > end) {
			throw new SizeOutOfBoundsException("size missing!");
		}

		// 检索结果数据流长度
		size = Numeric.toLong(b, seek, 8);
		seek += 8;
		// 行记录数
		rows = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 每行列成员数
		columns = Numeric.toShort(b, seek, 2);
		seek += 2;
		// 存储模型
		sm = b[seek++];
		// 数据库名称和表名称长度
		int schemaSize = b[seek++] & 0xFF;
		int tableSize = b[seek++] & 0xFF;
		
		if (seek + schemaSize + tableSize > end) {
			throw new SizeOutOfBoundsException("space size missing!");
		}
		schema = new UTF8().decode(b, seek, schemaSize);
		seek += schemaSize;
		table = new UTF8().decode(b, seek, tableSize);
		seek += tableSize;
		
		return seek - off;
	}

}
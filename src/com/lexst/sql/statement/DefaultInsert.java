/**
 * 
 */
package com.lexst.sql.statement;

import com.lexst.sql.schema.*;

/**
 * 数据写入类，是"INSERT INTO" 和 "INJECT INTO"的基类
 */
public class DefaultInsert extends SQLMethod {
	
	private static final long serialVersionUID = -4731606853426395343L;
	
	/** INSERT版本号，不同的版本号后续参数会有不同 **/
	protected final static int VERSION = 0x100;

	/** 数据库表属性配置 **/
	protected Table table;

	/**
	 * 初始化插入类
	 */
	public DefaultInsert() {
		super(Compute.INSERT_METHOD);
	}
	
	/**
	 * @param table
	 */
	public DefaultInsert(Table table) {
		this();
		this.setTable(table);
	}

	/**
	 * 设置表属性集合
	 * @param t
	 */
	public void setTable(Table t) {
		setSpace(t.getSpace());
		this.table = t;
	}

	/**
	 * 返回表属性集合
	 * @return
	 */
	public Table getTable() {
		return this.table;
	}
	
	/**
	 * 数据域和标识域合并输出
	 * @return
	 */
	protected byte[] build(byte[] data) {
		// 输出标识域
		InsertFlag flag = new InsertFlag();
		byte[] head = flag.build(this.table, data.length);
		// 合并并且返回
		byte[] buff = new byte[head.length + data.length];
		System.arraycopy(head, 0, buff, 0, head.length);
		System.arraycopy(data, 0, buff, head.length, data.length);
		return buff;
	}
	
	/**
	 * 解析SQL INSERT数据流的数据标识域
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public InsertFlag resolveFlag(byte[] b, int off, int len) {
		InsertFlag flag = new InsertFlag();
		flag.resolve(b, off, len);
		return flag;
	}
	
//	protected byte[] build_space() {
//		// space field
//		byte[] schema = space.getSchema().getBytes();
//		byte[] table = space.getTable().getBytes();
//		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
//		// db size and table size
//		buff.write((byte) (schema.length & 0xFF));
//		buff.write((byte) (table.length & 0xFF));
//		// database and table name
//		buff.write(schema, 0, schema.length);
//		buff.write(table, 0, table.length);
//		return buff.toByteArray();
//	}
//	
//	protected byte[] buildSheet() {
//		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
//
//		short cidCount = 0;
//		for(short columnId : table.idSet()) {
//			ColumnAttribute attribute = table.find(columnId);
//			String name = attribute.getName();
//
//			// 数据类型
//			buff.write(attribute.getType());
//			// 列ID
//			byte[] b = Numeric.toBytes(columnId);
//			buff.write(b, 0, b.length);
//			// 列名长度
//			b = name.getBytes();
//			byte size = (byte)(b.length & 0xff);
//			buff.write(size);
//			// 列名
//			buff.write(b, 0, b.length);
//			cidCount++;
//		}
//
//		byte[] data = buff.toByteArray();
//		byte[] count = com.lexst.util.Numeric.toBytes(cidCount);
//		byte[] sz = com.lexst.util.Numeric.toBytes(data.length);
//
//		buff.reset();
//		buff.write(count ,0, count.length);
//		buff.write(sz, 0, sz.length);
//		buff.write(data, 0, data.length);
//
//		return buff.toByteArray();
//	}
	

}

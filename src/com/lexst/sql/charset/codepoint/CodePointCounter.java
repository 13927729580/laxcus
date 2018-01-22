/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.charset.codepoint;

import java.util.*;
import java.io.*;

import com.lexst.sql.schema.*;
import com.lexst.util.*;

/**
 * 统计字符类型，首字符代码位出现的次数
 * 
 * 这个类用在DATA节点上
 */
public class CodePointCounter {

	/** 表区间 */
	private Docket docket;
	
	/** UTF16码位 -> UTF16码位出现的频率统计   **/
	private Map<java.lang.Integer, java.lang.Integer> codes = new TreeMap<java.lang.Integer, java.lang.Integer>();
	
	/**
	 * default
	 */
	public CodePointCounter() {
		super();
	}

	/**
	 * @param docket
	 */
	public CodePointCounter(Docket docket) {
		this();
		this.setDocket(docket);
	}
	
	/**
	 * 设置表区间
	 * @param d
	 */
	public void setDocket(Docket d) {
		this.docket = new Docket(d);
	}

	/**
	 * 返回表区间
	 * @return
	 */
	public Docket getDocket() {
		return this.docket;
	}

	/**
	 * 记录代码位
	 * 
	 * @param codePoint
	 */
	public void add(int codePoint) {
		java.lang.Integer count = codes.get(codePoint);
		if (count == null) {
			codes.put(new java.lang.Integer(codePoint), new java.lang.Integer(1));
		} else {
			// 限定最大值
			if (count < Integer.MAX_VALUE) count++;
		}
	}


	public Set<Integer> keySet() {
		return codes.keySet();
	}

	public java.util.Collection<Integer> values() {
		return codes.values();
	}

	public Integer find(Integer codePoint) {
		return codes.get(codePoint);
	}

	public int size() {
		return codes.size();
	}

	public boolean isEmpty() {
		return codes.isEmpty();
	}

	/**
	 * 返回代码位集合
	 * @return
	 */
	public int[] paris() {
		int size = codes.size();
		Iterator<Map.Entry<Integer, Integer>> iterators = codes.entrySet().iterator();
		int[] array = new int[size * 2];
		int index = 0;
		while (iterators.hasNext()) {
			Map.Entry<Integer, Integer> entry = iterators.next();
			array[index] = entry.getKey();
			array[index + 1] = entry.getValue();
			index += 2;
		}
		return array;
	}
	
	/**
	 * 生成数据流
	 * 
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);

		String schema = docket.getSchema();
		int schemaSize = (schema.getBytes().length & 0xFF);
		String table = docket.getTable();
		int tableSize = (table.getBytes().length & 0xFF);

		byte[] b = Numeric.toBytes(schemaSize);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(tableSize);
		buff.write(b, 0, b.length);

		b = schema.getBytes();
		buff.write(b, 0, b.length);
		b = table.getBytes();
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(docket.getColumnId());
		buff.write(b, 0, b.length);

		// 代码位成员数
		b = Numeric.toBytes(codes.size());
		buff.write(b, 0, b.length);
		// 写入代码位和统计数
		for (Integer codePoint : codes.keySet()) {
			b = Numeric.toBytes(codePoint.intValue());
			buff.write(b, 0, b.length);
			Integer count = codes.get(codePoint);
			b = Numeric.toBytes(count.intValue());
			buff.write(b, 0, b.length);
		}

		byte[] data = buff.toByteArray();
		b = Numeric.toBytes(4 + data.length); // 全部长度

		byte[] result = new byte[4 + data.length];
		System.arraycopy(b, 0, result, 0, b.length);
		System.arraycopy(data, 0, result, b.length, data.length);

		return result;
	}

	/**
	 * 解析数据流。成功返回解析的字节流长度，失败返回-1
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		// 数据流总长度
		int size = Numeric.toInteger(b, seek, 4);
		if (seek + size > end) {
			throw new SizeOutOfBoundsException("codepoint sizeout!");
		}
		seek += 4;

		// 表空间定义
		if (seek + 8 > end) {
			throw new SizeOutOfBoundsException("deck sizeout!");
		}
		int schemaSize = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int tableSize = Numeric.toInteger(b, seek, 4);
		seek += 4;

		if (seek + schemaSize + tableSize + 2 > end) {
			throw new SizeOutOfBoundsException("deck sizeout!");
		}
		String schema = new String(b, seek, schemaSize);
		seek += schemaSize;
		String table = new String(b, seek, tableSize);
		seek += tableSize;
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		docket = new Docket(schema, table, columnId);

		// 代码位成员数量
		if(seek + 4 > end) {
			throw new SizeOutOfBoundsException("codepoint sizeout!");
		}
		int elements = Numeric.toInteger(b, seek, 4);
		seek += 4;
		
		// 扫描全部代码位
		if (seek + elements * 8 > end) {
			throw new SizeOutOfBoundsException("codepoint sizeout!");
		}
		for (int i = 0; i < elements; i++) {
			int codePoint = Numeric.toInteger(b, seek, 4);
			seek += 4;
			int count = Numeric.toInteger(b, seek, 4);
			seek += 4;
			codes.put(new java.lang.Integer(codePoint), new java.lang.Integer(count));
		}

		return seek - off;
	}
	
}
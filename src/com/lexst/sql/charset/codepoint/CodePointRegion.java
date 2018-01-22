/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.charset.codepoint;

import java.io.*;
import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.util.*;
import com.lexst.util.range.*;

/**
 * 一个表中的字符类型的所有首字符代码位分布范围，用于HOME节点
 * 
 */
public class CodePointRegion {

	/** 表区间 */
	private Docket docket;
	
	/** 码位范围 -> 统计值 */
	private Map<IntegerRange, java.lang.Integer> ranges = new TreeMap<IntegerRange, java.lang.Integer>();

	/**
	 * default
	 */
	public CodePointRegion() {
		super();
	}

	/**
	 * @param docket
	 */
	public CodePointRegion(Docket docket) {
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
	 * 接受来自DATA节点的数据，保存一个代码位和它的统计值
	 * 
	 * @param codePoint
	 * @param count
	 */
	public void add(int codePoint, int count) {
		for (IntegerRange range : ranges.keySet()) {
			if (range.inside(codePoint)) {
				java.lang.Integer value = ranges.get(range);
				value += count;
				return;
			} else if (count + 1 == range.getBegin()) {
				java.lang.Integer value = ranges.get(range);
				IntegerRange it = new IntegerRange(count, range.getEnd());
				ranges.remove(range);
				ranges.put(it, value + count);
				return;
			} else if (range.getEnd() + 1 == count) {
				java.lang.Integer value = ranges.get(range);
				IntegerRange it = new IntegerRange(range.getBegin(), count);
				ranges.remove(range);
				ranges.put(it, value + count);
				return;
			}
		}

		ranges.put(new IntegerRange(codePoint, codePoint), count);
	}
	
	/**
	 * 返回代码位统计的整数集合
	 * 
	 * @return
	 */
	public int[] paris() {
		int size = ranges.size();
		int[] array = new int[size * 3];
		int index = 0;
		Iterator<Map.Entry<IntegerRange, Integer>> iterators = ranges.entrySet().iterator();
		while (iterators.hasNext()) {
			Map.Entry<IntegerRange, Integer> entry = iterators.next();
			array[index] = entry.getKey().getBegin();
			array[index + 1] = entry.getKey().getEnd();
			array[index + 2] = entry.getValue().intValue();
			index += 3;
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

		String schema = docket.getSpace().getSchema();
		int schemaSize = (schema.getBytes().length & 0xFF);
		String table = docket.getSpace().getTable();
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

		// 代码位范围成员数
		b = Numeric.toBytes( ranges .size());
		buff.write(b, 0, b.length);
		// 写入代码位范围和统计值
		for(IntegerRange range: ranges.keySet()) {
			b = Numeric.toBytes(  range.getBegin() );
			buff.write(b, 0, b.length);
			b = Numeric.toBytes(range.getEnd());
			buff.write(b, 0, b.length);
			Integer count = ranges.get(range);
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

		// 数据流总长度
		int size = Numeric.toInteger(b, seek, 4);
		if (seek + size > end) return -1;
		seek += 4;

		// 表空间定义
		if(seek + 8 > end) return -1;
		int schemaSize = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int tableSize = Numeric.toInteger(b, seek, 4);
		seek += 4;

		if (seek + schemaSize + tableSize + 2 > end) return -1;
		String schema = new String(b, seek, schemaSize);
		seek += schemaSize;
		String table = new String(b, seek, tableSize);
		seek += tableSize;
		short columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		this.docket = new Docket(new Space(schema, table), columnId);

		// 代码位成员数量
		if(seek + 4 > end) return -1;
		int elements = Numeric.toInteger(b, seek, 4);
		seek += 4;
		
		// 扫描全部代码位
		if (seek + elements * 12 > end) return -1;
		for (int i = 0; i < elements; i++) {
			int beginPoint = Numeric.toInteger(b, seek, 4);
			seek += 4;
			int endPoint = Numeric.toInteger(b, seek, 4);
			seek += 4;
			int count = Numeric.toInteger(b, seek, 4);
			seek += 4;

			// 保存代码位
			ranges.put(new IntegerRange(beginPoint, endPoint), new java.lang.Integer(count));
		}

		return seek - off;
	}

}
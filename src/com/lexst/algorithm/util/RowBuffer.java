/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.util;

import java.io.*;
import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;

/**
 * 数据分片后，存储所在区域的行记录
 *
 */
public final class RowBuffer {
	
	/** 下标编号 */
	private int index;

	/** 对应的数据表 */
	private Space space;

	/** 记录集合 */
	private List<Row> array = new ArrayList<Row>();

	/**
	 * default
	 */
	public RowBuffer(int index, Space space) {
		super();
		this.setIndex(index);
		this.setSpace(space);
	}

	/**
	 * 设置数据区域分片下标
	 * @param i
	 */
	public void setIndex(int i) {
		this.index = i;
	}
	
	/**
	 * 返回数据区域分片下标
	 * @return
	 */
	public int getIndex() {
		return this.index;
	}
	
	/**
	 * 设置数据库表名
	 * @param s
	 */
	public void setSpace(Space s) {
		this.space = new Space(s);
	}
	
	/**
	 * 返回数据库表名
	 * @return
	 */
	public Space getSpace() {
		return this.space;
	}

	/**
	 * 保存一行记录
	 * @param row
	 * @return
	 */
	public boolean add(Row row) {
		return array.add(row);
	}
	
	/**
	 * 产生数据流(确定是行存储模式)
	 * @return
	 */
	public byte[] build() {
		// 数据头部信息(输出的数据流，确定是行存储模式)
		AnswerFlag flag = new AnswerFlag();
		flag.setRows(array.size());
		flag.setColumns((short) array.get(0).size());
		flag.setStorage(Type.NSM);
		flag.setSpace(space);
		byte[] b = flag.build();
		
		// 确定分配所需空间
		int total = b.length;
		for(Row row : array) {
			total += row.capacity();
		}

		// 开辟内存空间
		ByteArrayOutputStream buff = new ByteArrayOutputStream(total - total % 32 + 32);
		buff.write(b, 0, b.length);
		for (Row row : array) {
			row.build(buff);
		}

		// 输出数据流
		byte[] data = buff.toByteArray();
		// 更新数据头部信息
		flag.setSize(data.length - b.length);
		b = flag.build();
		System.arraycopy(b, 0, data, 0, b.length);

		return data;
	}
}
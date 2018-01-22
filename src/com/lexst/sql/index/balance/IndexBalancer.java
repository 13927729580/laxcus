/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.balance;

import java.io.*;
import java.lang.Number;

import com.lexst.sql.index.range.*;
import com.lexst.sql.index.section.*;

/**
 * 平均索引数量生成器。<br>
 * 
 * 算法平衡分两步: <br>
 * 
 * <1> 调用add方法， 向内存中添加分片和统计值，转为IndexZone子类保存 <br>
 * 
 * <2> 调用balance方法，平均分布数据域，输出ColumnSector的子集, ColumnSector子集包含分片结果 <br>
 */
public interface IndexBalancer extends Serializable {

	/**
	 * 增加一个分片区，包括数据范围和频率(权重)
	 * 
	 * @param zone
	 * @return
	 */
	boolean add(IndexZone zone);

	/**
	 * 增加一个分片区
	 * 
	 * @param range
	 * @param weight
	 * @return
	 */
	boolean add(IndexRange range, int weight);

	/**
	 * 增加一个分片区
	 * 
	 * @param begin
	 * @param end
	 * @param weight
	 * @return
	 */
	boolean add(Number begin, Number end, int weight);

	/**
	 * 按照节点数平均分割数据区，返回一个分割后的结果
	 * 
	 * @param sites
	 * @return
	 */
	ColumnSector balance(int sites);
}
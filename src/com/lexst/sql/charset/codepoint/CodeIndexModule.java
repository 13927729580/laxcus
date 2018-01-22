/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.charset.codepoint;

import com.lexst.sql.index.balance.*;
import com.lexst.sql.schema.*;
import com.lexst.util.range.*;

import java.util.*;
import java.io.Serializable;

/**
 * 代码位针对字符串类型，通过代码位做数据分片处理
 * 
 * 一个表存在有组表区间索引分组，用于CALL/WORK节点
 */
public class CodeIndexModule implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 表区间 **/
	private Docket docket;
	
	/** 代码位分布范围 **/
	private ArrayList<IntegerZone> array = new ArrayList<IntegerZone>();
	
	/**
	 * @param docket
	 */
	public CodeIndexModule(Docket docket) {
		super();
		this.setDocket(docket);
	}

	/**
	 * 设置表区间名称
	 * @param d
	 */
	public void setDocket(Docket d) {
		this.docket = new Docket(d);
	}

	/**
	 * 返回表区间名称
	 * @return
	 */
	public Docket getDocket() {
		return this.docket;
	}
	
	/**
	 * 增加代码位范围
	 * @param range
	 * @param weight
	 */
	public void add(IntegerRange range, int weight) {
		array.add(new IntegerZone(range, weight));
		java.util.Collections.sort(array);
	}

	/**
	 * 增加代码位范围
	 * 
	 * @param begin
	 * @param end
	 * @param weigth
	 */
	public void add(int begin, int end, int weigth) {
		this.add(new IntegerRange(begin, end), weigth);
	}

	/**
	 * 输出
	 * @return
	 */
	public IntegerZone[] array() {
		IntegerZone[] s = new IntegerZone[array.size()];
		return array.toArray(s);
	}
	
	/**
	 * 收缩至有效空间
	 */
	public void trim() {
		array.trimToSize();
	}
}
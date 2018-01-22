/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.balance;

import java.io.*;
import java.math.*;

/**
 * 索引分布区域。<br>
 * 由数值范围和此范围中值的出现频率(权重)组成。<br>
 * 
 */
public class IndexZone implements Serializable, Cloneable {

	private static final long serialVersionUID = 7756469198642437568L;

	/** 范围存在次数统计值 **/
	private int weight;

	/**
	 * default
	 */
	public IndexZone() {
		super();
	}

	/**
	 * @param zone
	 */
	public IndexZone(IndexZone zone) {
		this();
		this.weight = zone.weight;
	}

	/**
	 * 范围出现频率
	 * 
	 * @param i
	 */
	public void setWeight(int i) {
		this.weight = i;
	}

	/**
	 * 出现频率统计
	 * 
	 * @return
	 */
	public int getWeight() {
		return this.weight;
	}

	/**
	 * 增加统计值，如果达到最大值时，限定为整型最大值
	 * 
	 * @param value
	 */
	public void addWeight(int value) {
		if (BigInteger.valueOf(weight).add(BigInteger.valueOf(value))
				.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
			this.weight = Integer.MAX_VALUE;
		} else {
			this.weight += value;
		}
	}

}
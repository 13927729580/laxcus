/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.select;

import java.io.*;
import java.util.*;

/**
 * 对应SQL "GROUP BY"的类实现
 * 
 */
public class GroupBy implements Serializable, Cloneable {

	private static final long serialVersionUID = -5206559160419457714L;

	/** 列标识集合 */
	private List<java.lang.Short> array = new ArrayList<java.lang.Short>();

	/** "HAVING"子句实例 */
	private Situation situation;

	/**
	 * default
	 */
	public GroupBy() {
		super();
	}

	/**
	 * 
	 * @param by
	 */
	public GroupBy(GroupBy by) {
		this();
		this.array.addAll(by.array);
		if (by.situation != null) {
			this.situation = new Situation(by.situation);
		}
	}

	public void addGroupId(short columnId) {
		array.add(columnId);
	}

	public short[] listGroupIds() {
		short[] s = new short[array.size()];
		for (int i = 0; i < s.length; i++) {
			s[i] = array.get(i).shortValue();
		}
		return s;
	}

	public void setSituation(Situation s) {
		this.situation = s;
	}

	public Situation getSituation() {
		return this.situation;
	}

	public byte[] build() {
		return null;
	}

	public int resolve(byte[] b, int off, int len) {
		return 0;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new GroupBy(this);
	}
}
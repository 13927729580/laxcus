/**
 * 
 */
package com.lexst.sql.parse.result;

import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.util.host.*;

public final class RebuildHostResult {

	/** 表名 */
	private Space space;

	/** 指定索引键 */
	private short columnId;

	/** 指定的DATA节点IP地址集合 */
	private List<Address> array = new ArrayList<Address>();

	/**
	 * 
	 */
	public RebuildHostResult() {
		super();
	}

	/**
	 * @param s
	 */
	public RebuildHostResult(Space s) {
		this();
		this.setSpace(s);
	}

	/**
	 * @param s
	 * @param id
	 */
	public RebuildHostResult(Space s, short id) {
		this(s);
		this.setColumnId(id);
	}

	public void setSpace(Space s) {
		space = new Space(s);
	}

	public Space getSpace() {
		return space;
	}

	/**
	 * 索引键
	 * 
	 * @param id
	 */
	public void setColumnId(short id) {
		this.columnId = id;
	}

	public short getColumnId() {
		return this.columnId;
	}

	public Address[] getAddresses() {
		Address[] s = new Address[array.size()];
		return array.toArray(s);
	}

	public boolean addAddresses(Collection<Address> s) {
		return this.array.addAll(s);
	}

}
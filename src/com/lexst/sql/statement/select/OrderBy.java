/**
 *
 */
package com.lexst.sql.statement.select;

import java.io.Serializable;
import java.util.*;

/**
 * SQL "ORDER BY" 类实现
 * 
 * 
 */
public class OrderBy implements Serializable, Cloneable {

	private static final long serialVersionUID = 3286440658449608412L;

	/** 升序排列(ascent sort) */
	public final static int ASC = 1;
	/** 附序排列(descent sort) */
	public final static int DESC = 2;

	/** 列标识 **/
	private short columnId;

	/** 列排序标识 **/
	private int type;

	/** 关联ORDER BY实例 */
	private OrderBy next;

	/**
	 *
	 */
	public OrderBy() {
		super();
	}

	/**
	 * @param columnId
	 * @param type
	 */
	public OrderBy(short columnId, int type) {
		this();
		this.set(columnId, type);
	}

	/**
	 * @param order
	 */
	public OrderBy(OrderBy order) {
		this();
		this.columnId = order.columnId;
		this.type = order.type;
		if (order.next != null) {
			this.next = new OrderBy(order.next);
		}
	}

	public void set(short columnId, int type) {
		if (type != OrderBy.ASC && type != OrderBy.DESC) {
			throw new java.lang.IllegalArgumentException("sort type error!");
		}
		this.columnId = columnId;
		this.type = type;
	}

	public short getColumnId() {
		return this.columnId;
	}

	/**
	 * 返回全部列标识号
	 * 
	 * @return
	 */
	public short[] listColumnIds() {
		ArrayList<java.lang.Short> a = new ArrayList<java.lang.Short>();
		a.add(columnId);
		OrderBy sub = this.next;
		while (sub != null) {
			a.add(sub.columnId);
			sub = sub.next;
		}

		short[] s = new short[a.size()];
		for (int i = 0; i < s.length; i++) {
			s[i] = a.get(i).shortValue();
		}
		return s;
	}

	public int getType() {
		return type;
	}

	public boolean isASC() {
		return this.type == OrderBy.ASC;
	}

	public boolean isDESC() {
		return this.type == OrderBy.DESC;
	}

	public void setLast(OrderBy object) {
		if (this.next == null) {
			this.next = object;
		} else {
			this.next.setLast(object);
		}
	}

	public OrderBy getLast() {
		if (next != null) {
			return next.getLast();
		}
		return this;
	}

	public OrderBy getNext() {
		return this.next;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public OrderBy clone() {
		return new OrderBy(this);
	}
}
/**
 *
 */
package com.lexst.sql.statement;

import java.util.*;

import com.lexst.sql.column.*;
import com.lexst.sql.schema.*;

public class Update extends Query {

	private static final long serialVersionUID = 1L;

	/** 被更新的列 **/
	private ArrayList<Column> array = new ArrayList<Column>(5);

	/**
	 * default
	 */
	public Update() {
		super(Compute.UPDATE_METHOD);
	}

	/**
	 * 
	 * @param space
	 */
	public Update(Space space) {
		this();
		this.setSpace(space);
	}
	
	/**
	 * @param update
	 */
	public Update(Update update) {
		super(update);
		array.addAll(update.array);
	}

	/**
	 * 增加一列更新参数
	 * @param column
	 */
	public void add(Column column) {
		array.add(column);
	}

	/**
	 * 返回更新参数集
	 * @return
	 */
	public List<Column> values() {
		return array;
	}

	/*
	 * 复制Update对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Update(this);
	}
}
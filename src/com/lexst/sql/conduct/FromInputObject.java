/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;

/**
 * 
 * 
 */
public class FromInputObject extends InputObject {

	private static final long serialVersionUID = -3608413216604485140L;
	
	/** 被检索的SQL SELECT集合(允许一次"diffuse"操作分别执行多个SELECT) */
	private List<Select> array = new ArrayList<Select>(3);

	/**
	 * default
	 */
	public FromInputObject() {
		super();
	}

	/**
	 * 
	 * @param inputor
	 */
	public FromInputObject(FromInputObject inputor) {
		super(inputor);
		this.array.addAll(inputor.array);
	}

	/**
	 * 保存一个SELECT命令
	 * 
	 * @param select
	 * @return
	 */
	public boolean addSelect(Select select) {
		return this.array.add(select);
	}

	/**
	 * 返回SELECT集合
	 * 
	 * @return
	 */
	public List<Select> getSelects() {
		return this.array;
	}

	/**
	 * 返回SELECT的表名集合
	 * 
	 * @return
	 */
	public List<Space> getSelectSpaces() {
		Set<Space> set = new TreeSet<Space>();
		for (Select select : array) {
			set.add(select.getSpace());
		}
		return new ArrayList<Space>(set);
	}

	/**
	 * 返回指定下标的"SQL SELECT"
	 * 
	 * @param index
	 * @return
	 */
	public Select getSelect(int index) {
		if (index < 0 || index >= this.array.size()) {
			return null;
		}
		return this.array.get(index);
	}

	/**
	 * 清除全部"SQL SELECT"
	 */
	public void clearSelects() {
		this.array.clear();
	}

	/**
	 * 统计"SQL SELECT"成员数
	 * 
	 * @return
	 */
	public int countSelect() {
		return this.array.size();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.distribute.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new FromInputObject(this);
	}
}
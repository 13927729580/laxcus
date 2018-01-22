/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.select;

import java.util.*;

import com.lexst.sql.row.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.function.*;

/**
 * SQL "HAVING"子句的类实现<br>
 * 
 */
public class Situation extends Gradation {
	private static final long serialVersionUID = 1L;
	
	/** 聚合函数(只允许聚合函数) **/
	private ColumnFunction function;

	/** 被比较的结果 */
	private SQLValue value;

	/** 同级比较 单元 */
	private List<Situation> partners = new ArrayList<Situation>(3);;

	/** 关联比较单元 **/
	private Situation next;

	/**
	 * default
	 */
	public Situation() {
		super();
	}

	/**
	 * @param function
	 * @param compare
	 * @param value
	 */
	public Situation(ColumnFunction function, byte compare, SQLValue value) {
		this();
		this.setFunction(function);
		super.setCompare(compare);
		this.setValue(value);
	}

	/**
	 * copy origin
	 * 
	 * @param situation
	 */
	public Situation(Situation situation) {
		super(situation);
		this.function = (ColumnFunction)situation.function.clone();		
		this.value = situation.value.clone();
		for (Situation partner : situation.partners) {
			partners.add((Situation) partner.duplicate());
		}
		if (situation.next != null) {
			next = new Situation(situation.next);
		}
	}
	
	public void setFunction(ColumnFunction def) {
		this.function = def;
	}
	
	public ColumnFunction getFunction() {
		return this.function;
	}

	public void setValue(SQLValue arg) {
		this.value = arg.clone();
	}
	public SQLValue getValue() {
		return this.value;
	}

	/**
	 * 筛选操作。根据行记录集合，执行函数计算，再比较结果是否成立
	 * 
	 * @param rows
	 * @return
	 */
	public boolean sifting(List<Row> rows) {
		SQLValue result = function.compute(rows);
		boolean ret = false;
		switch (super.getCompare()) {
		case Gradation.EQUAL:
			ret = (result.compareTo(value) == 0);
			break;
		case Gradation.NOT_EQUAL:
			ret = (result.compareTo(value) != 0);
			break;
		case Gradation.LESS:
			ret = (result.compareTo(value) < 0);
			break;
		case Gradation.LESS_EQUAL:
			ret = (result.compareTo(value) <= 0);
			break;
		case Gradation.GREATER:
			ret = (result.compareTo(value) > 0);
			break;
		case Gradation.GREATER_EQUAL:
			ret = (result.compareTo(value) >= 0);
			break;
		}
		return ret;
	}

	public void addPartner(Situation partner) {
		this.partners.add(partner);
	}

	public List<Situation> getPartners() {
		return this.partners;
	}

	public void setLast(Situation having) {
		if(next == null) {
			this.next = having;
		} else {
			this.next.setLast(having);
		}
	}

	public Situation getNext() {
		return this.next;
	}

	public Situation getLast() {
		if(next != null) {
			return next.getLast();
		}
		return this;
	}

	/*
	 * 复制Situation对象
	 * 
	 * @see com.lexst.sql.statement.Gradation#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new Situation(this);
	}
}
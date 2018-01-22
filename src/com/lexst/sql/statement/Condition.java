/**
 *
 */
package com.lexst.sql.statement;

import java.util.*;

import com.lexst.sql.index.*;

/**
 * SQL WHERE 检索条件
 * 
 */
public class Condition extends Gradation {
	
	private static final long serialVersionUID = 1L;
	
	/** 被检索的列名称 **/
	private String columnName;

	/** 被比较参数 **/
	private WhereIndex value;

	/** 同级关联单元 **/
	private List<Condition> partners = new ArrayList<Condition>(3);

	/** 下一级(从级)检索条件 **/
	private Condition next;

	/**
	 * default
	 */
	public Condition() {
		super();
	}

	/**
	 * @param condi
	 */
	public Condition(Condition condi) {
		super(condi);
		this.setColumnName(condi.columnName);
		this.value = (WhereIndex) condi.value.clone();
		for (Condition partner : condi.partners) {
			partners.add((Condition) partner.duplicate());
		}
		if (condi.next != null) {
			next = new Condition(condi.next);
		}
	}

	/**
	 * @param name
	 * @param compare
	 * @param value
	 */
	public Condition(String name, byte compare, WhereIndex value) {
		this();
		this.setColumnName(name);
		this.setCompare(compare);
		this.setValue(value);
	}

	/**
	 * @param related
	 * @param name
	 * @param compare
	 * @param value
	 */
	public Condition(byte related, String name, byte compare, WhereIndex value) {
		this();
		super.setRelation(related);
		this.setColumnName(name);
		this.setCompare(compare);
		this.setValue(value);
	}

	/**
	 * 设置检索列名
	 * @param name
	 */
	public void setColumnName(String name) {
		this.columnName = name;
	}

	/**
	 * 返回检索列名
	 * 
	 * @return
	 */
	public String getColumnName() {
		return this.columnName;
	}

	/**
	 * 返回检索的列ID
	 * @return
	 */
	public short getColumnId() {
		return value.getColumnId();
	}

	/**
	 * 设置检索值
	 * @param index
	 */
	public void setValue(WhereIndex index) {
		this.value = index;
	}
	
	/**
	 * 返回检索值
	 * @return
	 */
	public WhereIndex getValue() {
		return this.value;
	}

	/**
	 * 设置同级检索条件
	 * @param partner
	 */
	public void addPartner(Condition partner) {
		this.partners.add(partner);
	}

	/**
	 * 返回同级检索条件集合
	 * @return
	 */
	public List<Condition> getPartners() {
		return this.partners;
	}

	/**
	 * 设置最后的检索条件(相对于当前Condition)
	 * @param condi
	 */
	public void setLast(Condition condi) {
		if(this.next == null) {
			this.next = condi;
		} else {
			this.next.setLast(condi);
		}
	}

	/**
	 * 返回子级检索条件
	 * 
	 * @return
	 */
	public Condition getNext() {
		return this.next;
	}

	/**
	 * 返回最后的检索条件
	 * 
	 * @return
	 */
	public Condition getLast() {
		if(next != null) {
			return next.getLast();
		}
		return this;
	}
	
	/**
	 * 是否包含SELECT子检索
	 * 
	 * @return
	 */
	public boolean onSubSelect() {
		//1. 查找子级
		boolean existed = false;
		if (this.next != null) {
			existed = next.onSubSelect();
		}
		//2. 子级未到找情况下找同级从属条件
		if (!existed) {
			for (Condition condi : this.partners) {
				if (condi.onSubSelect()) {
					existed = true; break;
				}
			}
		}
		//3. 前两项不成功，检查本例
		if (!existed) {
			existed = (value != null && value.isSelectType());
		}

		return existed;
	}

	/** 
	 * 递归查找最后一个索引类型匹配的条件 <br>
	 * 三步优先级检查: <br>
	 * <1> 子级检查 (最高) <br>
	 * <2> 伙伴级检查 (其次) <br>
	 * <3> 自参数检查 (再次) <br>
	 * 
	 * @param source
	 * @return
	 */
	public static Condition findLastSelectCondition(Condition source) {
		Condition condi = null;
		//1. 首先找最后
		if (source.next != null) {
			condi = Condition.findLastSelectCondition( source.next );
		}
		//2. 在未到情况下，找伙伴级最后一个
		if (condi == null) {
			// 找到最后一个
			for (Condition partner : source.partners) {
				Condition sub = Condition.findLastSelectCondition(partner);
				if (sub != null) condi = sub;
			}
		}
		// 前两项没找到，判断本例
		if (condi == null && source.value != null && source.value.isSelectType()) {
			Select select = ((SelectIndex) source.value).getSelect();
			Condition slave = select.getCondition();
			
			// 两种情况: <1>有子条件 <2>如果没有子条件就是本例
			Condition sub = Condition.findLastSelectCondition(slave);
			if(sub != null) condi = sub;
			else condi = source;
		}

		return condi;
	}
	
	/*
	 * 复制Condition对象
	 * @see com.lexst.sql.statement.Gradation#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new Condition(this);
	}
	
}
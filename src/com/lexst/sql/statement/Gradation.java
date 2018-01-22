/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement;

import java.io.*;

/**
 * 列、函数的连接/比较对照<br><br>
 * 
 * 语法描述: <br>
 * column-name1 > 122 and column_name2 = 'abc'<br>
 * function(column-name) = 'little' <br>
 *
 */
public abstract class Gradation implements Serializable {
	
	private static final long serialVersionUID = -7379489096040391197L;
	
	/** 相邻单元的逻辑连接关系(AND|OR) **/
	public final static byte NONE = 0;
	public final static byte AND = 1;
	public final static byte OR = 2;

	/** 列/函数与数值之间的比较关系 **/
	public final static byte EQUAL = 1;
	public final static byte NOT_EQUAL = 2;
	public final static byte LESS = 3;
	public final static byte LESS_EQUAL = 4;
	public final static byte GREATER = 5;
	public final static byte GREATER_EQUAL = 6;
	public final static byte LIKE = 7;

	public final static byte ISNULL = 8;
	public final static byte NOTNULL = 9;
	public final static byte ISEMPTY = 10;
	public final static byte NOTEMPTY = 11;
	
	public final static byte IN = 12;
	
	/** 本单元与下一单元的连接关系  **/
	protected byte outsideRelation;
	/** 单元内，同级关联条件之间的关系 **/
	protected byte relation;
	/** 比较关系(等于,不等于,大于,大于等于,小于,小于等于,等于空,不等于空...) **/
	protected byte compare;

	/**
	 * default
	 */
	public Gradation() {
		super();
		this.outsideRelation = Gradation.NONE;
		this.relation = Gradation.NONE;
		this.compare = 0;
	}

	/**
	 * @param grada
	 */
	public Gradation(Gradation grada) {
		this();
		this.outsideRelation = grada.outsideRelation;
		this.relation = grada.relation;
		this.compare = grada.compare;
	}

	/**
	 * 解释比较字符
	 * 
	 * @param s
	 * @return
	 */
	public static byte translateCompare(String s) {
		if(s == null) {
			throw new NullPointerException("null pointer error!");
		}
		
		if (s.matches("^\\s*(?i)(?:>)\\s*$")) {
			return Gradation.GREATER;
		} else if (s.matches("^\\s*(?i)(?:>=)\\s*$")) {
			return Gradation.GREATER_EQUAL;
		} else if (s.matches("^\\s*(?i)(?:<)\\s*$")) {
			return Gradation.LESS;
		} else if (s.matches("^\\s*(?i)(?:<=)\\s*$")) {
			return Gradation.LESS_EQUAL;
		} else if (s.matches("^\\s*(?i)(?:=)\\s*$")) {
			return Gradation.EQUAL;
		} else if (s.matches("^\\s*(?i)(?:<>|!=)\\s*$")) {
			return Gradation.NOT_EQUAL;
		} else if (s.matches("^\\s*(?i)LIKE\\s*$")) {
			return Gradation.LIKE;
		} else if (s.matches("^\\s*(?i)(IS\\s+NULL)\\s*$")) {
			return Gradation.ISNULL;
		} else if (s.matches("^\\s*(?i)(IS\\s+NOT\\s+NULL)\\s*$")) {
			return Gradation.NOTNULL;
		} else if (s.matches("^\\s*(?i)(IS\\s+EMPTY)\\s*$")) {
			return Gradation.ISEMPTY;
		} else if (s.matches("^\\s*(?i)(IS\\s+NOT\\s+EMPTY)\\s*$")) {
			return Gradation.NOTEMPTY;
		} else if (s.matches("^\\s*(?i)IN\\s*$")) {
			return Gradation.IN;
		}
		
		throw new IllegalArgumentException("invalid compare!" + s);		
	}

	/**
	 * 解释比较字符
	 * 
	 * @param s
	 * @return
	 */
	public static String translateCompare(byte s) {
		switch (s) {
		case Gradation.EQUAL:
			return "=";
		case Gradation.NOT_EQUAL:
			return "<>";
		case Gradation.LESS:
			return "<";
		case Gradation.LESS_EQUAL:
			return "<=";
		case Gradation.GREATER:
			return ">";
		case Gradation.GREATER_EQUAL:
			return ">=";
		case Gradation.LIKE:
			return "LIKE";
		case Gradation.ISNULL:
			return "IS NULL";
		case Gradation.NOTNULL:
			return "IS NOT NULL";
		case Gradation.ISEMPTY:
			return "IS EMPTY";
		case Gradation.NOTEMPTY:
			return "IS NOT EMPTY";
		case Gradation.IN:
			return "IN";
		}
		return "";		
	}
	
	/**
	 * 解释逻辑字符
	 * 
	 * @param s
	 * @return
	 */
	public static byte translateLogic(String s) {
		if (s == null) {
			throw new NullPointerException("null pointer error");
		}
		if (s.matches("^\\s*(?i)AND\\s*$")) {
			return Gradation.AND;
		} else if (s.matches("^\\s*(?i)OR\\s*$")) {
			return Gradation.OR;
		}
		throw new IllegalArgumentException("illegal relate:" + s);
	}
	
	/**
	 * 转换逻辑字符
	 * 
	 * @param w
	 * @return
	 */
	public static String translateLogic(byte w) {
		switch(w) {
		case Gradation.AND:
			return "AND";
		case Gradation.OR:
			return "OR";
		}
		return "";
	}

	/**
	 * 设置比较值
	 * @param i
	 */
	public void setCompare(byte i) {
		if (Gradation.EQUAL <= i && i <= Gradation.IN) {
			this.compare = i;
		} else {
			throw new IllegalArgumentException("invalid compare value!");
		}
	}

	/**
	 * 返回比较值
	 * @return
	 */
	public byte getCompare() {
		return this.compare;
	}

	/**
	 * 设置外部逻辑关系
	 * @param b
	 */
	public void setOutsideRelation(byte b) {
		if (Gradation.NONE <= b && b <= Gradation.OR) {
			this.outsideRelation = b;
		} else {
			throw new IllegalArgumentException("invalid outside relate");
		}
	}

	/**
	 * 返回外部逻辑关系
	 * @return
	 */
	public byte getOutsideRelation() {
		return this.outsideRelation;
	}

	/**
	 * 设置同级逻辑连接关系
	 * @param b
	 */
	public void setRelation(byte b) {
		if (Gradation.NONE <= b && b <= Gradation.OR) {
			this.relation = b;
		} else {
			throw new IllegalArgumentException("invalid relate");
		}
	}
	
	/**
	 * 返回同级逻辑连接关系
	 * @return
	 */
	public byte getRelation() {
		return this.relation;
	}

	/**
	 * "与"逻辑关系
	 * @return
	 */
	public boolean isAND() {
		return this.relation == Gradation.AND;
	}

	/**
	 * "或"逻辑关系
	 * @return
	 */
	public boolean isOR() {
		return this.relation == Gradation.OR;
	}

	/**
	 * 无逻辑联系
	 * @return
	 */
	public boolean isNotRelate() {
		return this.relation == Gradation.NONE;
	}
	
	/*
	 * 复制对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return duplicate();
	}

	/**
	 * 复制对象，具体由子类实现
	 * @return
	 */
	public abstract Object duplicate();
}
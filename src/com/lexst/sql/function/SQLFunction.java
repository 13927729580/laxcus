/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import java.io.*;

import com.lexst.sql.schema.*;

/**
 * SQL函数基础类，定义基本操作接口。
 * 
 */
public abstract class SQLFunction implements Serializable, Cloneable {

	private static final long serialVersionUID = 5293991279164992598L;

	/** 计算结果的值类型，对应ColumnAttribute中的类型定义 **/
	protected byte returnType;

	/** 原语描述，来自正则表达式的解析 **/
	protected String description;

	/**
	 * default
	 */
	protected SQLFunction() {
		super();
		this.returnType = 0; //由子类定义
	}

	/**
	 * 复制成员
	 * 
	 * @param function
	 */
	protected SQLFunction(SQLFunction function) {
		this();
		this.setDescription(function.description);
		this.setReturnType(function.returnType);
	}

	/**
	 * 设置SQL原语
	 * @param s
	 */
	protected void setDescription(String s) {
		this.description = s;
	}

	/**
	 * 返回SQL原语
	 * @return
	 */
	public String getDescription() {
		return this.description;
	}

	/**
	 * 返回返回值数据类型
	 * @return
	 */
	public byte getReturnType() {
		return this.returnType;
	}

	/**
	 * 设置返回值的数据类型
	 * @param b
	 */
	protected void setReturnType(byte b) {
		this.returnType = b;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return duplicate();
	}

	/**
	 * 复制，由子类实现
	 * @return
	 */
	public abstract SQLFunction duplicate();

	/**
	 * 根据子类定义的正则表达式，解析SQL描述语句，生成子类实例<br>
	 * @param table
	 * @param cmd
	 * @return
	 */
	public abstract SQLFunction create(Table table, String cmd);

	/**
	 * 根据传入的值(可以是NULL)，计算结果(数据类型是ReturnType)
	 * 
	 * @param value
	 * @return
	 * @throws SQLFunctionException
	 */
	public abstract SQLValue compute(SQLValue value);
}
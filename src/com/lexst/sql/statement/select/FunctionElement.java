/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.select;

import com.lexst.sql.function.*;

/**
 * SQL函数的列成员
 *
 */
public class FunctionElement extends ShowElement {

	private static final long serialVersionUID = 1L;

	/** 函数成员的标识号，在对应表的标识号范围之外 **/
	private short functionId;

	/** SQL函数 */
	private SQLFunction function;

	/**
	 * default
	 */
	public FunctionElement() {
		super(ShowElement.FUNCTION);
	}

	/**
	 * 
	 * @param def
	 */
	public FunctionElement(short functionId, SQLFunction def, String alias) {
		this();
		this.setFunctionId(functionId);
		this.setFunction(def);
		this.setAlias(alias);
	}

	public void setFunctionId(short id) {
		this.functionId = id;
	}

	public void setFunction(SQLFunction def) {
		this.function = (SQLFunction)def.clone();
	}

	public SQLFunction getFunction() {
		return this.function;
	}
	
	/*
	 * 返回函数操作的列标识号
	 * @see com.lexst.sql.statement.select.ShowElement#getColumnId()
	 */
	@Override
	public short getColumnId() {
		if(function != null && (function instanceof ColumnFunction)) {
			return ((ColumnFunction)function).getColumnId();
		}
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.statement.select.ShowColumn#getColumnId()
	 */
	@Override
	public short getIdentity() {
		return this.functionId;
	}

	/*
	 * 根据函数的返回类型，确定这个成员的类型
	 * @see com.lexst.sql.statement.select.ShowElement#getType()
	 */
	@Override
	public byte getType() {
		return function.getReturnType();
	}

	/*
	 * 函数成员别名(别名由用户定义)
	 * 
	 * @see com.lexst.sql.statement.select.ShowColumn#getName()
	 */
	@Override
	public String getName() {
		if(alias != null) return alias;
		return function.getDescription();
	}

}
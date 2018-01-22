/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.statement.select;

import com.lexst.sql.column.attribute.*;

public class ColumnElement extends ShowElement {

	private static final long serialVersionUID = 1L;
	
	/** 列参数中的基本属性 **/
	private Spike spike = new Spike();
	
	/**
	 * default
	 */
	protected ColumnElement() {
		super(ShowElement.COLUMN);
	}
	
	/**
	 * @param spike
	 */
	public ColumnElement(Spike spike) {
		this();
		this.setSpike(spike);
	}
	
	/**
	 * @param spike
	 * @param alias
	 */
	public ColumnElement(Spike spike, String alias) {
		this(spike);
		this.setAlias(alias);
	}
	
	/**
	 * 设置列基本属性
	 * @param s
	 */
	public void setSpike(Spike s) {
		this.spike.set(s);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.select.ShowElement#getColumnId()
	 */
	@Override
	public short getColumnId(){
		return spike.getColumnId();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.select.ShowElement#getIdentity()
	 */
	@Override
	public short getIdentity() {
		return getColumnId();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.select.ShowElement#getType()
	 */
	@Override
	public byte getType() {
		return spike.getType();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.statement.select.ShowColumn#getName()
	 */
	@Override
	public String getName() {
		if(alias != null) return alias;
		return spike.getName();
	}

}
/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.index;

import com.lexst.sql.column.*;

/**
 * WHERE检索时的校验参数
 *
 */
public abstract class ColumnIndex extends WhereIndex {

	private static final long serialVersionUID = 1L;
	
	/** 检索参数 **/
	protected Column column;

	/**
	 * @param type
	 */
	protected ColumnIndex(byte type) {
		super(type);
	}

	/**
	 * 复制ColumnIndex
	 * @param index
	 */
	protected ColumnIndex(ColumnIndex index) {
		super(index);
		if (index.column != null) {
			this.column = (Column) index.column.clone();
		}
	}

	/**
	 * 设置检索列参数
	 * @param arg
	 */
	public void setColumn(Column arg) {
		this.column = arg;
	}

	/**
	 * 返回检索列参数
	 * @return
	 */
	public Column getColumn() {
		return this.column;
	}
	
	/*
	 * 设置列ID
	 * @see com.lexst.sql.index.WhereIndex#setColumnId(short)
	 */
	@Override
	public void setColumnId(short id) {
		if (column != null) {
			column.setId(id);
		}
	}

	/*
	 * 返回列ID
	 * @see com.lexst.sql.index.WhereIndex#getColumnId()
	 */
	@Override
	public short getColumnId() {
		if (column != null) {
			return column.getId();
		}
		return 0;
	}

}
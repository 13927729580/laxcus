/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.index;

import java.io.*;

import com.lexst.sql.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;

/**
 * 嵌套检索
 *
 */
public class SelectIndex extends WhereIndex {
	
	private static final long serialVersionUID = 5916119111811968139L;

	/** 被检索的列ID */
	private short columnId;
	
	/** SELECT语句 */
	private Select select;

	/**
	 * @param type
	 */
	public SelectIndex() {
		super(Type.SELECT_INDEX);
		this.columnId = 0;
	}

	/**
	 * 复制SelectIndex
	 * 
	 * @param index
	 */
	public SelectIndex(SelectIndex index) {
		super(index);
		this.columnId = index.columnId;
		if (index.select != null) {
			select = (Select) index.select.clone();
		}
	}
	
	/**
	 * 设置参数
	 * @param columnId
	 * @param select
	 */
	public SelectIndex(short columnId, Select select) {
		this();
		this.setColumnId(columnId);
		this.setSelect(select);
	}
	
	/**
	 * 设置SELECT实例
	 * @param obj
	 */
	public void setSelect(Select obj) {
		this.select = obj;
	}
	
	/**
	 * 返回SELECT实例
	 * @return
	 */
	public Select getSelect() {
		return this.select;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#getColumnId()
	 */
	@Override
	public short getColumnId() {
		return this.columnId;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#setColumnId(short)
	 */
	@Override
	public void setColumnId(short id) {
		this.columnId = id;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#duplicate()
	 */
	@Override
	public WhereIndex duplicate() {
		return new SelectIndex(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#build(java.io.ByteArrayOutputStream)
	 */
	@Override
	public void build(ByteArrayOutputStream buff) {
		// 类型定义
		buff.write(super.getType());
		// 列标识(column identity)
		byte[] b = Numeric.toBytes(this.getColumnId());
		buff.write(b, 0, b.length);
		// SELECT数据
		byte[] data = select.build();
		b = Numeric.toBytes(data.length);
		buff.write(b, 0, b.length);
		buff.write(data, 0, data.length);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.index.WhereIndex#resolve(byte[], int, int)
	 */
	@Override
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;

		if (seek + 7 > end) {
			throw new SizeOutOfBoundsException("SelectIndex sizeout!");
		}

		// 类型定义
		setType(b[seek]);
		seek += 1;
		// 列ID
		columnId = Numeric.toShort(b, seek, 2);
		seek += 2;
		// SELECT语句参数
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(seek + size > end) {
			throw new SizeOutOfBoundsException("select stream sizeout");
		}
		// 解析SELECT
		select = new Select();
		int ret = select.resolve(b, seek, size);
		seek += ret;

		return seek - off;
	}

}
/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.function;

import java.util.*;

import com.lexst.sql.function.value.*;
import com.lexst.sql.row.*;

/**
 * 操作某列数据的函数，如SQL聚合函数: Sum,Count,Avg
 *
 */
public abstract class ColumnFunction extends SQLFunction {

	private static final long serialVersionUID = 1L;
	
	/** 列标识号，与ColumnAttribute中的列标识一致。默认是0，未定义 **/
	protected short columnId;

	/**
	 * default
	 */
	protected ColumnFunction() {
		super();
		this.columnId = 0;
	}
	
	/**
	 * @param function
	 */
	protected ColumnFunction(ColumnFunction function) {
		super(function);
		this.setColumnId(function.columnId);
	}
	
	/**
	 * 设置操作列标识号
	 * @param i
	 */
	public void setColumnId(short i) {
		this.columnId = i;
	}

	/**
	 * 返回操作列标识号
	 * @return
	 */
	public short getColumnId() {
		return this.columnId;
	}


	/**
	 * 执行计算，与指定columnId数据类型相同的计算结果
	 * 
	 * @param values
	 * @return
	 */
	public SQLValue compute(List<Row> values) {
		SQLRowSet set = new SQLRowSet();
		set.setValue(values);
		return compute(set);
	}
	
}
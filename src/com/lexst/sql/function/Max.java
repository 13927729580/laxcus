/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.function;

import com.lexst.sql.schema.*;

/**
 * MAX 函数返回一列中的最大值。NULL 值不包括在计算中
 * MIN 和 MAX 也可用于文本列，以获得按字母顺序排列的最高或最低值。
 */
public class Max extends ColumnFunction {

	/**
	 * 
	 */
	public Max() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param function
	 */
	public Max(ColumnFunction function) {
		super(function);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sql) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#compute(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public SQLValue compute(SQLValue value) {
		// TODO Auto-generated method stub
		return null;
	}

}

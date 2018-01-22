/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.function;

import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.parse.*;
import com.lexst.sql.schema.*;

/**
 * SQL AVG。AVG 函数返回数值列的平均值。NULL 值不包括在计算中。
 *
 */
public class Avg extends ColumnFunction {

	private static final long serialVersionUID = 1L;
	
	private final static String REGEX = "^\\s*(?i)(?:AVG)\\s*\\(\\s*(.+?)\\s*\\)\\s*$";
	
	/**
	 * default
	 */
	public Avg() {
		super();
	}

	/**
	 * @param function
	 */
	public Avg(Avg function) {
		super(function);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		return new Avg(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sql) {
		// 语法: AVG(column_name)
		Pattern pattern = Pattern.compile(Avg.REGEX);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}
		String name = matcher.group(1);

		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		
		Avg cmd = new Avg();
		cmd.setDescription(sql.trim());
		cmd.setColumnId(attribute.getColumnId());
		cmd.setReturnType(attribute.getType());

		return cmd;
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

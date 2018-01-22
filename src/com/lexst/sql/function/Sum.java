/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.value.*;
import com.lexst.sql.parse.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;

/**
 * 聚合函数: SUM，统计列参数总量
 * 
 */
public class Sum extends ColumnFunction {

	private static final long serialVersionUID = 1L;

	private final static String REGEX = "^\\s*(?i)(?:SUM)\\s*\\(\\s*(.+?)\\s*\\)\\s*$";

	/**
	 * default
	 */
	public Sum() {
		super();
	}

	/**
	 * @param sum
	 */
	public Sum(Sum sum) {
		super(sum);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		return new Sum(this);
	}
	
	private short count(List<Row> rows) {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.lexst.sql.function.SQLFunction#compute(com.lexst.sql.function.SQLValue
	 * )
	 */
	@Override
	public SQLValue compute(SQLValue value) {
		
		switch(super.getReturnType()) {
		case Type.SHORT:
			break;
		case Type.INTEGER:
			break;
		case Type.LONG:
			break;
		case Type.FLOAT:
			break;
		case Type.DOUBLE:
			break;
		}
		
		if (value.isRowset()) {
			SQLRowSet def = (SQLRowSet) value;
			List<Row> rows = def.getValue();
			short columnId = 0; // find column identity

			long sum = 0;
			for (Row row : rows) {
				Column column = row.find(columnId);
				if (column.isShort()) {
					com.lexst.sql.column.Short sht = (com.lexst.sql.column.Short) column;
					sum += sht.getValue();
				} else if (column.isInteger()) {

				} else if (column.isLong()) {

				} else if (column.isFloat()) {

				} else if (column.isDouble()) {

				}
			}

			return new SQLong(sum);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sql) {
		// 语法: SUM(column_name)
		Pattern pattern = Pattern.compile(Sum.REGEX);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}
		String name = matcher.group(1);

		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		
		Sum cmd = new Sum();
		cmd.setDescription(sql.trim());
		cmd.setColumnId(attribute.getColumnId());
		cmd.setReturnType(attribute.getType());

		return cmd;
	}
}
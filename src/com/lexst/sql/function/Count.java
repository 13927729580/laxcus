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
 * SQL函数:Count，统计某列集合行数，含NULL
 *
 */
public class Count extends ColumnFunction {

	private static final long serialVersionUID = 1L;
	
	private final static String REGEX = "^\\s*(?i)(?:COUNT)\\s*\\(\\s*(\\*|\\w+)\\s*\\)\\s*$";

	private boolean showall;
	
	/**
	 * default
	 */
	public Count() {
		super();
		this.showall = false;
	}

	/**
	 * @param def
	 */
	public Count(Count def) {
		super(def);
		this.showall = def.showall;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#duplicate()
	 */
	@Override
	public SQLFunction duplicate() {
		return new Count(this);
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#create(com.lexst.sql.schema.Table, java.lang.String)
	 */
	@Override
	public SQLFunction create(Table table, String sqlText) {
		Pattern pattern = Pattern.compile(Count.REGEX);
		Matcher matcher = pattern.matcher(sqlText);
		if (!matcher.matches()) {
			return null;
		}

		Count function = new Count();
		function.setDescription(sqlText.trim());
		function.setReturnType(Type.INTEGER);

		String name = matcher.group(1);

		if ("*".equalsIgnoreCase(name)) {
			function.showall = true;
		} else {
			ColumnAttribute attribute = table.find(name);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find \'%s\'", name);
			}

			function.setColumnId(attribute.getColumnId());
		}
		return function;
	}

	/* (non-Javadoc)
	 * @see com.lexst.sql.function.SQLFunction#compute(com.lexst.sql.function.SQLValue)
	 */
	@Override
	public SQLValue compute(SQLValue value) {
		if (!value.isRowset()) {
			// / ERROR
		}

		SQLRowSet def = (SQLRowSet) value;
		List<Row> rows = def.getValue();

		if (this.showall) {
			return new SQLInteger(rows.size());
		}

		int count = 0;
		for (Row row : rows) {
			Column column = row.find(super.columnId);
			if (column != null && !column.isNull()) count++;
		}

		return new SQLInteger(count);
	}

}

/**
 * 
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;

public class DeleteParser extends SQLParser {

	private final static String SQL_DELETE = "^\\s*(?i)(?:DELETE\\s+FROM)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:WHERE)\\s+(.+)\\s*$";

	/**
	 * 
	 */
	public DeleteParser() {
		super();
	}

	/**
	 * 解析 DELETE FROM ... 语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Delete split(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(DeleteParser.SQL_DELETE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("illegal delete syntax");
		}
		
		Space space = new Space(matcher.group(1), matcher.group(2));
		Table table = chooser.findTable(space);
		if (table == null) {
			throw new SQLSyntaxException("cannot find: %s", space);
		}

		String sqlWhere = matcher.group(3);
		Delete delete = new Delete(space);
		// 解析WHERE语句
		WhereParser parser = new WhereParser();
		Condition condi = parser.split(table, chooser, sqlWhere);
		delete.setCondition(condi);

		return delete;
	}
}
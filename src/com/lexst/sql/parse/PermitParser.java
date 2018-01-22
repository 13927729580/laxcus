/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.account.*;

/**
 * SQL权限解析器
 *
 */
class PermitParser extends SQLParser {

	/** 权限关键字集合 **/
	private final static String SELECT = "^\\s*(?i)SELECT\\s*$";
	private final static String INSERT = "^\\s*(?i)INSERT\\s*$";
	private final static String DELETE = "^\\s*(?i)DELETE\\s*$";
	private final static String UPDATE = "^\\s*(?i)UPDATE\\s*$";
	private final static String CONDUCT = "^\\s*(?i)CONDUCT\\s*$";
	private final static String ALL = "^\\s*(?i)ALL\\s*$";
	private final static String DBA = "^\\s*(?i)DBA\\s*$";
	private final static String GRANT = "^\\s*(?i)GRANT\\s*$";
	private final static String REVOKE = "^\\s*(?i)REVOKE\\s*$";
	private final static String CREATE_USER = "^\\s*(?i)(?:CREATE\\s+USER)\\s*$";
	private final static String DROP_USER = "^\\s*(?i)(?:DROP\\s+USER)\\s*$";
	private final static String ALTER_USER = "^\\s*(?i)(?:ALTER\\s+USER)\\s*$";
	private final static String CREATE_SCHEMA = "^\\s*(?i)(?:CREATE)\\s+(?i)(?:SCHEMA|DATABASE)\\s*$";
	private final static String DROP_SCHEMA = "^\\s*(?i)(?:DROP)\\s+(?i)(?:SCHEMA|DATABASE)\\s*$";
	private final static String CREATE_TABLE = "^\\s*(?i)CREATE\\s+(?i)TABLE\\s*$";
	private final static String DROP_TABLE = "^\\s*(?i)DROP\\s+(?i)TABLE\\s*$";

	/** 许可控制选项语法 **/
	private final static String CONTROL_FIRST = "^\\s*(.+?)(\\s*|\\s*\\,.*)\\s*$";
	private final static String CONTROL_NEXT = "^\\s*\\,\\s*(.+?)(\\s*|\\s*\\,.*)\\s*$";

	/**
	 * default
	 */
	protected PermitParser() {
		super();
	}
	
	/**
	 * 分割选项集合，各选项之间以逗号分隔
	 * @param sql
	 * @return
	 */
	protected String[] splitItems(String sql) {
		ArrayList<String> array = new ArrayList<String>();

		for (int i = 0; sql.trim().length() > 0; i++) {
			Pattern pattern = Pattern.compile(i == 0 ? PermitParser.CONTROL_FIRST
							: PermitParser.CONTROL_NEXT);
			Matcher matcher = pattern.matcher(sql);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("cannot resolve %s", sql);
			}

			array.add(matcher.group(1));
			sql = matcher.group(2);
		}

		String[] s = new String[array.size()];
		return array.toArray(s);
	}
	
	/**
	 * 解析各选项名称与指定值是否匹配，并返回选项的数字集合
	 * @param items
	 * @return
	 */
	protected int[] splitSQLControl(String[] items) {
		ArrayList<Integer> a = new ArrayList<Integer>();
		for (String item : items) {
			if (item.matches(PermitParser.SELECT)) {
				a.add(Control.SELECT);
			} else if (item.matches(PermitParser.INSERT)) {
				a.add(Control.INSERT);
			} else if (item.matches(PermitParser.DELETE)) {
				a.add(Control.DELETE);
			} else if (item.matches(PermitParser.UPDATE)) {
				a.add(Control.UPDATE);
			} else if (item.matches(PermitParser.CONDUCT)) {
				a.add(Control.CONDUCT);
			} else if (item.matches(PermitParser.ALL)) {
				a.add(Control.ALL);
			} else if (item.matches(PermitParser.DBA)) {
				a.add(Control.DBA);
			} else if (item.matches(PermitParser.GRANT)) {
				a.add(Control.GRANT);
			} else if (item.matches(PermitParser.REVOKE)) {
				a.add(Control.REVOKE);
			} else if (item.matches(PermitParser.CREATE_USER)) {
				a.add(Control.CREATE_USER);
			} else if (item.matches(PermitParser.DROP_USER)) {
				a.add(Control.DROP_USER);
			} else if (item.matches(PermitParser.ALTER_USER)) {
				a.add(Control.ALTER_USER);
			} else if (item.matches(PermitParser.CREATE_SCHEMA)) {
				a.add(Control.CREATE_SCHEMA);
			} else if (item.matches(PermitParser.DROP_SCHEMA)) {
				a.add(Control.DROP_SCHEMA);
			} else if (item.matches(PermitParser.CREATE_TABLE)) {
				a.add(Control.CREATE_TABLE);
			} else if (item.matches(PermitParser.DROP_TABLE)) {
				a.add(Control.DROP_TABLE);
			} else {
				throw new SQLSyntaxException("cannot resolve %s", item);
			}
		}

		int[] s = new int[a.size()];
		for (int i = 0; i < s.length; i++) {
			s[i] = a.get(i).intValue();
		}
		return s;
	}

}
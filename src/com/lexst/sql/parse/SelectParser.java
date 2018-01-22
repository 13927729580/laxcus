/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.index.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;

/**
 * 解析SQL SELECT语句<br>
 * 
 * 语法:<br>
 * SELECT <br>
 * [TOP {digit}] [RANGE {digit, digit}] <br>
 * column_name, column_name AS alias, function_name, function AS alias, ... <br>
 * FROM SCHEMA.TABLE <br>
 * WHERE condition [AND|OR condition] <br>
 * GROUP BY column_name, column_name2, ... [HAVING aggregate_function] <br>
 * ORDER BY column_name [ASC|DESC]<br>
 *
 */
public class SelectParser extends SQLParser {

	/** 过滤分隔逗号","*/
	private final static String SQL_FILTEREFIX = "^\\s*(?:\\,)\\s*(.+)\\s*$";

	/** SELECT格式(FROM左侧最小化匹配，WHERE右侧最大化匹配) **/
	private final static String SQL_SELECT_ALL = "^\\s*(?i)(?:SELECT)\\s+(.+?)\\s+(?i)(?:FROM)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:WHERE)\\s+(.+)\\s+((?i)(?:GROUP\\s+BY)\\s+.+)\\s+((?i)(?:ORDER\\s+BY)\\s+.+)\\s*$";
	private final static String SQL_SELECT_GROUPBY = "^\\s*(?i)(?:SELECT)\\s+(.+?)\\s+(?i)(?:FROM)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:WHERE)\\s+(.+)\\s+((?i)(?:GROUP\\s+BY)\\s+.+)\\s*$";
	private final static String SQL_SELECT_ORDERBY = "^\\s*(?i)(?:SELECT)\\s+(.+?)\\s+(?i)(?:FROM)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:WHERE)\\s+(.+)\\s+((?i)(?:ORDER\\s+BY)\\s+.+)\\s*$";
	private final static String SQL_SELECT_WHERE  = "^\\s*(?i)(?:SELECT)\\s+(.+?)\\s+(?i)(?:FROM)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:WHERE)\\s+(.+)\\s*$";

	/** 列数量 */
	private final static String SQL_SELECT_PREFIX_TOP = "^\\s*(?i)TOP\\s+(\\d+)(.*)$";
	private final static String SQL_SELECT_PREFIX_RANGE = "^\\s*(?i)RANGE\\s*\\(\\s*(\\d+)\\s*\\,\\s*(\\d+)\\s*\\)(.*)$";
	
	/** 列格式 */
	private final static String SHOW_ALL = "^\\s*(\\*)(\\s*\\,.+|\\s*)$";
	private final static String SHOW_FUNCTION_ALIAS = "^\\s*(\\w+\\s*\\(\\s*\\w+\\s*\\))\\s+(?i)AS\\s+(\\w+)(\\s*\\,.+|\\s*)$";
	private final static String SHOW_FUNCTION = "^\\s*(\\w+\\s*\\(\\s*\\w+\\s*\\))(\\s*\\,.+|\\s*)$";
	private final static String SHOW_COLUMN_ALIAS = "^\\s*(\\w+)\\s+(?i)AS\\s+(\\w+)(\\s*\\,.+|\\s*)$";
	private final static String SHOW_COLUMN = "^\\s*(\\w+)(\\s*\\,.+|\\s*)$";

	/** GROUP BY语句，先解析GROUP BY，再解析HAVING */
	private final static String SQL_GROUPBY_HAVING = "^\\s*(?i)(?:GROUP\\s+BY)\\s+(.+?)\\s+(?i)(?:HAVING)\\s+(.+?)\\s*$";
	private final static String SQL_GROUPBY = "^\\s*(?i)(?:GROUP\\s+BY)\\s+(.+?)\\s*$";
	private final static String SQL_GROUPBY_ELEMENT = "^\\s*(\\w+)(\\s*\\,.+|\\s*)$";

	/** ORDER BY语句 */
	private final static String SQL_ORDERBY = "^\\s*(?i)(?:ORDER\\s+BY)\\s+(.+?)\\s*$";
	private final static String SQL_ORDERBY_ELEMENT1 = "^\\s*(\\w+)\\s+(?i)(ASC|DESC)(\\s*\\,.+|\\s*)$";
	private final static String SQL_ORDERBY_ELEMENT2 = "^\\s*(\\w+)(\\s*\\,.+|\\s*)$";

	/**
	 * default
	 */
	public SelectParser() {
		super();
	}
	
	/**
	 * 解析 TOP digit 
	 * @param table
	 * @param select
	 * @param sql
	 * @return
	 */
	private String splitSelectPrefixTop(Table table, Select select, String sql) {
		Pattern pattern = Pattern.compile(SelectParser.SQL_SELECT_PREFIX_TOP);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			return sql;
		}

		String top = matcher.group(1);
		String suffix = matcher.group(2);
		select.setRange(1, Integer.parseInt(top));
		return suffix;
	}

	/**
	 * 解析 RANGE begin, end
	 * @param table
	 * @param select
	 * @param sql
	 * @return
	 */
	private String splitSelectPrefixRange(Table table, Select select, String sql) {
		Pattern pattern = Pattern.compile(SelectParser.SQL_SELECT_PREFIX_RANGE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql;
		}

		String begin = matcher.group(1);
		String end = matcher.group(2);
		String suffix = matcher.group(3);

		int num1 = Integer.parseInt(begin);
		int num2 = Integer.parseInt(end);
		if (num1 > num2) {
			throw new SQLSyntaxException("illegal range %s - %s", begin, end);
		}
		select.setRange(num1, num2);
		return suffix;
	}
	
	/**
	 * 解析显示成员，保存入ShowSheet并且返回
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private ShowSheet splitShowElement(Table table,  String sql) {
		ShowSheet sheet = new ShowSheet();
		
		// 函数标识号是在原有列标识之外建立
		short functionId = (short) (table.size() + 1);
		
		for (int index = 0; sql.trim().length() > 0; index++) {
			// 过滤前面的逗号分隔符(必须有)
			if(index > 0) {
				Pattern pattern = Pattern.compile(SelectParser.SQL_FILTEREFIX);
				Matcher matcher = pattern.matcher(sql);
				if(!matcher.matches()) {
					throw new SQLSyntaxException("illegal syntax:%s", sql);
				}
				sql = matcher.group(1);
			}
			
			if (sql.trim().length() == 0) {
				throw new SQLSyntaxException("empty sql string!");
			}
			
			//1. 如果是"*"时，显示全部
			Pattern pattern = Pattern.compile(SelectParser.SHOW_ALL);
			Matcher matcher = pattern.matcher(sql);
			boolean match = matcher.matches();
			if(match) {
				sql = matcher.group(2);
				// 保存全部显示列
				for (ColumnAttribute attribute : table.values()) {
					ColumnElement element = new ColumnElement(attribute.getSpike());
					if (sheet.exists(element.getColumnId())) {
						throw new SQLSyntaxException("overlap column '%s'", attribute.getName());
					}
					sheet.add(element);
				}
				continue;
			}
			
			//2. 检查如果是函数格式
			String show = null;
			String alias = null;
			pattern = Pattern.compile(SelectParser.SHOW_FUNCTION_ALIAS);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				show = matcher.group(1);
				alias = matcher.group(2);
				sql = matcher.group(3);
			} else {
				pattern = Pattern.compile(SelectParser.SHOW_FUNCTION);
				matcher = pattern.matcher(sql);
				if(match = matcher.matches()) {
					show = matcher.group(1);
					sql = matcher.group(2);
				}
			}
			if (match) {
				SQLFunction function = SQLFunctionCreator.create(table, show); // 生成函数
				if (function == null) {
					throw new SQLSyntaxException("invalid function:%s", show);
				}
				FunctionElement element = new FunctionElement(functionId++, function, alias);
				sheet.add(element);
				continue;
			}
			
			//3. 检查如果是列格式
			pattern = Pattern.compile(SelectParser.SHOW_COLUMN_ALIAS);
			matcher = pattern.matcher(sql);
			if(match = matcher.matches()) {
				show = matcher.group(1);
				alias = matcher.group(2);
				sql = matcher.group(3);
			} else {
				pattern = Pattern.compile(SelectParser.SHOW_COLUMN);
				matcher = pattern.matcher(sql);
				if(match = matcher.matches()) {
					show = matcher.group(1);
					sql = matcher.group(2);
				}
			}
			if (match) {
				ColumnAttribute attribute = table.find(show); // 列名
				if (attribute == null) {
					throw new SQLSyntaxException("cannot find '%s'", show);
				}
				if (sheet.exists(attribute.getColumnId())) {
					throw new SQLSyntaxException("overlap column '%s'", show);
				}
				ColumnElement element = new ColumnElement(attribute.getSpike(), alias);
				sheet.add(element);
				continue;
			}

			// 全部解析不成功
			throw new SQLSyntaxException("invalid show element:%s", sql);
		}

		return sheet;
	}
	
	/**
	 * 分析 SELECT ... FROM 之间的数据
	 * 
	 * @param table
	 * @param select
	 * @param sql
	 */
	private void splitSelectPrefix(Table table, Select select, String sql) {
		// 解析TOP... | RANGE ... 
		do {
			String suffix = splitSelectPrefixTop(table, select, sql);
			if (!sql.equals(suffix)) {
				sql = suffix;
				continue;
			}
			suffix = splitSelectPrefixRange(table, select, sql);
			if (!sql.equals(suffix)) {
				sql = suffix;
				continue;
			}
		} while (false);
		
		// 解析显示列集合
		ShowSheet sheet = splitShowElement(table, sql);
		select.setShowSheet(sheet);
	}
	
	/**
	 * 解析"GROUP BY"，语法分为两部分:列分组和"HAVING"条件比较
	 * @param table
	 * @param sql
	 * @return
	 */
	private GroupBy splitGroupBy(Table table, String sql) {
		Pattern pattern = Pattern.compile(SelectParser.SQL_GROUPBY_HAVING);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			pattern = Pattern.compile(SelectParser.SQL_GROUPBY);
			matcher = pattern.matcher(sql);
		}
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid syntax:%s", sql);
		}

		String align = matcher.group(1);
		String having = (matcher.groupCount() > 1 ? matcher.group(2) : null);
		GroupBy by = new GroupBy();
		
		//1. 解析"GROUP BY" 分组列名
		for(int index = 0; align.trim().length() > 0; index++) {
			if (index > 0) {
				pattern = Pattern.compile(SelectParser.SQL_FILTEREFIX);
				matcher = pattern.matcher(align);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("invalid syntax:%s", align);
				}
				align = matcher.group(1);
			}

			pattern = Pattern.compile(SelectParser.SQL_GROUPBY_ELEMENT);
			matcher = pattern.matcher(align);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("invalid syntax:%s", align);
			}
			String name = matcher.group(1);
			align = matcher.group(2);
			// 查找对应的列属性
			ColumnAttribute attribute = table.find(name);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find:%s", name);
			}
			by.addGroupId(attribute.getColumnId());
		}

		//2. 解析"HAVING"后续函数
		if(having != null) {
			HavingParser parser = new HavingParser();
			Situation situa = parser.split(table, having);
			by.setSituation(situa);
		}
		
		return by;
	}

	/**
	 * 解析"ORDER BY"语句
	 * @param table
	 * @param select
	 * @param sql
	 */
	private OrderBy splitOrderBy(Table table, String sql) {
		Pattern pattern = Pattern.compile(SelectParser.SQL_ORDERBY);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("invalid syntax:%s", sql);
		}
		
		sql = matcher.group(1);
		OrderBy by = null;
		
		for(int index = 0; sql.trim().length() > 0; index++) {
			//1. 过滤","符号
			if (index > 0) {
				pattern = Pattern.compile(SelectParser.SQL_FILTEREFIX);
				matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("invalid syntax:%s", sql);
				}
				sql = matcher.group(1);
			}
			// "ORDER BY"两种情况
			String name = null;
			String sort = null;
			pattern = Pattern.compile(SelectParser.SQL_ORDERBY_ELEMENT1);
			matcher = pattern.matcher(sql);
			boolean match = matcher.matches();
			if (match) {
				name = matcher.group(1);
				sort = matcher.group(2);
				sql = matcher.group(3);
			} else {
				pattern = Pattern.compile(SelectParser.SQL_ORDERBY_ELEMENT2);
				matcher = pattern.matcher(sql);
				if (match = matcher.matches()) {
					name = matcher.group(1);
					sql = matcher.group(2);
				}
			}
			// 语法错误
			if (!match) {
				throw new SQLSyntaxException("error syntax:%s", sql);
			}

			// 查找列属性
			ColumnAttribute attribute = table.find(name);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find:%s", name);
			}
			int asc = "ASC".equalsIgnoreCase(sort) ? OrderBy.ASC : OrderBy.DESC;
			if (by == null) {
				by = new OrderBy(attribute.getColumnId(), asc);
			} else {
				by.setLast(new OrderBy(attribute.getColumnId(), asc));
			}
		}
		
		return by;
	}

	/**
	 * 解析SELECT语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Select split(String sql, SQLChooser chooser) {
		Space space = null;
		String prefix = null;
		String where = null;
		String groupby = null;
		String orderby = null;
		
		Pattern pattern = Pattern.compile(SelectParser.SQL_SELECT_ALL);
		Matcher matcher = pattern.matcher(sql);		
		boolean match = matcher.matches();
		if (match) {
			prefix = matcher.group(1);
			space = new Space(matcher.group(2), matcher.group(3));
			where = matcher.group(4);
			groupby = matcher.group(5);
			orderby = matcher.group(6);
		}
		if(!match) {
			pattern = Pattern.compile(SelectParser.SQL_SELECT_GROUPBY);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
			if(match) {
				prefix = matcher.group(1);
				space = new Space(matcher.group(2), matcher.group(3));
				where = matcher.group(4);
				groupby = matcher.group(5);
			}
		}
		if(!match) {
			pattern = Pattern.compile(SelectParser.SQL_SELECT_ORDERBY);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
			if(match) {
				prefix = matcher.group(1);
				space = new Space(matcher.group(2), matcher.group(3));
				where = matcher.group(4);
				orderby = matcher.group(5);				
			}
		}
		if(!match) {
			pattern = Pattern.compile(SelectParser.SQL_SELECT_WHERE);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
			if(match) {
				prefix = matcher.group(1);
				space = new Space(matcher.group(2), matcher.group(3));
				where = matcher.group(4);
			}
		}
		// 全部条件不能匹配，"SELECT"语法错误
		if(!match) {
//			throw new SQLSyntaxException("invalid syntax:%s", sql);
			throwable("syntax error or missing!");
		}
		
		// 查找匹配的数据表
		Table table = chooser.findTable(space); 
		if (table == null) {
			throw new SQLSyntaxException("cannot find %s", space);
		}
		
//		System.out.printf("%s|PREFIX:%s\n", sql, prefix);
		
		Select select = new Select(space);
		// 解析 "select * from" 之间的语句
		this.splitSelectPrefix(table, select, prefix);
		// 解析 WHERE语句
		WhereParser parser = new WhereParser();
		Condition condi = parser.split(table, chooser, where);
		select.setCondition(condi);
		// 解析 "GROUP BY"语句
		if (groupby != null) {
			GroupBy by = splitGroupBy(table, groupby);
			select.setGroupBy(by);
		}
		// 解析 "ORDER BY"语句
		if (orderby != null) {
			OrderBy by = splitOrderBy(table, orderby);
			select.setOrderBy(by);
		}
		
		// 检查查询条件
		while (condi != null) {
			// 主查询条件
			WhereIndex index = condi.getValue();
			short columnId = buildNormalId(index.getColumnId());
			ColumnAttribute attribute = table.find(columnId);
			if(!attribute.isKey()) {
				throw new SQLSyntaxException("invalid key: %s", condi.getColumnName());
			}
			// 友查询条件
			for(Condition partner : condi.getPartners()) {
				index = partner.getValue();
				columnId = buildNormalId(index.getColumnId());
				attribute = table.find(columnId);
				if (!attribute.isKey()) {
					throw new SQLSyntaxException("invalid key: %s", partner.getColumnName());
				}
			}
			// 子查询条件
			condi = condi.getNext();
		}
		
		return select;
	}
}
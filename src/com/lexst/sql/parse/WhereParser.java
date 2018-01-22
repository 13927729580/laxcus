/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * "select, delete, update" where syntax
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 7/1/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.sql.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.value.*;
import com.lexst.sql.index.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;
import com.lexst.sql.util.*;
import com.lexst.util.*;

public class WhereParser extends GradationParser {
	
	class LogicString {
		public String logic;
		public String value;

		public LogicString() {
			super();
		}

		public LogicString(String logic, String value) {
			this.logic = logic;
			this.value = value;
		}
	}
	
	/**
	 * BWTEEN有两种情况:
	 * <1> 前面如果有AND|OR逻辑符，再前面必须有一段其它字符
	 * <2> 前面如果没有AND|OR逻辑符，再前面必须是空的
	 * 
	 * // 基本比较表达式: (\w+\s+(?i)(?:NOT\s+BETWEEN|BETWEEN)\s+.+?\s+(?i)AND\s+.+?)\s*$
	 * // 有其它参数: ^(.+)(?i)(\s+AND\s+|\s+OR\s+)(\w+\s+(?i)(?:NOT\s+BETWEEN|BETWEEN)\s+.+?\s+(?i)AND\s+.+?)\s*$
	 * // 无参数: ^\s*(\w+\s+(?i)(?:NOT\s+BETWEEN|BETWEEN)\s+.+?\s+(?i)AND\s+.+?)\s*$
	 * 
	 * ^(\s*|.+\s+(?i)(AND\s+|OR\s+))(\w+\s+(?i)(?:NOT\s+BETWEEN|BETWEEN)\s+.+?(?i)AND\s+.+?)\s*$
	 * 
	 * 其它WHERE条件大致相同
	 */
	
	/******* SQL WHERE 子句格式 *********/
	private final static String SQL_WHERE_NULLEMPTY = "(\\w+\\s+(?i)(?:IS\\s+NULL|IS\\s+NOT\\s+NULL|IS\\s+EMPTY|IS\\s+NOT\\s+EMPTY))\\s*$";
	private final static String SQL_WHERE_BETWEEN = "(\\w+\\s+(?i)(?:NOT\\s+BETWEEN|BETWEEN)\\s+.+?(?i)AND\\s+.+?)\\s*$";
	private final static String SQL_WHERE_IN = "(\\w+\\s+(?i)(?:IN)\\s*\\(.+?\\))\\s*$";
	private final static String SQL_WHERE_DIGIT = "(\\w+\\s*(?:=|!=|<>|>|<|>=|<=)\\s*(?:[+|-]{0,1}[0-9]+[\\.]{0,1}[0-9]*))\\s*$";
	private final static String SQL_WHERE_RAW = "(\\w+\\s*(?:=|!=|<>)\\s*(?i)(?:0X)(?:[0-9a-fA-F]+))\\s*$";
	private final static String SQL_WHERE_LIKE = "(\\w+\\s+(?i)(?:LIKE)\\s+\\'.+?\\')\\s*$";
	private final static String SQL_WHERE_CALENDAR = "(\\w+\\s*(?:=|!=|<>|>|<|>=|<=)\\s*\\'(?:[0-9\\.\\:\\-\\/\\p{Space}]+)\\')\\s*$";
	private final static String SQL_WHERE_STRING = "(\\w+\\s*(?:=|!=|<>)\\s*\\'.+?\\')\\s*$";
	private final static String SQL_WHERE_SELECT = "(\\w+(?i)(?:\\s+IN\\s+|\\s*=\\s*|\\s*!=\\s*|\\s*<>\\s*|\\s*>\\s*|\\s*<\\s*|\\s*>=\\s*|\\s*<=\\s*)\\(\\s*(?i)(?:SELECT\\s+).+\\))\\s*$";
	
	/******** WHERE 各单元被检索值格式  **********/	
	private final static String SQL_COLUMN_ISNULL = "^\\s*(\\w+)\\s+(?i)(IS\\s+NULL)\\s*$";
	private final static String SQL_COLUMN_NOTNULL = "^\\s*(\\w+)\\s+(?i)(IS\\s+NOT\\s+NULL)\\s*$";
	
	private final static String SQL_COLUMN_ISEMPTY = "^\\s*(\\w+)\\s+(?i)(IS\\s+EMPTY)\\s*$";
	private final static String SQL_COLUMN_NOTEMPTY = "^\\s*(\\w+)\\s+(?i)(IS\\s+NOT\\s+EMPTY)\\s*$";
	
	private final static String SQL_COLUMN_RAW = "^\\s*(\\w+)\\s*(=|!=|<>)\\s*(?i)(?:0X)([0-9a-fA-F]+)\\s*$";
	private final static String SQL_COLUMN_LIKE = "^\\s*(\\w+)\\s+(?i)(LIKE)\\s+\\'(.+)\\'\\s*$";
	private final static String SQL_COLUMN_NUMBER = "^\\s*(\\w+)\\s*(=|!=|<>|>|<|>=|<=)\\s*([+|-]{0,1}[0-9]+[\\.]{0,1}[0-9]*)\\s*$";
	private final static String SQL_COLUMN_STRING = "^\\s*(\\w+)\\s*(=|!=|<>)\\s*\\'(.+)\\'\\s*$";
	private final static String SQL_COLUMN_CALENDAR = "^\\s*(\\w+)\\s*(=|!=|<>|>|<|>=|<=)\\s*\\'([0-9\\.\\:\\-\\/\\p{Space}]+)\\'\\s*$";

	private final static String SQL_COLUMN_IN = "^\\s*(\\w+)\\s+(?i)(?:IN)\\s*\\((.+)\\)\\s*$";
	private final static String SQL_COLUMN_BETWEEN = "^\\s*(\\w+)\\s+(?i)(BETWEEN|NOT\\s+BETWEEN)\\s+(.+)\\s+(?i)(?:AND)(.+)\\s*$";
	private final static String SQL_COLUMN_SELECT = "^\\s*(\\w+)\\s*(?i)(IN|=|!=|<>|>|<|>=|<=)\\s*\\(\\s*((?i)(?:SELECT\\s+\\w+\\s+FROM\\s+\\w+\\.\\w+).+)\\)\\s*$";
	
	/** WHERE COLUMN_NAME IN (...) 括号中的参数 */
	private final static String SQL_COLUMN_IN_STRING = "^\\s*\\'(.+?)\\'(\\s*|\\s*\\,\\s*\\'.+)$";
	private final static String SQL_COLUMN_IN_CALENDAR = "^\\s*\\'([0-9\\.\\:\\-\\/\\p{Space}]+)\\'(\\s*|\\s*\\,\\s*\\'.+)$";
	private final static String SQL_COLUMN_IN_NUMBER = "^\\s*([+|-]{0,1}[0-9]+[\\.]{0,1}[0-9]*)(\\s*|\\s*\\,.+)$";
	private final static String SQL_COLUMN_IN_RAW = "^\\s*(?i)(?:0X)([0-9a-fA-F]+)(\\s*|\\s*\\,.+)$";

	/**
	 * default
	 */
	public WhereParser() {
		super();
	}
	
	/**
	 * 右侧最小匹配，取出参数，分割条件是逻辑连接符号(AND|OR)<br>
	 * SQL语句最前面已经取消了逻辑符号(AND|OR)<br>
	 * 
	 * @param sql
	 * @return
	 */
	private LogicString[] splitWhereMember(String sql) {
		ArrayList<LogicString> array = new ArrayList<LogicString>();
		final String prefix = "^(.+\\s+(?i)(AND\\s+|OR\\s+)|\\s*)";
		while(sql.trim().length() > 0) {
			//1. 检查 COLUMN_NAME [IS NULL|IS EMPTY|IS NOT NULL|IS NOT EMPTY...]
			Pattern pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_NULLEMPTY);
			Matcher matcher = pattern.matcher(sql);
			boolean match = matcher.matches();
			//2. 检查 COLUMN_NAME [BETWEEN|NOT BETWEEN] ... AND ...
			if (!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_BETWEEN);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//3. 检查WHERE COLUMN_NAME IN ()
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_IN);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//4. 检查WHERE COLUMN_NAME (=|<>|!=|>=|<=|>|<) [digit]
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_DIGIT);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//5. 检查二进制数字
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_RAW);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//6. 检查WHERE COLUMN_NAME LIKE '...'
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_LIKE);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//7. 检查日期
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_CALENDAR);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//8. 检查字符 WHERE COLUMN_NAME [=|<>|!=] '...' 
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_STRING);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			//9. 检查子检索:WHERE COLUMN_NAME (<>|!=|IN|=|>|<|>=|<=) (SELECT....
			if(!match) {
				pattern = Pattern.compile(prefix + WhereParser.SQL_WHERE_SELECT);
				matcher = pattern.matcher(sql);
				match = matcher.matches();
			}
			if (!match || matcher.groupCount() != 3) {
				throw new SQLSyntaxException("illegal syntax:%s", sql);
			}
			
			// 解析字符串，保存在数组集合的最前面
			sql = matcher.group(1);
			String logic = matcher.group(2);
			if (logic == null) logic = "";
			sql = sql.substring(0, sql.length() - logic.length());
			array.add(0, new LogicString(logic, matcher.group(3)));
		}
		
//		// debug code, start
//		for(LogicString s : array) {
//			System.out.printf("%s|%s\n", s.logic, s.value);
//		}
//		// debug code, end
		
		LogicString[] s = new LogicString[array.size()];
		return array.toArray(s);
	}

//	/**
//	 * 采用右侧最小匹配，以AND|OR、列名、比较符为条件，进行切割
//	 * 
//	 * @param sql
//	 * @return
//	 */
//	private String[] splitWhereElement(String sql) {
//		System.out.printf("{%s}\n", sql);
//		
//		// 右侧最小匹配
//		final String regex = "^(.+)(\\s+(?i)(?:AND|OR)\\s+\\w+(?i)(?:\\s*=\\s*|\\s*!=\\s*|\\s*<>\\s*|\\s*>\\s*|\\s*>=\\s*|\\s*<\\s*|\\s*<=\\s*|\\s+IN\\s+|\\s+BETWEEN\\s+|\\s+NOT\\s+BETWEEN\\s+))(.+?)$";
//		List<String> array = new ArrayList<String>();
//		do {
//			Pattern pattern = Pattern.compile(regex);
//			Matcher matcher = pattern.matcher(sql);
//			if(!matcher.matches()) {
//				array.add(0, sql);
//				break;
//			}
//			sql = matcher.group(1);
//			array.add(0, matcher.group(2) + matcher.group(3));
//		} while(true);
//		
//		// debug code, star
//		for(String s : array) {
//			System.out.println(s);
//		}
//		// debug code, end
//		
//		String[] s = new String[array.size()];
//		return array.toArray(s);
//	}
	
	/**
	 * 处理字符串中的转义字符
	 * 
	 * @param s
	 * @return
	 */
	private String translate(String s) {
		return s.replaceAll("\\'", "\'");
	}

	/**
	 * 判断IS NULL|IS NOT NULL，生成条件
	 * @param table
	 * @param name
	 * @param isnull
	 * @return
	 */
	private Condition splitNull(Table table, String name, boolean isnull) {
		ColumnAttribute attribute = table.find(name);
		if(attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		
		WhereIndex index = null;
		switch (attribute.getType()) {
		case Type.SHORT:
			index = new ShortIndex((short) 0, new com.lexst.sql.column.Short());
			break;
		case Type.INTEGER:
			index = new IntegerIndex(0, new com.lexst.sql.column.Integer());
			break;
		case Type.DATE:
			index = new IntegerIndex(0, new com.lexst.sql.column.Date());
			break;
		case Type.TIME:
			index = new IntegerIndex(0, new com.lexst.sql.column.Time());
			break;
		case Type.RAW:
			index = new LongIndex(0L, new com.lexst.sql.column.Raw());
			break;
		case Type.CHAR:
			index = new LongIndex(0L, new com.lexst.sql.column.Char());
			break;
		case Type.SCHAR:
			index = new LongIndex(0L, new com.lexst.sql.column.SChar());
			break;
		case Type.WCHAR:
			index = new LongIndex(0L, new com.lexst.sql.column.WChar());
			break;
		case Type.LONG:
			index = new LongIndex(0L, new com.lexst.sql.column.Long());
			break;
		case Type.TIMESTAMP:
			index = new LongIndex(0L, new com.lexst.sql.column.Timestamp());
			break;
		case Type.FLOAT:
			index = new FloatIndex(0.0f, new com.lexst.sql.column.Float());
			break;
		case Type.DOUBLE:
			index = new DoubleIndex(0.0f, new com.lexst.sql.column.Double());
			break;
		default:
			throw new SQLSyntaxException("invalid column: %d", attribute.getType());
		}

//		index.getColumn().setId(attribute.getColumnId());
//		index.getColumn().setNull(isnull);
		
		index.setColumnId(attribute.getColumnId());
		((ColumnIndex) index).getColumn().setNull(isnull);
		
		Condition condi = new Condition(name, isnull ? Condition.ISNULL : Condition.NOTNULL, index);
		return condi;
	}

	/**
	 * 判断IS EMPTY|IS NOT EMPTY，生成检索条件(只限可变长类型)<br>
	 * 
	 * @param table
	 * @param name
	 * @param isempty
	 * @return
	 */
	private Condition splitEmpty(Table table, String name, boolean isempty) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}

		// 只限可变长类型
		LongIndex index = new LongIndex();
		try {
			if (attribute.isRaw()) {
				index.setColumn(VariableGenerator.toRaw(table.isDSM(), (RawAttribute)attribute, new byte[0]));
			} else if (attribute.isChar()) {
				index.setColumn(VariableGenerator.toChar(table.isDSM(), (CharAttribute)attribute, new String())); 
			} else if (attribute.isSChar()) {
				index.setColumn(VariableGenerator.toSChar(table.isDSM(), (SCharAttribute)attribute, new String()));
			} else if (attribute.isWChar()) {
				index.setColumn(VariableGenerator.toWChar(table.isDSM(), (WCharAttribute)attribute, new String()));
			} else {
				throw new SQLSyntaxException("cannot support:%s", name);
			}
		} catch(IOException e) {
			throw new SQLSyntaxException(e);
		}

		index.setColumnId(attribute.getColumnId());
		index.getColumn().setNull(false);

		return new Condition(name, (isempty ? Condition.ISEMPTY : Condition.NOTEMPTY), index);
	}
	
	/**
	 * 解析 WHERE column_name [IN] (value1, value2...) 语句
	 * @param table
	 * @param name
	 * @param sql
	 * @return
	 */
	private Condition splitIn(Table table, String name, String sql) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		
		Condition parent = null;
		for (int index = 0; sql.trim().length() > 0; index++) {
			//1. 必须有逗号做分隔符
			if(index > 0) {
				if(!isCommaPrefix(sql)) {
					throw new SQLSyntaxException("illegal:%s", sql);
				}
				sql = filteCommaPrefix(sql);
			}
			
			if (attribute.isWord()) {
				// 去掉两侧的引号
				Pattern pattern = Pattern.compile(WhereParser.SQL_COLUMN_IN_STRING);
				Matcher matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal string:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				
				// 字符串转义
				value = translate(value);
				Condition condi = splitString(table, name, "=", value);
				if (parent == null) {
					parent = condi;
				} else {
					condi.setRelation(Gradation.OR);
					parent.addPartner(condi);
				}
			} else if (attribute.isCalendar()) {
				Pattern pattern = Pattern.compile(WhereParser.SQL_COLUMN_IN_CALENDAR);
				Matcher matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal calendar:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				
				Condition condi = this.splitString(table, name, "=", value);
				if(parent == null) {
					parent = condi;
				} else {
					condi.setRelation(Gradation.OR);
					parent.addPartner(condi);
				}
			} else if (attribute.isNumber()) {
				Pattern pattern = Pattern.compile(WhereParser.SQL_COLUMN_IN_NUMBER);
				Matcher matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal number:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				
				Condition condi = this.splitNumber(table, name, "=", value);
				if(parent == null) {
					parent = condi;
				} else {
					condi.setRelation(Gradation.OR);
					parent.addPartner(condi);
				}
			} else if (attribute.isRaw()) {
				Pattern pattern = Pattern.compile(WhereParser.SQL_COLUMN_IN_RAW);
				Matcher matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal binary:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				
				Condition condi = this.splitRaw(table, name, "=", value);
				if(parent == null) {
					parent = condi;
				} else {
					condi.setRelation(Gradation.OR);
					parent.addPartner(condi);
				}
			}
		}
		
		return parent;		
	}
	
	/**
	 * 解析 WHERE column_name [BETWEEN|NOT BETWEEN] value1 AND value2 语句
	 * @param table
	 * @param name
	 * @param between
	 * @param value1
	 * @param value2
	 * @return
	 */
	private Condition splitBetween(Table table, String name, boolean between, String value1, String value2) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		// 不支持可变长类型(二进制字节和字符)
		if(attribute.isVariable()) {
			throw new SQLSyntaxException("cannot support variable by %s", name);
		}

		String compare1 = (between ? ">=" : "<");
		String compare2 = (between ? "<=" : ">");
		byte relate = (between ? Gradation.AND : Gradation.OR);
		
		// 如果是日期/时间格式，过滤掉两侧引号
		if(attribute.isCalendar()) {
			final String regex = "^\\s*\\'(.+)\\'\\s*$";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(value1);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("illegal calendar style:%s", value1);
			}
			value1 = matcher.group(1);
			matcher = pattern.matcher(value2);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("illegal calendar style:%s", value2);
			}
			value2 = matcher.group(2);
		}
		// 解析参数
		if (attribute.isCalendar()) {
			Condition condi1 = this.splitString(table, name, compare1, value1);
			Condition condi2 = this.splitString(table, name, compare2, value2);
			condi2.setRelation(relate);
			condi1.addPartner(condi2);
			return condi1;
		}
		if (attribute.isNumber()) {
			Condition condi1 = this.splitNumber(table, name, compare1, value1);
			Condition condi2 = this.splitNumber(table, name, compare2, value2);
			condi2.setRelation(relate);
			condi1.addPartner(condi2);
			return condi1;
		}
		
		throw new SQLSyntaxException("invalid attribute:", name);
	}
	
	/**
	 * 解析二地制数据，生成检索条件
	 * 
	 * @param table
	 * @param name
	 * @param compare
	 * @param value
	 * @return
	 */
	private Condition splitRaw(Table table, String name, String compare, String value) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		if (!attribute.isRaw()) {
			throw new SQLSyntaxException("invalid column:%s", name);
		}
		
		// 字符串转成字节流
		byte[] b = atob(value);
		try {
			Raw raw = VariableGenerator.toRaw(table.isDSM(), (RawAttribute) attribute, b);
			b = raw.getValid();

			long hash = Sign.sign(b, 0, b.length);
			LongIndex index = new LongIndex(hash, raw);

			return new Condition(name, Condition.translateCompare(compare), index);
		} catch (IOException e) {
			throw new SQLSyntaxException(e);
		}
	}
	
	/**
	 * 生成LIKE比较条件，只限字符类型
	 * 
	 * @param table
	 * @param name
	 * @param value
	 * @return
	 */
	private Condition splitLike(Table table, String name, String value) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		if (!attribute.isWord()) {
			throw new SQLSyntaxException("cannot support:%s", name);
		}
		
		SQLString string = super.splitLike(attribute, value);
		short left = string.getLeft();
		short right = string.getRight();
		String text = string.getValue();

		LongIndex index = new LongIndex();		
		try {
			if (left == 0 && right == 0) {
				Word word = null;
				if (attribute.isChar()) {
					word = VariableGenerator.toChar(table.isDSM(), (CharAttribute) attribute, text);
				} else if (attribute.isSChar()) {
					word = VariableGenerator.toSChar(table.isDSM(), (SCharAttribute) attribute, text);
				} else if (attribute.isWChar()) {
					word = VariableGenerator.toWChar(table.isDSM(), (WCharAttribute) attribute, text);
				}
				byte[] b = word.getValid();
				long hash = Sign.sign(b, 0, b.length);
				index.setValue(hash);
				index.setColumn(word);
			} else {
				VWord vword = null;
				if (attribute.isChar()) {
					vword = VariableGenerator.toVChar((CharAttribute) attribute, left, right, text);
				} else if (attribute.isSChar()) {
					vword = VariableGenerator.toVSChar((SCharAttribute) attribute, left, right, text);
				} else if (attribute.isWChar()) {
					vword = VariableGenerator.toVWChar((WCharAttribute) attribute, left, right, text);
				}
				byte[] b = vword.getIndex();
				long hash = Sign.sign(b, 0, b.length);
				index.setValue(hash);
				index.setColumn(vword);
			}
		} catch (IOException e) {
			throw new SQLSyntaxException(e);
		}
		
		return new Condition(name, Condition.LIKE, index);
	}
	
	/**
	 * 解析嵌套查询
	 * 
	 * @param table
	 * @param chooser
	 * @param name
	 * @param compare
	 * @param sqlSelect
	 * @return
	 */
	private Condition splitSelect(Table table, SQLChooser chooser, String name, String compare, String sqlSelect) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}

		// 解析 "SELECT ... FROM schema.table" 语句
		SelectParser parser = new SelectParser();
		Select select = parser.split(sqlSelect, chooser);
		// 检查: <1>检查参数必须只有一个，<2>比较属性必须一致
		ShowSheet sheet = select.getShowSheet();
		if (sheet.size() != 1) {
			throw new SQLSyntaxException("select syntax error! %s", sqlSelect);
		}
		ShowElement showElement = sheet.get(0);
		if(!showElement.isColumn()) {
			throw new SQLSyntaxException("column error!");
		}
//		ColumnAttribute other = ((ColumnElement) showElement).getAttribute();
		if (attribute.getType() != showElement.getType()) {// other.getType()) {
			throw new SQLSyntaxException("cannot match type!");
		}

//		System.out.printf("%s compare is:%s | %d\n", name, compare, Condition.translateCompare(compare));
		
		SelectIndex index = new SelectIndex(attribute.getColumnId(), select);
		return new Condition(name, Condition.translateCompare(compare), index);
	}
	
	/**
	 * 解析字符串格式
	 * 
	 * @param table
	 * @param name
	 * @param compare
	 * @param value
	 * @return
	 */
	private Condition splitString(Table table, String name, String compare, String value) {
		ColumnAttribute attribute = table.find(name);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		
		WhereIndex index = null;
		if (attribute.isWord()) {
			Word column = null;
			// 加入数据
			try {
				if (attribute.isChar()) {
					column = VariableGenerator.toChar(table.isDSM(),(CharAttribute) attribute, value);
				} else if (attribute.isSChar()) {
					column = VariableGenerator.toSChar(table.isDSM(), (SCharAttribute) attribute, value);
				} else if (attribute.isWChar()) {
					column = VariableGenerator.toWChar(table.isDSM(), (WCharAttribute) attribute, value);
				}
			} catch(IOException e){
				throw new SQLSyntaxException(e);
			}
			byte[] b = column.getValid();
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, column);
		} else if (attribute.isCalendar()) {
			short columnId = attribute.getColumnId();
			if(attribute.isDate()) {
				int num = super.splitDate(value);
				com.lexst.sql.column.Date date = new com.lexst.sql.column.Date(columnId, num);
				index = new IntegerIndex(num, date);
			} else if (attribute.isTime()) {
				int num = super.splitTime(value);
				com.lexst.sql.column.Time time = new com.lexst.sql.column.Time(columnId, num);
				index = new IntegerIndex(num, time);
			} else if (attribute.isTimestamp()) {
				long num = super.splitTimestamp(value);
				com.lexst.sql.column.Timestamp stamp = new com.lexst.sql.column.Timestamp(columnId, num);
				index = new LongIndex(num, stamp);
			}
		} else {
			throw new SQLSyntaxException("illegal attribute: %s - %s", name, value);
		}

		// 生成条件
		return new Condition(name, Condition.translateCompare(compare), index);
	}
	
	/**
	 * 解析定长参数（日期/时间、数值），生成比较条件
	 * @param table
	 * @param name
	 * @param compare
	 * @param value
	 * @return
	 */
	private Condition splitNumber(Table table, String name, String compare, String value) {
		ColumnAttribute attribute = table.find(name);
		if(attribute == null) {
			throw new SQLSyntaxException("cannot find \'%s\'", name);
		}
		if (!attribute.isNumber()) {
			throw new SQLSyntaxException("invalid attribute: %s - %s", name, value);
		}
		
		WhereIndex index = null;
		short columnId = attribute.getColumnId();
		if (attribute.isShort()) {
			short num = super.splitShort(value); // java.lang.Short.parseShort(value.trim());
			com.lexst.sql.column.Short small = new com.lexst.sql.column.Short(columnId, num);
			index = new ShortIndex(num, small);
		} else if (attribute.isInteger()) {
			int num = super.splitInt(value); // java.lang.Integer.parseInt(value.trim());
			com.lexst.sql.column.Integer integer = new com.lexst.sql.column.Integer(columnId, num);
			index = new IntegerIndex(num, integer);
		} else if (attribute.isLong()) {
			long num = splitLong(value); // java.lang.Long.parseLong(value.trim());
			com.lexst.sql.column.Long big = new com.lexst.sql.column.Long(columnId, num);
			index = new LongIndex(num, big);
		} else if (attribute.isFloat()) {
			float num = splitFloat(value); // java.lang.Float.parseFloat(value.trim());
			com.lexst.sql.column.Float real = new com.lexst.sql.column.Float(columnId, num);
			index = new FloatIndex(num, real);
		} else if (attribute.isDouble()) {
			double num = splitDouble(value); // java.lang.Double.parseDouble(value.trim());
			com.lexst.sql.column.Double du = new com.lexst.sql.column.Double(columnId, num);
			index = new DoubleIndex(num, du);
		} 
		
//		else if (attribute.isDate()) {
//			int num = super.splitDate(value);
//			com.lexst.sql.column.Date date = new com.lexst.sql.column.Date(columnId, num);
//			index = new IntegerIndex(num, date);
//		} else if (attribute.isTime()) {
//			int num = super.splitTime(value);
//			com.lexst.sql.column.Time time = new com.lexst.sql.column.Time(columnId, num);
//			index = new IntegerIndex(num, time);
//		} else if (attribute.isTimestamp()) {
//			long num = super.splitTimestamp(value);
//			com.lexst.sql.column.Timestamp stamp = new com.lexst.sql.column.Timestamp(columnId, num);
//			index = new LongIndex(num, stamp);
//		}

		Condition condi = new Condition(name, Condition.translateCompare(compare), index);
		return condi;	
	}
	
	/**
	 * 解析一列，返回查询条件<br>
	 * 六种查询条件: <br>
	 * <1>IS NULL <2>NOT NULL <3>EMPTY <4>NOT EMPTY <5>字符串 <6>数值类型<br>
	 * 
	 * @param sql
	 * @return
	 */
	private Condition splitColumn(Table table, SQLChooser chooser, String sql) {
		//1. 空值检查
		Pattern pattern = Pattern.compile(WhereParser.SQL_COLUMN_ISNULL);
		Matcher matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			return splitNull(table, name, true);
		}
		//2. 非空值检查
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_NOTNULL);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			return splitNull(table, name, false);
		}
		//3. EMPTY值检查(限可变长类型, RAW,CHAR,SCHAR,WCHAR)
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_ISEMPTY);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			return splitEmpty(table, name, true);
		}
		//4. 非EMPTY值检查
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_NOTEMPTY);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			return splitEmpty(table, name, false);
		}
		//5. 处理SELECT嵌套
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_SELECT);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			String compare = matcher.group(2);
			String syntax = matcher.group(3);
			return splitSelect(table, chooser, name, compare, syntax);
		}
		//6. 检查IN关键字
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_IN);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String value = matcher.group(2);
			return splitIn(table, name, value);
		}
		//7. 检查BETWEEN关键字和值
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_BETWEEN);
		matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String name = matcher.group(1);
			String abole = matcher.group(2);
			String value1 = matcher.group(3);
			String value2 = matcher.group(4);
			boolean yes = "BETWEEN".equalsIgnoreCase(abole);
			return this.splitBetween(table, name, yes, value1, value2);
		}
		//8. 二进制数据搜索(只限RAW类型)
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_RAW);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			String compare = matcher.group(2);
			String value = matcher.group(3);
			return splitRaw(table, name, compare, value);
		}
		//8. 字符串LIKE查询(限CHAR,SCHAR,WCHAR)
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_LIKE);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			String value = matcher.group(2);
			// 在解析前处理转义字符
			return splitLike(table, name, translate(value));
		}
		//9. 数值类型查询
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_NUMBER);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			String compare = matcher.group(2);
			String value = matcher.group(3);
			return splitNumber(table, name, compare, value);
		}

		//10. 日期格式
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_CALENDAR);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			String compare = matcher.group(2);
			String value = matcher.group(3);
			return splitString(table, name, compare, value);
		}
		//11. 字符串查询
		pattern = Pattern.compile(WhereParser.SQL_COLUMN_STRING);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String name = matcher.group(1);
			String compare = matcher.group(2);
			String value = matcher.group(3);
			// 解析前处理转义字符
			return splitString(table, name, compare, translate(value));
		}
		
		throw new SQLSyntaxException("illegal: %s", sql);
	}
	
//	/**
//	 * 参数在进入前，已经取消了括号，语句之间可能会存在"AND|OR"
//	 * 
//	 * @param sql
//	 * @return
//	 */
//	private Condition splitUnit2(Table table, Map<Space, Table> tables, String sql) {
//		//1. 过滤两侧可能存在的无意义的括号
//		sql = filteBrackets(sql);
//		//2. 找到"AND|OR",分隔成多列查询条件
//		String[] columns = splitWhereElement(sql);
//
//		//3.1  第1列是肯定不带(AND|OR)
//		Condition condi = splitColumn(table, tables, columns[0]);
//		//3.2  从第2列或以后肯定要带(AND|OR)
//		final String regex = "^\\s*(?i)(AND|OR)\\s+(.+)\\s*$";
//		for(int i = 1; i < columns.length; i++) {
//			Pattern pattern = Pattern.compile(regex);
//			Matcher matcher = pattern.matcher(columns[i]);
//			if(!matcher.matches()) {
//				throw new SQLSyntaxException("error:%s", columns[i]);
//			}
//			String logic = matcher.group(1);
//			String query = matcher.group(2);
//			
//			Condition partner = splitColumn(table, tables, query);
//			partner.setRelation( Condition.translateLogic(logic) );
//			condi.addPartner(partner);
//		}
//
//		return condi;
//	}
	
	/**
	 * 参数在进入前，已经取消了括号，语句之间可能会存在"AND|OR"
	 * 
	 */
	private Condition splitUnit(Table table, SQLChooser chooser, String sql) {
		//1. 过滤两侧可能存在的无意义的括号
		sql = filteBrackets(sql);
		//2. 根据逻辑连接符(AND|OR)，分隔成多列查询条件
		LogicString[] columns = this.splitWhereMember(sql);

		//3.1  第1列是肯定不带(AND|OR)
		Condition condi = splitColumn(table, chooser, columns[0].value);
		//3.2  从第2列或以后肯定要带(AND|OR)
		for(int i = 1; i < columns.length; i++) {
			String logic = columns[i].logic;
			String query = columns[i].value;
			
			Condition partner = splitColumn(table, chooser, query);
			if (logic != null && logic.length() > 0) {
				partner.setRelation(Condition.translateLogic(logic));
			}
			condi.addPartner(partner);
		}

		return condi;
	}
	
	/**
	 * 分割WHERE语句. 步骤:
	 * <1> 过滤最外两侧可能存在的括号(这个括号无用,但是必须匹配)
	 * <2> 按括号对进行分组
	 * <3> 检查每个一个分组,直到最小化(不存在括号)
	 * <4> 最小化后,放入单元中继续解析(splitUnit), 这一段设置"外部逻辑连接关系"
	 * <5> 解析单元首先将"AND|OR"提出来,对每一个列进行解析. 这一段设置同级逻辑连接关系)
	 * 
	 * @param table
	 * @param sqlWhere
	 */
	private Condition splitCondition(Table table, SQLChooser chooser, String sqlWhere) {		
		//1. 过滤两侧的括号
		sqlWhere = filteBrackets(sqlWhere);
		//2. 按"对称的括号对"进行分组
		String[] groups = splitGroup(sqlWhere);
		//3. 各分组检查
		Pattern pattern = Pattern.compile(GradationParser.SQL_PART_LOGICPREFIX);
		Condition parent = null;
		
		for(String group : groups) {
			//3.1  如果开始存在逻辑连接符号,取出来
			byte relation = Condition.NONE;
			Matcher matcher = pattern.matcher(group);
			if (matcher.matches()) {
				String logic = matcher.group(1);
				group = matcher.group(2);
				relation = Condition.translateLogic(logic);
			}
			//3.2  将一个分组切割成多个"段"
			String[] parts = splitGroup(group);
			
			//3.3  两种情况:<1>只有一个分组,表示没有括号,是最小单元. <2>继续分组
			if(parts.length == 1) {
				Condition condi = splitUnit(table, chooser, parts[0]);
				condi.setOutsideRelation(relation);
				if(parent == null) parent = condi;
				else parent.setLast(condi); // 下一级分组
			} else { // 有多组,继续分解,直以最小
				Condition slave = null;
				for(String part: parts) {
					byte slaveRelation = Condition.NONE;
					matcher = pattern.matcher(part);
					if (matcher.matches()) {
						String logic = matcher.group(1);
						part = matcher.group(2);
						slaveRelation = Condition.translateLogic(logic);
					}
					
					Condition condi = splitCondition(table, chooser, part);
					condi.setOutsideRelation(slaveRelation);
					if(slave == null) slave = condi;
					else slave.addPartner(condi);
				}
				slave.setOutsideRelation(relation);
				if(parent == null) parent = slave;
				else parent.setLast(slave);
			}
		}

		return parent;
	}

	/**
	 * 解析"WHERE"语句
	 * @param table
	 * @param query
	 * @param sql
	 */
	public Condition split(Table table, SQLChooser chooser, String sql) {
		return splitCondition(table, chooser, sql);
	}
}
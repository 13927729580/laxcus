/**
 * 
 */
package com.lexst.sql.parse;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.util.*;

public class InsertParser extends SQLParser {

	/** 插入一行记录: INSERT INTO SCHEMA.TABLE (column_name1, ...) VALUES (column1,...) */
	private final static String SQL_INSERT = "^\\s*(?i)(?:INSERT\\s+INTO)\\s+(\\w+)\\.(\\w+)\\s*\\((.+?)\\)\\s*(?i)VALUES\\s*\\((.+)\\)\\s*$";
	
	/** 插入多行记录: INJECT INTO SCHEMA.TABLE (column_name1,....) VALUES (column1,...),(column1,...),(column1,...) */
	private final static String SQL_INJECT = "^\\s*(?i)(?:INJECT\\s+INTO)\\s+(\\w+)\\.(\\w+)\\s*\\((.+?)\\)\\s*(?i)VALUES\\s*(.+)\\s*$";

	/** 分割INJECT INTO的 VALUES域 */
	private final static String SPLIT_COMMA = "^\\s*\\,\\s*(.+)$";
	private final static String SPLIT_VALUES = "^\\s*\\((.+?)\\)(\\s*\\,\\s*\\(.+|\\s*)$";
	
	/** 值参数格式  */
	private final static String SQL_RAW = "^\\s*(?i)0x([0-9a-fA-F]+)(\\s*\\,.+|\\s*)$";
	private final static String SQL_STRING = "^\\s*\\'(.+?)\\'(\\s*\\,.+|\\s*)$";
	private final static String SQL_NUMBER = "^\\s*([+|-]{0,1}[0-9]+[\\.]{0,1}[0-9]*)(\\s*\\,.+|\\s*)$";

	/**
	 * default
	 */
	public InsertParser() {
		super();
	}
	
	/**
	 * 过滤两侧的括号和括号之间的逗号，返回字符串数组
	 * 
	 * @param sql
	 * @return
	 */
	private String[] splitValues(String sql) {
		ArrayList<String> array = new ArrayList<String>();

		Pattern comma = Pattern.compile(InsertParser.SPLIT_COMMA);
		Pattern pattern = Pattern.compile(InsertParser.SPLIT_VALUES);
		Matcher matcher = pattern.matcher(sql);
		// 第一段前面必须没有逗号
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal values:%s", sql);
		}
		array.add(matcher.group(1));
		sql = matcher.group(2);
		// 后叙段前面必须有逗号
		while (sql.trim().length() > 0) {
			// 过滤 检查前面的逗号
			matcher = comma.matcher(sql);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("illegal values:%s", sql);
			}
			sql = matcher.group(1);

			// 判断字符串是否匹配
			matcher = pattern.matcher(sql);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("illegal values:%s", sql);
			}
			array.add(matcher.group(1));
			sql = matcher.group(2);
		}

		String[] s = new String[array.size()];
		return array.toArray(s);
	}
	
	/**
	 * 一行记录中，如果有某列不存在，定义一个默认值
	 * 
	 * @param row
	 * @param table
	 */
	private void fill(Row row, Table table) {
		for (ColumnAttribute attribute : table.values()) {
			short columnId = attribute.getColumnId();
			Column column = row.find(columnId);
			if (column != null) continue;
			// 生成一个默认值
			column = attribute.getDefault(columnId);
			if (column == null) {
				throw new SQLSyntaxException("%s cannot support default", attribute.getName());
			}
			row.add(column);
		}
	}
	
	/**
	 * 解析一行记录，返回Row对象
	 * @param table
	 * @param names
	 * @param sql
	 * @return
	 */
	private Row splitItem(Table table, String[] names, String sql) throws IOException {
		Pattern comma = Pattern.compile(InsertParser.SPLIT_COMMA);
		Row row = new Row();
		for(int i = 0; i < names.length; i++) {
			// 根据名称，找到匹配的属性
			ColumnAttribute attribute = table.find(names[i]);
			if(attribute == null) {
				throw new SQLSyntaxException("cannot find %s", names[i]);
			}
			// 第二行及以后需要去掉逗号
			if (i > 0) {
				Matcher matcher = comma.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal prefix: %s", sql);
				}
				sql = matcher.group(1);
			}
			// 参数不足
			if (sql.trim().isEmpty()) {
				throw new SQLSyntaxException("values missing!");
			}
			
			Column column = null;
			if (attribute.isRaw()) {
				// 二进制格式
				Pattern pattern = Pattern.compile(InsertParser.SQL_RAW);
				Matcher matcher = pattern.matcher(sql);
				if(!matcher.matches()) {
					throw new SQLSyntaxException("illegal raw:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				column = VariableGenerator.toRaw(table.isDSM(), (RawAttribute) attribute, atob(value));
			} else if (attribute.isCalendar() || attribute.isWord()) {
				// 字符串格式，按照字符串格式分解
				Pattern pattern = Pattern.compile(InsertParser.SQL_STRING);
				Matcher matcher = pattern.matcher(sql);
				if(!matcher.matches()) {
					throw new SQLSyntaxException("illegal string:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				if (attribute.isChar()) {
					column = VariableGenerator.toChar(table.isDSM(), (CharAttribute) attribute, value);
				} else if (attribute.isSChar()) {
					column = VariableGenerator.toSChar(table.isDSM(), (SCharAttribute) attribute, value);
				} else if (attribute.isWChar()) {
					column = VariableGenerator.toWChar(table.isDSM(), (WCharAttribute) attribute, value);
				} else if (attribute.isDate()) {
					column = splitDate((DateAttribute) attribute, value);
				} else if (attribute.isTime()) {
					column = splitTime((TimeAttribute) attribute, value);
				} else if (attribute.isTimestamp()) {
					column = splitTimestamp((TimestampAttribute) attribute, value);
				}
			} else if (attribute.isNumber()) {
				Pattern pattern = Pattern.compile(InsertParser.SQL_NUMBER);
				Matcher matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal number:%s", sql);
				}
				String value = matcher.group(1);
				sql = matcher.group(2);
				
				if (attribute.isShort()) {
					column = splitShort((ShortAttribute) attribute, value);
				} else if (attribute.isInteger()) {
					column = splitInt((IntegerAttribute)attribute, value);
				} else if (attribute.isLong()) {
					column = splitLong((LongAttribute)attribute, value);
				} else if (attribute.isFloat()) {
					column = splitFloat((FloatAttribute) attribute, value);
				} else if (attribute.isDouble()) {
					column = splitDouble((DoubleAttribute) attribute, value);
				}
			}
			
			if(column == null) {
				throw new SQLSyntaxException("cannot resolve:%s", sql);
			}
			
			column.setId(attribute.getColumnId());
			row.add(column);
		}
		
		// 填充没有的列
		fill(row, table);
		return row;
	}
	
	/**
	 * 分割属性名称
	 * 
	 * @param fields
	 * @return
	 */
	private String[] splitFieldNames(String fields) {
		return fields.split("\\s*\\,\\s*");
	}
	
	/**
	 * 解析 INSERT INTO 语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Insert splitInsert(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(InsertParser.SQL_INSERT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}
		Space space = new Space(matcher.group(1), matcher.group(2));
		Table table = chooser.findTable(space);
		if (table == null) {
			throw new SQLSyntaxException("cannot find %s", space);
		}
		
		String fields = matcher.group(3);
		String values = matcher.group(4);

		String[] names = splitFieldNames(fields);
		Row row = null;
		try {
			row = splitItem(table, names, values);
		} catch (IOException e) {
			throw new SQLSyntaxException(e);
		}

		Insert insert = new Insert(table, row);
		return insert;
	}

	/**
	 * 解析 INJECT INTO 语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Inject splitInject(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(InsertParser.SQL_INJECT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		Table table = chooser.findTable(space);		
		if (table == null) {
			throw new SQLSyntaxException("cannot find %s", space);
		}
		
		String fields = matcher.group(3);
		String values = matcher.group(4);
		
		String[] names = splitFieldNames(fields);
		String[] items = splitValues(values);
		Inject inject = new Inject(table);
		
		// 解析"INJECT INTO"的VALUES域
		for (String line : items) {
			// 表、属性名称、值集合，三项条件生成一行记录
			try {
				Row row = splitItem(table, names, line);
				// 加入集合
				inject.add(row);
			} catch (IOException e) {
				throw new SQLSyntaxException(e);
			}
		}
		
		return inject;
	}
	
}
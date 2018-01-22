/**
 * 
 */
package com.lexst.sql.parse;

import java.io.*;
import java.util.regex.*;

import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.util.*;

/**
 * 解析SQL UPDATE 语句
 */
public class UpdateParser extends SQLParser {

	/** 更新记录: UPDATE schema.table SET new_column_name=new_value, ... WHERE old_column_name=old_value [AND|OR] ... */
	private final static String SQL_UPDATE = "^\\s*(?i)UPDATE\\s+(\\w+)\\.(\\w+)\\s+(?i)SET\\s+(.+?)\\s+(?i)WHERE\\s+(.+)\\s*$";
	
	/** UPDATE SET 语句格式  */
	private final static String SPLIT_COMMA = "^\\s*\\,\\s*(.+)$";
	private final static String SPLIT_NAME = "^\\s*(\\w+?)(\\s*\\=\\s*.+)$";
	private final static String SQL_RAW = "^\\s*(\\w+?)\\s*\\=\\s*(?i)0x([0-9a-fA-F]+)(\\s*\\,\\s*\\w+.+|\\s*)$";
	private final static String SQL_STRING = "^\\s*(\\w+?)\\s*\\=\\s*\\'(.+?)\\'(\\s*\\,\\s*\\w+\\s*.+|\\s*)$";
	private final static String SQL_NUMBER = "^\\s*(\\w+?)\\s*\\=\\s*([+|-]{0,1}[0-9]+[\\\\.]{0,1}[0-9]*)(\\s*\\,\\s*\\w+\\s*.+|\\s*)$";

	/**
	 * default
	 */
	public UpdateParser() {
		super();
	}

	/**
	 * 解析 UPDATE scheme.table SET ... WHERE 语句
	 * 
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Update split(String sql, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(UpdateParser.SQL_UPDATE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("illegal update syntax");
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		Table table = chooser.findTable(space);
		if (table == null) {
			throw new SQLSyntaxException("cannot find: %s", space);
		}
		
		String sqlSet = matcher.group(3);
		String sqlWhere = matcher.group(4);

		Update update = new Update(space);
		// 解析更新字段
		try {
			splitSet(table, sqlSet, update);
		} catch(IOException e) {
			throw new SQLSyntaxException(e);
		}
		// 解析WHERE语句
		WhereParser parser = new WhereParser();
		Condition condi = parser.split(table, chooser, sqlWhere);
		update.setCondition(condi);
		
		return update;
	}
	
	/**
	 * 解析UPDATE SET语句中的列参数集合
	 * 
	 * @param table
	 * @param sql
	 * @param update
	 */
	private void splitSet(Table table, String sql, Update update) throws IOException {
		Pattern comma = Pattern.compile(UpdateParser.SPLIT_COMMA);
		Pattern key = Pattern.compile(UpdateParser.SPLIT_NAME);
		
		for (int index = 0; sql.trim().length() > 0; index++) {
			// 过滤分隔符逗号
			if (index > 0) {
				Matcher matcher = comma.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal values:%s", sql);
				}
				sql = matcher.group(1);
			}
			// 取列名称
			Matcher matcher = key.matcher(sql);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("invalid sql:%s", sql);
			}
			String name = matcher.group(1);
			// 根据名称，找到匹配的属性
			ColumnAttribute attribute = table.find(name);
			if (attribute == null) {
				throw new SQLSyntaxException("cannot find attribute by %s", name);
			}
			
			Column column = null;
			if(attribute.isRaw()) { 
				// 二进制数组格式
				Pattern pattern = Pattern.compile(UpdateParser.SQL_RAW);
				matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal raw:%s", sql);
				}
				String value = matcher.group(2);
				sql = matcher.group(3);
				column = VariableGenerator.toRaw(table.isDSM(), (RawAttribute) attribute, atob(value));
			} else if(attribute.isCalendar() || attribute.isWord()) { 
				// 字符串格式，包括日期和字符
				Pattern pattern = Pattern.compile(UpdateParser.SQL_STRING);
				matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal string:%s", sql);
				}
				String value = matcher.group(2);
				sql = matcher.group(3);

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
			} else if(attribute.isNumber()) {
				// 数字格式
				Pattern pattern = Pattern.compile(UpdateParser.SQL_NUMBER);
				matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throw new SQLSyntaxException("illegal number:%s", sql);
				}
				String value = matcher.group(2);
				sql = matcher.group(3);

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
			// 保存更新参数
			column.setId(attribute.getColumnId());
			update.add(column);
		}
	}

}
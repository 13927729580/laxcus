/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.function.value.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.Situation;

/**
 * GROUP BY HAVING 子句解析
 *
 */
public class HavingParser extends GradationParser {

	private final static String SQL_FUNCTION_LIKE = "^\\s*(\\w+\\s*\\(\\s*\\w+\\s*\\))\\s+(?i)LIKE\\s+\\'(.+)\\'\\s*$";
	private final static String SQL_FUNCTION_STRING = "^\\s*(\\w+\\s*\\(\\s*\\w+\\s*\\))\\s*(=|!=|<>)\\s*\\'(.+)\\'\\s*$";
	private final static String SQL_FUNCTION_NUMBER = "^\\s*(\\w+\\s*\\(\\s*\\w+\\s*\\))\\s*(=|!=|<>|>|<|>=|<=)\\s*([-]{0,1}[0-9]+[\\.]{0,1}[0-9]*)\\s*$";

	/**
	 * default
	 */
	public HavingParser() {
		super();
	}
	
	/**
	 * 根据正则表达式"贪婪"算法，找到"AND|OR"标记，分割字符串
	 * 
	 * @param sql
	 * @return
	 */
	private String[] splitByLogic(String sql) {
//		System.out.printf("{%s}\n", sql);
		// 贪婪算法，采取从右向左匹配(最大化匹配)
		final String regex = "^(.+)(\\s+(?i)AND|OR\\s+)(.+)\\s*$";
		List<String> array = new ArrayList<String>();
		do {
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(sql);
			if(!matcher.matches()) {
				array.add(0, sql);
				break;
			}
			sql = matcher.group(1);
			array.add(0, matcher.group(2) + matcher.group(3));
		} while(true);
		String[] s = new String[array.size()];
		return array.toArray(s);
	}
	
	private SQLValue splitLike(short columnId, Table table, String sql) {
		ColumnAttribute attribute = table.find(columnId);
		if(attribute == null) {
			throw new SQLSyntaxException("cannot find: %d", columnId);
		}
		return splitLike(attribute, sql);
	}
	
	private SQLValue splitString(short columnId, Table table, String text) {
		ColumnAttribute attribute = table.find(columnId);
		if (attribute == null) {
			throw new SQLSyntaxException("cannot find: %d", columnId);
		}
		if (!attribute.isWord()) {
			throw new SQLSyntaxException("this is not character: %d", columnId);
		}

		SQLString string = new SQLString(text);
		string.setSentient(((WordAttribute) attribute).isSentient());
		return string;
	}
	
	private SQLValue splitNumber(short columnId, Table table, String text) {
		ColumnAttribute attri = table.find(columnId);
		// 如果没有列ID,默认返回一个整型值
		if(attri == null) {
			return new SQLInteger( Integer.parseInt(text) );
		}
		
		if(!attri.isNumber()) {
			throw new SQLSyntaxException("this is not number: %d", columnId);
		}
		
		if(attri.isShort()) {
			return new SQLShort(Short.parseShort(text));
		} else if(attri.isInteger()) {
			return new SQLInteger(Integer.parseInt(text));
		} else if(attri.isLong()) {
			return new SQLong(Long.parseLong(text));
		} else if(attri.isFloat()) {
			
		} else if(attri.isDouble()) {
			
		} else if(attri.isDate()) {
			
		} else if(attri.isTime()) {
			
		} else if(attri.isTimestamp()) {
//			return new SQLTimestamp( )
		}
		
		return null;
	}
	
	/**
	 * 解析函数,返回查询
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private Situation splitFunction(Table table, String sql) {
		Pattern	pattern = Pattern.compile(HavingParser.SQL_FUNCTION_LIKE);
		Matcher matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String funcText = matcher.group(1);
			String content = matcher.group(2);			
			// 处理转义字符
			content = content.replaceAll("\\'", "\'");

			SQLFunction function =  SQLFunctionCreator.create(table, funcText);
			if (function == null) {
				throw new SQLSyntaxException("cannot create '%s'", funcText);
			} else if(!(function instanceof ColumnFunction)) {
				throw new SQLSyntaxException("%s is not aggregate function", funcText);
			}
			
			short columnId = ((ColumnFunction) function).getColumnId();
			SQLValue value = splitLike(columnId, table, content);
			
			return new Situation((ColumnFunction)function, Gradation.LIKE, value);
		}
		
		pattern = Pattern.compile(HavingParser.SQL_FUNCTION_STRING);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String funcText = matcher.group(1);
			String compare = matcher.group(2);
			String content = matcher.group(3);
			// 处理转义字符
			content = content.replaceAll("\\'", "\'");
			
			SQLFunction function = SQLFunctionCreator.create(table, funcText);
			if (function == null) {
				throw new SQLSyntaxException("cannot create '%s'", funcText);
			} else if(!(function instanceof ColumnFunction)) {
				throw new SQLSyntaxException("%s is not aggregate function", funcText);
			}
			
			short columnId = ((ColumnFunction) function).getColumnId();
			SQLValue value = this.splitString(columnId, table, content);
			
			return new Situation((ColumnFunction)function, Gradation.translateCompare(compare), value);
		}
		
		pattern = Pattern.compile(HavingParser.SQL_FUNCTION_NUMBER);
		matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String funcText = matcher.group(1);
			String compare = matcher.group(2);
			String content = matcher.group(3);
			
			SQLFunction function = SQLFunctionCreator.create(table, funcText);
			if (function == null) {
				throw new SQLSyntaxException("cannot create '%s'", funcText);
			} else if(!(function instanceof ColumnFunction)) {
				throw new SQLSyntaxException("%s is not aggregate function", funcText);
			}
			
			short columnId = ((ColumnFunction) function).getColumnId();
			SQLValue value = splitNumber(columnId, table, content);
			return new Situation((ColumnFunction)function, Gradation.translateCompare(compare), value);
		}
		
		throw new SQLSyntaxException("Illegal: %s", sql);
	}
	
	/**
	 * 参数在进入前，已经取消了括号，语句之间可能会存在"AND|OR"
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	private Situation splitUnit(Table table, String sql) {
		//1. 过滤两侧可能存在的无意义的括号
		sql = filteBrackets(sql);
		//2. 找到"AND|OR",分隔成多列查询条件
		String[] columns = splitByLogic(sql);

		//3.1  第1列是肯定不带(AND|OR)
		Situation situa = splitFunction(table, columns[0]);
		//3.2  从第2列或以后肯定要带(AND|OR)
		final String regex = "^\\s*(?i)(AND|OR)\\s+(.+)\\s*$";
		for(int i = 1; i < columns.length; i++) {
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(columns[i]);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("error:%s", columns[i]);
			}
			String logic = matcher.group(1);
			String query = matcher.group(2);
			
			Situation partner = splitFunction(table, query);
			partner.setRelation( Condition.translateLogic(logic) );
			situa.addPartner(partner);
		}

		return situa;
	}
	
//	/**
//	 * 将一个WHERE查询语句，找到最外层的"AND|OR|("(AND|OR加上左括号)分割点进行分割
//	 * 
//	 * @param sql
//	 * @return
//	 */
//	private String[] splitGroup1(String sql) {
//		//1. 过滤两侧无意义的括号
//		sql = this.filteBrackets(sql);
//		//2. 对WHERE语句进行分组
//		int index = 0, begin = 0, seek = 0;
//		boolean ignore = false;
//		List<String> array = new ArrayList<String>();
//		while (seek < sql.length()) {
//			char w = sql.charAt(seek++);
//			
//			if (w == '\'') { // 过滤单引号之间的数据
//				if (seek - 2 >= 0) {
//					w = sql.charAt(seek - 2);
//					if (w == '\\') continue;
//				}
//				ignore = !ignore;
//			} else if (ignore) { // 在单引号之间的数据
//				continue;
//			} else if (w == '(') {
//				
////				if(array.isEmpty()) { // 第一个分段开始
////					final String regex = "^\\s*(.+)(\\s+(?i)(?:AND|OR)\\s+\\()$";
////					Pattern pattern = Pattern.compile(regex);
////					Matcher matcher = pattern.matcher(sql.substring(0, seek));
////					if(matcher.matches()) {
////						int start = matcher.start(2);
////						array.add(sql.substring(0, start));
////						begin = start;
////					}
////				}
//				
//				if (index == 0) {
//					final String regex = "^\\s*(.+)\\s+(?i)(AND|OR)\\s+\\(\\s*$";
//					String prefix = sql.substring(begin, seek);
////					String suffix = sql.substring(seek);
////					System.out.printf("前缀:[%s] | {%s}\n", prefix, suffix);
//					
//					Pattern pattern = Pattern.compile(regex);
//					Matcher matcher = pattern.matcher(prefix);
//					if (matcher.matches()) {
//						int start = matcher.start(2);
//						prefix = sql.substring(begin, start);
////						System.out.printf("%s | %d | %s\n", prefix, start, sql.substring(start));
//						array.add(prefix);
//						begin = start;
//					}
//				}
//				
//				index++;
//			} else if (w == ')') {
//				index--;
//				if (index != 0) continue;  // 未归0，继续
//				
//				// 归0，判断标记值。匹配则记录
//				final String regex = "^\\)\\s+(?i)(AND|OR)\\s+(.+)\\s*$";
//				String prefix = sql.substring(begin, seek);
//				String suffix = sql.substring(seek - 1);
////				System.out.printf("后缀:{%s} | [%s]\n", prefix, suffix);
//				
//				Pattern pattern = Pattern.compile(regex);
//				Matcher matcher = pattern.matcher(suffix);
//				if (matcher.matches()) {
//					array.add(prefix);
//					begin = seek;
//				}
//			}
//			
//		}
//
//		if (ignore || index != 0) {
//			throw new SQLSyntaxException("Illegal Syntax: %s", sql);
//		}
//		
//		if(begin < sql.length()) {
//			array.add(sql.substring(begin));
//		}
//		
////		// debug code, start
////		for(String s: array) {
////			System.out.printf("[%s]\n", s);
////		}
////		System.out.println("----------------");
////		// debug code, end
//		
//		String[] s = new String[array.size()];
//		return array.toArray(s);
//	}

	/**
	 * 分割HAVING语句. 步骤:
	 * <1> 过滤最外两侧可能存在的括号(这个括号无用,但是必须匹配)
	 * <2> 按括号对进行分组
	 * <3> 检查每个一个分组,直到最小化(不存在括号)
	 * <4> 最小化后,放入单元中继续解析(splitUnit), 这一段设置"外部逻辑连接关系"
	 * <5> 解析单元首先将"AND|OR"提出来,对每一个列进行解析. 这一段设置同级逻辑连接关系)
	 * 
	 * @param table
	 * @param sqlHaving
	 * @return
	 */
	private Situation splitSituation(Table table, String sqlHaving) {
		//1. 过滤两侧的括号
		sqlHaving = filteBrackets(sqlHaving);
		//2. 按"对称的括号对"进行分组
		String[] groups = splitGroup(sqlHaving);
		//3. 各分组检查
		Pattern pattern = Pattern.compile(HavingParser.SQL_PART_LOGICPREFIX);
		Situation parent = null;

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
				Situation situa = splitUnit(table, parts[0]);
				situa.setOutsideRelation(relation);
				if(parent == null) parent = situa;
				else parent.setLast(situa); // 下一级分组
			} else { // 有多组,继续分解,直以最小
				Situation slave = null;
				for(String part: parts) {
					byte slaveRelation = Condition.NONE;
					matcher = pattern.matcher(part);
					if (matcher.matches()) {
						String logic = matcher.group(1);
						part = matcher.group(2);
						slaveRelation = Condition.translateLogic(logic);
					}
					
					Situation situa = splitSituation(table, part);
					situa.setOutsideRelation(slaveRelation);
					if(slave == null) slave = situa;
					else slave.addPartner(situa);
				}
				slave.setOutsideRelation(relation);
				if(parent == null) parent = slave;
				else parent.setLast(slave);
			}
		}
		
		return parent;
	}

	/**
	 * 解析"HAVING"语句
	 * 
	 * @param table
	 * @param sqlHaving
	 * @return
	 */
	public Situation split(Table table, String sqlHaving) {
		return this.splitSituation(table, sqlHaving);
	}
	
}
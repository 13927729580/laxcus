/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.value.*;

/**
 * 
 *
 */
public class GradationParser extends SQLParser {

	/** 截取表示式前面的逻辑连接符号  */
	protected final static String SQL_PART_LOGICPREFIX =  "^\\s*(?i)(AND|OR)\\s+(.+)\\s*$";	
	/** LIKE表达式 */
	protected final static String SQL_LIKE = "^\\s*([%_]*)(.+?)([%_]*)\\s*$";

	/**
	 * 
	 */
	public GradationParser() {
		super();
	}
	
	/**
	 * 分析忽略字数 ，-1表示无限制
	 * 
	 * @param symbol
	 * @return
	 */
	protected short getLikeSize(String symbol) {
		if (symbol.isEmpty()) {
			return 0;
		} else if ("%".equalsIgnoreCase(symbol)) {
			return -1; // 0xffff,无限制
		}
		
		for (int i = 0; i < symbol.length(); i++) {
			char w = symbol.charAt(i);
			if (w != '_') {
				throw new SQLSyntaxException("invalid like symbol:%s", symbol);
			}
		}
		return (short) symbol.length();
	}
	
	/**
	 * 取出字符串两侧的"%","_"字符
	 * @param attri
	 * @param sql
	 * @return
	 */
	protected SQLString splitLike(ColumnAttribute attribute, String sql) {
		if (!attribute.isWord()) {
			throw new SQLSyntaxException("invalid column:%s", attribute.getName());
		}
		WordAttribute consts = (WordAttribute) attribute;
		if (!consts.isLike()) {
			throw new SQLSyntaxException("cannot support LIKE by '%s'", attribute.getName());
		}
		
		Pattern pattern = Pattern.compile(GradationParser.SQL_LIKE);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			throw new SQLSyntaxException("invalid sql like:%s", sql);
		}
		short left = getLikeSize(matcher.group(1));
		String text = matcher.group(2);
		short right = getLikeSize(matcher.group(3));
		
		SQLString string = new SQLString(left, right, text);
		string.setSentient(consts.isSentient());
		return string;
	}

//	/**
//	 * 根据正则表达式"贪婪"算法，找到"AND|OR"标记，分割字符串
//	 * 
//	 * @param sql
//	 * @return
//	 */
//	protected String[] splitByLogic2(String sql) {
//		System.out.printf("{%s}\n", sql);
//		// 贪婪算法，采取从右向左匹配(最大化匹配)
//		final String regex = "^(.+)(\\s+(?i)AND|OR\\s+)(.+)\\s*$";
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
//		String[] s = new String[array.size()];
//		return array.toArray(s);
//	}

//	protected String[] splitByLogic(String sql) {
//		System.out.printf("{%s}\n", sql);
//		// 采用由右向左匹配
//		final String sep = "^\\s*(\\w+\\s+(?i)(?:NOT\\s+BETWEEN|BETWEEN)\\s+.+?\\s+(?i)(?:AND)\\s+.+?)(\\s+.+|\\s*)$";
//		// 贪婪算法，采取从右向左匹配(最大化匹配)
//		final String regex = "^(.+?)(\\s+(?i)AND|OR\\s+)(.+)\\s*$";
//		List<String> array = new ArrayList<String>();
//		do {
//			System.out.printf("{%s}\n", sql);
//			//1. 检查特殊情况
//			Pattern pattern = Pattern.compile(sep);
//			Matcher matcher = pattern.matcher(sql);
//			if(matcher.matches()) {
//				array.add(0, matcher.group(1));
//				sql = matcher.group(2);
//				continue;
//			}
//			//2. 一般情况
//			pattern = Pattern.compile(regex);
//			matcher = pattern.matcher(sql);
//			if(!matcher.matches()) {
//				array.add(0, sql);
//				break;
//			}
//			sql = matcher.group(1);
//			array.add(0, matcher.group(2) + matcher.group(3));
//		} while(true);
//		
//		for(String s : array) {
////			System.out.printf("{%s}\n", s);
//		}
//
//		String[] s = new String[array.size()];
//		return array.toArray(s);
//	}
	
	/**
	 * 过滤外层的括号， 取出中间数据(外层括号必须是匹配的)
	 * 
	 * @param sql
	 * @return
	 */
	protected String filteBrackets(String sql) {
		final String regex = "^\\s*\\((.+)\\)\\s*$";
		do {
			//1. 表达式是否匹配,不匹配退出
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(sql);
			if (!matcher.matches()) break;

			//2. 检查最旁边两侧的括号是否对称
			int index = 0, seek = 0;
			boolean begin = false, ignore = false;
			
			while (seek < sql.length()) {
				char w = sql.charAt(seek++);
				if (w == '\'') {
					ignore = !ignore;
				} else if (ignore) {
					continue;
				} else if (w == '(') { // 在第一个括号前,必须是空字符
					if(!begin) {
						String s = sql.substring(0, seek - 1);
						begin = s.trim().isEmpty();
					}
					index++;
				} else if (w == ')') {
					index--;
				}
			}

			// 首字符必须是左括号,括号对必须匹配
			if (begin && index == 0) {
				sql = matcher.group(1);
				continue;
			}
		} while (false);

		return sql;
	}

	/**
	 * 将一个WHERE查询语句，找到最外层的"AND|OR|("(AND|OR加上左括号)分割点进行分割
	 * 
	 * 按照括号对进行分组,判断条件是:
	 * 1. 左括号'('之前必须有 "AND|OR"
	 * 2. 右括号')'之后必须有 "AND|OR"
	 * 
	 * @param sql
	 * @return
	 */
	protected String[] splitGroup(String sql) {
		//1. 过滤两侧无意义的括号
		sql = this.filteBrackets(sql);
		//2. 对WHERE语句进行分组
		int index = 0, begin = 0, seek = 0;
		boolean ignore = false;
		List<String> array = new ArrayList<String>();
		while (seek < sql.length()) {
			char w = sql.charAt(seek++);
			
			if (w == '\'') { // 过滤单引号之间的数据
				if (seek - 2 >= 0) {
					w = sql.charAt(seek - 2);
					if (w == '\\') continue;
				}
				ignore = !ignore;
			} else if (ignore) { // 在单引号之间的数据
				continue;
			} else if (w == '(') {
				if (index == 0) {
					final String regex = "^(.+)(\\s+(?i)(?:AND|OR)\\s+\\()$";
					String prefix = sql.substring(begin, seek); // 取前缀
					Pattern pattern = Pattern.compile(regex);
					Matcher matcher = pattern.matcher(prefix); 
					if (matcher.matches()) {
						prefix = matcher.group(1);
						if (prefix.trim().length() > 0) array.add(prefix);
						begin += prefix.length();
					}
				}				
				index++;
			} else if (w == ')') {
				index--;
				if (index != 0) continue; // 没有归0,不处理				
				
				final String regex = "^(\\)\\s+(?i)(?:AND|OR)\\s+)(.+)$";
				String suffix = sql.substring(seek - 1);
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(suffix); 
				if (matcher.matches()) {
					String prefix = sql.substring(begin, seek);
					if (prefix.trim().length() > 0) array.add(prefix);
					begin = seek;
				}
			}
		}

		if (ignore || index != 0) {
			throw new SQLSyntaxException("illegal sql:%s", sql);
		}
		
		if(begin < sql.length()) {
			array.add(sql.substring(begin));
		}

		String[] s = new String[array.size()];		
		return array.toArray(s);
	}
}
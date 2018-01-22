/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * sql syntax parser  (super class)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/27/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;

import java.math.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.sql.column.attribute.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;

/**
 * SQL语法解析基础类
 *
 */
public class SQLParser {

	protected final static int m = 1024 * 1024;
	protected final static long M = 1024 * 1024;
	protected final static long G = 1024 * 1024 * 1024;
	protected final static long T = 1024 * 1024 * 1024 * 1024;
	protected final static long P = 1024 * 1024 * 1024 * 1024 * 1024;

	private final static String FILTE_COMMA = "^\\s*(?:,)\\s*(.+)$";
	
	/**
	 * default
	 */
	protected SQLParser() {
		super();
	}
	
	/*
	 * 在限定符之后添加问号(?)，则使限定符成为“勉强模式”。
	 * 勉强模式的限定符，总是尽可能少的匹配。
	 * 如果之后的表达式匹配失败，勉强模式也可以尽可能少的再匹配一些，以使整个表达式匹配成功。
	 */
	
//	/** IPv4 地址格式  **/
//	private final static String IPv4 = "^\\s*([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\s*$";

	private final static String SQL_SHORT = "^\\s*([+|-]{0,1}[0-9]{1,5})\\s*$";
	private final static String SQL_INTEGER = "^\\s*([+|-]{0,1}[0-9]{1,10})\\s*$";
	private final static String SQL_LONG = "^\\s*([+|-]{0,1}[0-9]{1,19})\\s*$";
	private final static String SQL_DECIMAL = "^\\s*([+|-]{0,1}[0-9]+[\\.]{0,1}[0-9]*)\\s*$";
	
	/**
	 * 日期格式: (年/月/日, 年-月-日, 年.月.日, 年月日). 如果年份采用两位数字，如"12"，默认是"2012"
	 * 时间格式: (时:分:秒   毫秒) 毫秒可省略，秒与毫秒之间由空格分隔
	 * 时间戳(日期时间)格式. 是日期和时间的组合，中间由空格分开.
	 */
	
	/** 日期格式 **/
	private final static String TABLE_COLUMN_DATE1 = "^\\s*([0-9]{4}|[0-9]{2})\\.([0-9]{1,2})\\.([0-9]{1,2})\\s*$";
	private final static String TABLE_COLUMN_DATE2 = "^\\s*([0-9]{4}|[0-9]{2})\\-([0-9]{1,2})\\-([0-9]{1,2})\\s*$";
	private final static String TABLE_COLUMN_DATE3 = "^\\s*([0-9]{4}|[0-9]{2})\\/([0-9]{1,2})\\/([0-9]{1,2})\\s*$";
	private final static String TABLE_COLUMN_DATE4 = "^\\s*([0-9]{4})([0-9]{2})([0-9]{2})\\s*$";

	/** 时间格式 **/
	private final static String TABLE_COLUMN_TIME = "^\\s*([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})(\\s+[0-9]{1,3}|\\s*)$";

	/**
	 * throw a error
	 * @param sql
	 * @param index
	 */
	protected void throwable(String sql, int index) throws SQLSyntaxException {
		StringBuilder b = new StringBuilder();
		if (index > 0) {
			char[] c = new char[index - 1];
			for (int i = 0; i < c.length; i++) {
				c[i] = 0x20;
			}
			b.append(c);
		}
		b.append('^');
		String s = String.format("%s\n%s\nsql syntax error", sql, b.toString());
		throw new SQLSyntaxException(s);
	}
	
	/**
	 * 弹出错误
	 * @param s
	 * @throws SQLSyntaxException
	 */
	protected void throwable(String s) throws SQLSyntaxException {
		throw new SQLSyntaxException(s);
	}
	
	/**
	 * 弹出错误
	 * @param format
	 * @param args
	 * @throws SQLSyntaxException
	 */
	protected void throwable(String format, Object... args) throws SQLSyntaxException {
		throwable(String.format(format, args));	
	}

	protected final short buildLikeId(short columnId) {
		return (short) (columnId | 0x8000);
	}

	protected final short buildNormalId(short columnId) {
		return (short) (columnId & 0x7FFF);
	}
	
	/**
	 * 返回一个short值
	 * @param sql
	 * @return
	 */
	public short splitShort(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_SHORT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal short:%s", sql);
		}
		
		String s = matcher.group(1);
		if (s.charAt(0) == '+') s = s.substring(1);

		BigInteger value = new BigInteger(s);
		BigInteger min = new BigInteger(java.lang.Short.toString(java.lang.Short.MIN_VALUE));
		BigInteger max = new BigInteger(java.lang.Short.toString(java.lang.Short.MAX_VALUE));
		if (min.compareTo(value) <= 0 && value.compareTo(max) <= 0) {
			return value.shortValue();
		}

		throw new SQLSyntaxException("short %s out!", sql);
	}
	
	/**
	 * 返回Short列
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitShort(ShortAttribute attribute, String value) {
		short num = splitShort(value);
		return new com.lexst.sql.column.Short(attribute.getColumnId(), num);
	}

	/**
	 * 返回一个int值
	 * @param sql
	 * @return
	 */
	public int splitInt(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_INTEGER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal int:%s", sql);
		}
		
		String s = matcher.group(1);
		if (s.charAt(0) == '+') s = s.substring(1);

		BigInteger value = new BigInteger(s);
		BigInteger min = new BigInteger(java.lang.Integer.toString(java.lang.Integer.MIN_VALUE));
		BigInteger max = new BigInteger(java.lang.Integer.toString(java.lang.Integer.MAX_VALUE));
		if (min.compareTo(value) <= 0 && value.compareTo(max) <= 0) {
			return value.intValue();
		}

		throw new SQLSyntaxException("int %s out!", sql);
	}
	
	/**
	 * 返回Integer列
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitInt(IntegerAttribute attribute, String value) {
		int num = splitInt(value);
		return new com.lexst.sql.column.Integer(attribute.getColumnId(), num);
	}
	
	/**
	 * 返回一个long值
	 * @param sql
	 * @return
	 */
	public long splitLong(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_LONG);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal long:%s", sql);
		}
		
		String s = matcher.group(1);
		if (s.charAt(0) == '+') s = s.substring(1);

		BigInteger value = new BigInteger(s);
		BigInteger min = new BigInteger(java.lang.Long.toString(java.lang.Long.MIN_VALUE));
		BigInteger max = new BigInteger(java.lang.Long.toString(java.lang.Long.MAX_VALUE));
		if (min.compareTo(value) <= 0 && value.compareTo(max) <= 0) {
			return value.longValue();
		}

		throw new SQLSyntaxException("long %s out!", sql);
	}
	
	/**
	 * 解析long值，返回Long列
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitLong(LongAttribute attribute, String value) {
		long num = splitLong(value);
		return new com.lexst.sql.column.Long(attribute.getColumnId(), num);
	}
	
	/**
	 * 返回float值
	 * @param sql
	 * @return
	 */
	public float splitFloat(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_DECIMAL);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal float:%s", sql);
		}

		String s = matcher.group(1);
		if (s.charAt(0) == '+') s = s.substring(1);

		BigDecimal value = new BigDecimal(s);
		BigDecimal min = new BigDecimal(java.lang.Float.toString(java.lang.Float.MIN_VALUE));
		BigDecimal max = new BigDecimal(java.lang.Float.toString(java.lang.Float.MAX_VALUE));

		if (min.compareTo(value) <= 0 && value.compareTo(max) <= 0) {
			return value.floatValue();
		}

		throw new SQLSyntaxException("float %s out!", sql);
	}
	
	/**
	 * 解析float值，返回Float列
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitFloat(FloatAttribute attribute, String value) {
		float num = splitFloat(value);
		return new com.lexst.sql.column.Float(attribute.getColumnId(), num);
	}

	/**
	 * 返回double值
	 * @param sql
	 * @return
	 */
	public double splitDouble(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_DECIMAL);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal double:%s", sql);
		}

		String s = matcher.group(1);
		if (s.charAt(0) == '+') s = s.substring(1);

		BigDecimal value = new BigDecimal(s);
		BigDecimal min = new BigDecimal(java.lang.Double.toString(java.lang.Double.MIN_VALUE));
		BigDecimal max = new BigDecimal(java.lang.Double.toString(java.lang.Double.MAX_VALUE));

		if (min.compareTo(value) <= 0 && value.compareTo(max) <= 0) {
			return value.doubleValue();
		}

		throw new SQLSyntaxException("double %s out!", sql);
	}
	
	/**
	 * 解析double值，返回Double列
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitDouble(DoubleAttribute attribute, String value) {
		double num = splitDouble(value);
		return new com.lexst.sql.column.Double(attribute.getColumnId(), num);
	}

	/**
	 * 判断是不是逗号前缀
	 * 
	 * @param sql
	 * @return
	 */
	public boolean isCommaPrefix(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.FILTE_COMMA);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	/**
	 * 过滤逗号(如果前面有逗号则过滤，否则原值返回)
	 * @param sql
	 * @return
	 */
	protected String filteCommaPrefix(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.FILTE_COMMA);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			return matcher.group(1);
		}
		return sql;
	}


	
//	/**
//	 * hex string change to binary array
//	 * 
//	 * @param data
//	 * @return
//	 */
//	protected byte[] hexToBytes1(String data) {
//		if (data.length() < 2 || data.length() % 2 != 0) {
//			throw new SQLSyntaxException("invalid raw string:%s", data);
//		}
//		byte[] array = new byte[data.length() / 2];
//		for (int i = 0, j = 0; i < data.length(); i += 2) {
//			String s = data.substring(i, i + 2);
//			try {
//				byte b = (byte) (Integer.parseInt(s, 16) & 0xFF);
//				array[j++] = b;
//			} catch (NumberFormatException exp) {
//				throw new SQLSyntaxException("invalid hex word:%s", data);
//			}
//		}
//		return array;
//	}
	
	/**
	 * 将十六进制字符串转为字节流
	 * 
	 * @param text
	 * @return
	 */
	protected byte[] atob(String text) {
		int len = text.length() / 2 + (text.length() % 2);
		byte[] array = new byte[len];
		try {
			int seek = 0, index = 0;
			if (text.length() % 2 == 1) {
				String s = text.substring(0, 1);
				array[index++] = (byte) (Integer.parseInt(s, 16) & 0xFF);
				seek++;
			}
			for (; seek < text.length(); seek += 2) {
				String s = text.substring(seek, seek + 2);
				array[index++] = (byte) (Integer.parseInt(s, 16) & 0xFF);
			}
		} catch (NumberFormatException e) {
			throw new SQLSyntaxException("invalid hex word:%s", text);
		}
		return array;
	}

//	/**
//	 * 是一组IP地址串分解成IPV4地址数组字符串。分隔符是逗号
//	 * 
//	 * @param sql
//	 * @return
//	 */
//	protected List<String> splitIP(String sql) {
//		String[] total = sql.split(",");
//		ArrayList<String> array = new ArrayList<String>();
//		for (int i = 0; total != null && i < total.length; i++) {
//			if (!isIPv4(total[i])) {
//				throw new SQLSyntaxException("invalid ipv4 address: %s", total[i]);
//			}
//			array.add(total[i]);
//		}
//		return array;
//	}
	
	/**
	 * 解析网络地址(IPv6或者IPv4)
	 * @param sql
	 * @return
	 */
	protected List<Address> splitIP(String sql) {
		String[] items = sql.split(",");
		ArrayList<Address> array = new ArrayList<Address>();
		for (int i = 0; items != null && i < items.length; i++) {
			try {
				array.add(new Address(items[i]));
			} catch (UnknownHostException e) {
				throw new SQLSyntaxException("invalid network address: %s", items[i]);
			}
		}
		return array;
	}

	/**
	 * 解析日期格式
	 * 
	 * @param sql
	 * @return
	 * @throws SQLSyntaxException
	 */
	protected int splitDate(String sql) {
		String[] regexs = { SQLParser.TABLE_COLUMN_DATE1,
				SQLParser.TABLE_COLUMN_DATE2, SQLParser.TABLE_COLUMN_DATE3, 
				SQLParser.TABLE_COLUMN_DATE4 };

		int year = 0, month = 0, day = 0;
		for (int i = 0; i < regexs.length; i++) {
			Pattern pattern = Pattern.compile(regexs[i]);
			Matcher matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String s = matcher.group(1);
				if (s.length() == 2) {
					s = "20" + s;
				}
				year = Integer.parseInt(s);
				month = Integer.parseInt(matcher.group(2));
				day = Integer.parseInt(matcher.group(3));
				return SimpleDate.format(year, month, day);
			}
		}

		throw new SQLSyntaxException("invalid date:%s", sql);
	}
	
	/**
	 * 解析日期格式，返回Date列
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitDate(DateAttribute attribute, String value) {
		int num = splitDate(value);
		return new com.lexst.sql.column.Date(attribute.getColumnId(), num);
	}

	/**
	 * 解析时间格式
	 * 
	 * @param sql
	 * @return
	 * @throws SQLSyntaxException
	 */
	protected int splitTime(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.TABLE_COLUMN_TIME);
		Matcher matcher = pattern.matcher(sql);

		// 不匹配，弹出错误
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid time: %s", sql);
		}

		int hour = Integer.parseInt(matcher.group(1));
		int minute = Integer.parseInt(matcher.group(2));
		int second = Integer.parseInt(matcher.group(3));
		int milsecond = 0;
		String s = matcher.group(4).trim();
		if (s.length() > 0) milsecond = Integer.parseInt(s);

		// 检查时间范围
		if (!(0 <= hour && hour < 24) || !(0 <= minute && minute < 60)
				|| !(0 <= second && second < 60)) {
			throw new SQLSyntaxException("invalid time: %s", sql);
		}

		return SimpleTime.format(hour, minute, second, milsecond);
	}

	/**
	 * 解析时间格式，返回Time列
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitTime(TimeAttribute attribute, String value) {
		int num = splitTime(value);
		return new com.lexst.sql.column.Time(attribute.getColumnId(), num);
	}

	/**
	 * 解析时间戳格式
	 * 
	 * @param sql
	 * @return
	 * @throws SQLSyntaxException
	 */
	protected long splitTimestamp(String sql) {
		int index = sql.indexOf(0x20);
		if (index == -1) {
			throw new SQLSyntaxException("invalid timestamp:%s", sql);
		}
		
		String s1 = sql.substring(0, index);
		String s2 = sql.substring(index + 1);
		
		int d = this.splitDate(s1);
		int t = this.splitTime(s2);		
		java.util.Date dt = SimpleDate.format(d);
		java.util.Date tt = SimpleTime.format(t);

		Calendar instan = Calendar.getInstance();
		instan.setTime(dt);
		Calendar mode = Calendar.getInstance();
		mode.setTime(tt);

		instan.set(Calendar.HOUR_OF_DAY, mode.get(Calendar.HOUR_OF_DAY));
		instan.set(Calendar.MINUTE, mode.get(Calendar.MINUTE));
		instan.set(Calendar.SECOND, mode.get(Calendar.SECOND));
		instan.set(Calendar.MILLISECOND, mode.get(Calendar.MILLISECOND));

		return SimpleTimestamp.format(instan.getTime());
	}

	/**
	 * 解析时间戳格式，返回Timestamp列
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public com.lexst.sql.column.Column splitTimestamp(TimestampAttribute attribute, String value) {
		long num = splitTimestamp(value);
		return new com.lexst.sql.column.Timestamp(attribute.getColumnId(), num);
	}

}
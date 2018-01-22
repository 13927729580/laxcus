/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * "schmea" syntax parser
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/27/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.schema.*;

public class SchemaParser extends SQLParser {

	/** 建立数据库语法格式: create schema|create database name  maxsize=xxx[M|G|T|P] **/
//	private final static String CREATE_SCHEMA = "^\\s*(?:(?i)CREATE\\s+SCHEMA|CREATE\\s+DATABASE)\\s+(\\w+?)(\\s+.+|\\s*)$";
	private final static String CREATE_SCHEMA = "^\\s*(?i)(?:CREATE\\s+SCHEMA|CREATE\\s+DATABASE)\\s+(\\w+?)(\\s+.+|\\s*)$";
	private final static String MAXSIZE = "^\\s*(?i)(?:MAXSIZE\\s*=\\s*)([0-9]{1,})(?i)(M|G|T|P)\\s*$";

	/** 删除数据库|显示数据库的语法格式 **/
	private final static String DROP_SCHEMA = "^\\s*(?i)(?:DROP\\s+SCHEMA|DROP\\s+DATABASE)\\s+(\\w+)\\s*$";
	private final static String SHOW_SCHEMA = "^\\s*(?i)(?:SHOW\\s+SCHEMA|SHOW\\s+DATABASE)\\s+(\\w+)\\s*$";

	/**
	 * default
	 */
	public SchemaParser() {
		super();
	}

	/**
	 * 解析"CREATE SCHEMA"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Schema splitCreateSchema(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SchemaParser.CREATE_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}

		String name = matcher.group(1);
		if (online && chooser.onSchema(name)) {
			throw new SQLSyntaxException("%s existed!", name);
		}
		// 取出数据库名称
		Schema schema = new Schema(name);
		// 其实参数（可能有或者没有)
		String suffix = matcher.group(2);
		// 解析参数
		while (suffix.trim().length() > 0) {			
			pattern = Pattern.compile(SchemaParser.MAXSIZE);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				String digit = matcher.group(1);
				String unit = matcher.group(2);
				suffix = matcher.group(3);

				long value = Long.parseLong(digit);
				if ("M".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * SQLParser.M);
				} else if ("G".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * SQLParser.G);
				} else if ("T".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * SQLParser.T);
				} else if ("P".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * SQLParser.P);
				}
				continue;
			}
			
			throw new SQLSyntaxException("invalid syntax:" + suffix);
		}
		return schema;
	}

	/**
	 * 解析"DROP SCHEMA"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Schema splitDropSchema(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SchemaParser.DROP_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}

		String name = matcher.group(1);
		if (online && !chooser.onSchema(name)) {
			throw new SQLSyntaxException("cannot find %s", name);
		}
		return new Schema(name);
	}

	/**
	 * 解析"SHOW SCHEMA"语句
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public String splitShowSchema(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(SchemaParser.SHOW_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}

		String schema = matcher.group(1);
		if (online && !chooser.onSchema(schema)) {
			throw new SQLSyntaxException("cannot find %s", schema);
		}
		return schema;
	}
}
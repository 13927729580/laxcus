/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * "grant" syntax parser
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

import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;

/**
 * 数据库的用户、数据库、数据库表授权权限表解析类。<br>
 * 
 * 权限表文本参数见Control类中定义。<br>
 *
 */
public class GrantParser extends PermitParser {
	
	/** 用户权限定义，格式: GRANT 权限表  TO 用户名表 **/
	private final static String GRANT_USER   = "^\\s*(?i)(?:GRANT)\\s+(\\p{Print}+?)\\s+(?i)(?:TO)\\s+(.+?)\\s*$";

	/** 数据库权限定义，格式: GRANT 权限表  ON SCHEMA|ON DATABASE 数据库名   TO 用户名(只能有一个) **/
	private final static String GRANT_SCHEMA = "^\\s*(?i)(?:GRANT)\\s+(\\p{Print}+?)\\s+(?i)(?:ON\\s+SCHEMA|ON\\s+DATABASE)\\s+(\\w+)\\s+(?i)(?:TO)\\s+(.+)\\s*$"; 

	/** 数据库表权限定义，格式: GRANT 权限表 ON TABLE 数据库表名  TO 用户名 (只能有一个) */
	private final static String GRANT_TABLE  = "^\\s*(?i)(?:GRANT)\\s+(\\p{Print}+?)\\s+(?i)(?:ON\\s+TABLE)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:TO)\\s+(.+)\\s*$"; 
	
	/**
	 * 初始化
	 */
	public GrantParser() {
		super();
	}
	
	/**
	 * 解析授权配置
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Permit split(String sql, boolean online, SQLChooser chooser) {
		//1. 数据库表授权
		Permit permit = splitGrantTable(sql, online, chooser);
		//2. 对数据库授权
		if (permit == null) {
			permit = splitGrantSchema(sql, online, chooser);
		}
		//3. 对用户授权
		if (permit == null) {
			permit = splitGrantUser(sql, online, chooser);
		}
		// 出错
		if (permit == null) {
			throwable("cannot resolve '%s'", sql);
		}
		return permit;
	}

	/**
	 * 解析授权语句
	 * @param sql - 格式: GRANT [operator] ON schema.table TO username
	 * @return
	 */
	private Permit splitGrantTable(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(GrantParser.GRANT_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}
		
		// 数据库表
		Space space = new Space(matcher.group(2), matcher.group(3));
		// 操作权限
		String[] options = splitItems(matcher.group(1));
		// 用户账号名
		String[] users = splitItems(matcher.group(4));

		// 检查数据库表
		if (online && !chooser.onTable(space)) {
			throw new SQLSyntaxException("cannot accept %s", space);
		}
		// 检查用户
		for (int i = 0; i < users.length; i++) {
			if (online && !chooser.onUser(users[i])) {
				throw new SQLSyntaxException("cannot accept %s", users[i]);
			}
		}

		TablePermit permit = new TablePermit();
		permit.setUsers(users);
		int[] tags = splitSQLControl(options);
		
		Control control = new Control();
		boolean success = control.set(Permit.TABLE_PERMIT, tags);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		permit.add(space, control);
		return permit;
	}

	/**
	 * 解析数据库权限
	 * @param sql - GRANT [operator] ON SCHEMA schemaname TO username
	 * @return
	 */
	private Permit splitGrantSchema(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(GrantParser.GRANT_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}

		String[] options = splitItems(matcher.group(1));
		String[] schemas = splitItems(matcher.group(2));
		String[] users = splitItems(matcher.group(3));

		// 检查数据库
		for (int i = 0; i < schemas.length; i++) {
			if (online && !chooser.onSchema(schemas[i])) {
				throw new SQLSyntaxException("cannot accept %s", schemas[i]);
			}
		}
		// 检查用户
		for (int i = 0; i < users.length; i++) {
			if (online && !chooser.onUser(users[i])) {
				throw new SQLSyntaxException("cannot accept %s", users[i]);
			}
		}
		
		SchemaPermit permit = new SchemaPermit();
		permit.setUsers(users);
		int[] tags = splitSQLControl(options);

		Control control = new Control();
		boolean success = control.set(Permit.SCHEMA_PERMIT, tags);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		for (String s : schemas) {
			permit.add(s, control);
		}
		return permit;
	}

	/**
	 * 解析授权用户的配置
	 * @param sql - GRANT [options] TO username
	 * @param online
	 * @param chooser
	 * @return
	 */
	private Permit splitGrantUser(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(GrantParser.GRANT_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}

		String[] options = splitItems(matcher.group(1));
		String[] users = splitItems(matcher.group(2));

		// 在线检查注册用户
		for (int i = 0; i < users.length; i++) {
			if (online && !chooser.onUser(users[i])) {
				throw new SQLSyntaxException("cannot accept %s", users[i]);
			}
		}

		// 解析用户权限
		UserPermit permit = new UserPermit();
		permit.setUsers(users);
		int[] tags = splitSQLControl(options);
		
		Control control = new Control();
		boolean success = control.set(Permit.USER_PERMIT, tags);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		for (String s : users) {
			permit.add(s, control);
		}
		return permit;
	}

}
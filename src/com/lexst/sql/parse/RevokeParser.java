/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * revoke syntax
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/29/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.sql.account.*;
import com.lexst.sql.schema.*;

/**
 * 数据库的用户、数据库、数据库表回收权限表解析类。<br>
 * 权限表文本参数见Control类中定义。<br><br>
 * 
 * 例: REVOKE all ON DATABASE system_table FROM histes 
 */
public class RevokeParser extends PermitParser {

	/** 回收用户权限，格式: REVOKE 权限表 FROM 用户名表 **/
	private final static String REVOKE_USER = "^\\s*(?i)(?:REVOKE)\\s+(\\p{Print}+?)\\s+(?i)(?:FROM)\\s+(.+)\\s*$";

	/** 回收数据库权限，格式: REVOKE 权限表 ON SCHEMA|ON DATABASE 数据库名 FROM 用户名 **/
	private final static String REVOKE_SCHEMA = "^\\s*(?i)(?:REVOKE)\\s+(\\p{Print}+?)\\s+(?i)(?:ON\\s+SCHEMA|ON\\s+DATABASE)\\s+(\\w+)\\s+(?i)(?:FROM)\\s+(.+)\\s*$";
	
	/** 回收数据库表权限，格式: REVOKE 权限表  ON TABLE 表名  FROM 用户名 **/
	private final static String REVOKE_TABLE = "^\\s*(?i)(?:REVOKE)\\s+(\\p{Print}+?)\\s+(?i)(?:ON\\s+TABLE)\\s+(\\w+)\\.(\\w+)\\s+(?i)(?:FROM)\\s+(.+)\\s*$";

	/**
	 * default
	 */
	public RevokeParser() {
		super();
	}
	
	/**
	 * @param sql
	 * @param chooser
	 * @return
	 */
	public Permit split(String sql, boolean online, SQLChooser chooser) {
		//1. 回收数据库表操作权限
		Permit permit = splitRevokeTable(sql, online, chooser);
		//2. 回收数据库操作权限
		if (permit == null) {
			permit = splitRevokeSchema(sql, online, chooser);
		}
		//3. 回收用户权限
		if (permit == null) {
			permit = splitRevokeUser(sql, online, chooser);
		}
		// 出错
		if (permit == null) {
			throwable("cannot resolve '%s'", sql);
		}
		return permit;
	}

	/**
	 * 回收数据库表权限
	 * 
	 * @param sql - 格式: REVOKE [options] ON schema.table FROM user1, user2,...
	 * @param chooser
	 * @return
	 */
	private Permit splitRevokeTable(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(RevokeParser.REVOKE_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}
		
		String[] options = splitItems(matcher.group(1));
		Space space = new Space(matcher.group(2), matcher.group(3));
		String[] users = splitItems(matcher.group(4));

		// 检查数据库表
		if (online && !chooser.onTable(space) ) {
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
	 * 回收数据库权限
	 * 
	 * @param sql - 格式: REVOKE [options] ON SCHEMA schema1,schema2,... FROM user1,user2,...
	 * @param chooser
	 * @return
	 */
	private Permit splitRevokeSchema(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(RevokeParser.REVOKE_SCHEMA);
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
	 * 回收用户权限
	 * 
	 * @param sql - 格式: REVOKE [options] FROM username1,username2,... 
	 * @param chooser
	 * @return
	 */
	private Permit splitRevokeUser(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(RevokeParser.REVOKE_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}

		String[] options = splitItems(matcher.group(1));
		String[] users = splitItems(matcher.group(2));

		// 检查用户
		for (int i = 0; i < users.length; i++) {
			if (online && !chooser.onUser(users[i])) {
				throw new SQLSyntaxException("cannot accept %s", users[i]);
			}
		}

		// 回收授权选项
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
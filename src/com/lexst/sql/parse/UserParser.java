/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * "user" syntax parser
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

public class UserParser extends SQLParser {
	
	/** 
	 * 建立用户账号(包括用户名、密码、最大空间)<br>
	 * CREATE USER username password 'XXX' | CREATER USER username identified by 'XXX' | CREATE USER username password='XXX'  [MAXSIZE={digit}[M|G|T|P]] 
	 */
	private final static String CREATE_USER = "^\\s*(?i)(?:CREATE\\s+USER)\\s+(\\p{Graph}{6,})\\s+(?i)(?:IDENTIFIED\\s+BY\\s+|PASSWORD\\s*=\\s*|PASSWORD\\s+)\\'(\\p{Graph}{1,})\\'(\\s+.+|\\s*)$";
	private final static String MAXSIZE = "^\\s*(?i)MAXSIZE\\s*[=]\\s*([0-9]{1,})(?i)(M|G|T|P)\\s*(.*)$";

	/** 删除用户账号. DROP USER username */
	private final static String DROP_USER = "^\\s*(?i)(?:DROP\\s+USER)\\s+(\\p{Graph}{6,})\\s*$";

	/** 删除用户账号(输入SHA1串码,40个字符)，DROP SHA1 USER 2339000..... */
	private final static String DROP_SHA1USER = "^\\s*(?i)(?:DROP\\s+SHA1\\s+USER)\\s+([0-9a-fA-F]{40})\\s*$";

	/** 修改用户账号密码. <br>
	 * ALTER USER username IDENTIFIED BY 'XXX' | ALTER USER username PASSWORD 'XXX' | ALTER USER username PASSWORD='XXX' 
	 */
	private final static String ALTER_USER = "^\\s*(?i)(?:ALTER\\s+USER)\\s+(\\p{Graph}{6,})\\s+(?i)(?:IDENTIFIED\\s+BY\\s+|PASSWORD\\s*\\=\\s*|PASSWORD\\s+)\\'(\\p{Graph}{1,})\\'\\s*$";
	
	/**
	 * default
	 */
	public UserParser() {
		super();
	}

	/**
	 * 建立用户账号<br>
	 * 
	 * @param sql - 格式:CREATE USER username [PASSWORD 'xxx'|IDENTIFIED BY 'xxx'|PASSWORD='xx'] [MAXSIZE={digit}M|G|T|P]
	 * @param online - 在线检查
	 * @param chooser
	 * @return
	 */
	public User splitCreateUser(String sql, boolean online, SQLChooser chooser) {
		// 解析用户账号
		Pattern pattern = Pattern.compile(UserParser.CREATE_USER);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throwable("syntax error or missing!");
		}
		
		String username = matcher.group(1);
		String password = matcher.group(2);
		String suffix = matcher.group(3);
		User user = new User(username, password);
		
		// 检查用户账号是否存在
		if (online) {
			if (chooser.onUser(username)) {
				throwable("%s existed!", username);
			}
		}

		// 账号的空间尺寸
		if (suffix.trim().length() > 0) {
			pattern = Pattern.compile(UserParser.MAXSIZE);
			matcher = pattern.matcher(suffix);
			if (!matcher.matches()) {
				throwable(sql, 0);
			}
			String digit = matcher.group(1);
			String unit = matcher.group(2);
			long value = Long.parseLong(digit);
			if ("M".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * SQLParser.M);
			} else if ("G".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * SQLParser.G);
			} else if ("T".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * SQLParser.T);
			} else if ("P".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * SQLParser.P);
			}
		}
		return user;
	}

	/**
	 * 删除用户账号<br>
	 * @param sql - 格式: DROP USER username
	 * @param online - 在线检查
	 * @param chooser
	 * @return
	 */
	public User splitDropUser(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(UserParser.DROP_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}
		String username = matcher.group(1);

		// 账号不存在
		if (online) {
			if (!chooser.onUser(username)) {
				throwable("cannot find %s", username);
			}
		}

		return new User(username);
	}
	
	/**
	 * 删除用户账号 (SHA1，必须是40个16进制字符) <br>
	 * 
	 * @param sql - 格式: DROP SHA1 USER {digit}[40]
	 * @return
	 */
	public User splitDropSHA1User(String sql) {
		Pattern pattern = Pattern.compile(UserParser.DROP_SHA1USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}

		String hex = matcher.group(1);
		User user = new User();
		user.setHexUsername(hex);
		return user;
	}

	/**
	 * 修改用户账号密码<br>
	 * @param sql - 格式: ALTER USER username [IDENTIFIED BY 'xxx'|PASSWORD 'xxx'|PASSWORD='xxx']
	 * @param online - 在线检查
	 * @param chooser
	 * @return
	 */
	public User splitAlterUser(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(UserParser.ALTER_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable("syntax error or missing!");
		}

		String username = matcher.group(1);
		String password = matcher.group(2);
		
		// 如果账号不存在弹出异常
		if (online) {
			if (!chooser.onUser(username)) {
				throwable("cannot find %s", username);
			}
		}
		return new User(username, password);
	}
}
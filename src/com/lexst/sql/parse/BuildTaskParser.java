/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * build task (on build node)
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/28/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.parse.result.*;
import com.lexst.util.host.*;

/**
 * 解析BUILD任务命名
 *
 */
public class BuildTaskParser extends SQLParser {

	/** BUILD TASK 表达式. eg: BUILD TASK naming [TO address, address, ...] **/
	private final static String BUILD_TASK1 = "^\\s*(?i)(?:BUILD\\s+TASK)\\s+(\\w+)\\s*$";
	private final static String BUILD_TASK2 = "^\\s*(?i)(?:BUILD\\s+TASK)\\s+(\\w+?)\\s+(?i)(?:TO)\\s+(.+)$";
	
	/**
	 * default
	 */
	public BuildTaskParser() {
		super();
	}

	/**
	 * 解析 BUILD TASK参数
	 * @param sql
	 * @return
	 */
	public NamingHostResult split(String sql) {
		Pattern pattern = Pattern.compile(BuildTaskParser.BUILD_TASK2);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(BuildTaskParser.BUILD_TASK1);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("cannot resolve:%s", sql);
		}

		String naming = matcher.group(1);
		NamingHostResult host = new NamingHostResult(naming);
		String suffix = (matcher.groupCount() > 1 ? matcher.group(2) : null);
		if (suffix != null) {
			List<Address> list = splitIP(suffix);
			host.addAddresses(list);
			
//			if (list != null) {
//				host.addAllIP(list);
//			}
		}

		return host;
	}
}
/**
 * 
 */
package com.lexst.sql.parse;

import java.util.regex.*;


public class CollectParser extends SQLParser {

	// get collect path
	private final static String SET_COLLECT_PATH = "^\\s*(?i)SET\\s+(?i)COLLECT\\s+(?i)PATH\\s+(.+)\\s*$";

	// get collect task name
	private final static String TEST_COLLECT_TASK = "^\\s*(?i)TEST\\s+(?i)COLLECT\\s+(?i)TASK\\s+(.+)\\s*$";

	/**
	 * 
	 */
	public CollectParser() {
		super();
	}

	public String splitCollectPath(String sql) {
		Pattern pattern = Pattern.compile(CollectParser.SET_COLLECT_PATH);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}
		return matcher.group(1).trim();
	}

	
	public String splitCollectTask(String sql) {
		Pattern pattern = Pattern.compile(CollectParser.TEST_COLLECT_TASK);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}
		return matcher.group(1).trim();
	}
}

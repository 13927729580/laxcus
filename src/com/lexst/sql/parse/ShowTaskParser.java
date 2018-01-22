/**
 * 
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.parse.result.*;
import com.lexst.util.host.*;

public class ShowTaskParser extends SQLParser {

	// show task naming. eg: show task diffuse from site 12.2.33.9,12.33.0.8
	private final static String SHOW_TASK1 = "^\\s*(?i)SHOW\\s+(?i)TASK\\s+(?i)(ALL|DIFFUSE|AGGREGATE|COLLECT|BUILD)\\s*$";
	private final static String SHOW_TASK2 = "^\\s*(?i)SHOW\\s+(?i)TASK\\s+(?i)(ALL|DIFFUSE|AGGREGATE|COLLECT|BUILD)\\s+(?i)FROM\\s+(?i)SITE\\s+(.+)\\s*$";

	/**
	 * 
	 */
	public ShowTaskParser() {
		super();
	}

	public TaskHostResult splitShowTask(String sql) {
		Pattern pattern = Pattern.compile(ShowTaskParser.SHOW_TASK1);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String tag = matcher.group(1).trim();
			return new TaskHostResult(tag);
		}

		pattern = Pattern.compile(ShowTaskParser.SHOW_TASK2);
		matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}

		String tag = matcher.group(1);
		TaskHostResult host = new TaskHostResult(tag);
		
		String suffix = matcher.group(2);
		List<Address> list = splitIP(suffix);
		host.addAddresses(list);

//		List<String> ip_list = this.splitIP(suffix);
//		if (ip_list != null) {
//			host.addAllIP(ip_list);
//		}
		return host;
	}

}

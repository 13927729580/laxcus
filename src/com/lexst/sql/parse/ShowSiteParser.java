/**
 * 
 */
package com.lexst.sql.parse;

import java.util.regex.*;

import com.lexst.site.*;
import com.lexst.sql.parse.result.*;
import com.lexst.util.host.*;

/**
 * 在终端上打印节点地址
 *
 */
public class ShowSiteParser extends SQLParser {
	
	/** 显示全部或者某类节点地址(不包括TOP): SHOW SITE home [FROM 123.3.9.23,12.3.9.2,...] */
	private final static String SHOW_SITE1 = "^\\s*(?i)SHOW\\s+(?i)SITE\\s+(?i)(ALL|HOME|LOG|DATA|WORK|BUILD|CALL)\\s*$";
	private final static String SHOW_SITE2 = "^\\s*(?i)SHOW\\s+(?i)SITE\\s+(?i)(ALL|HOME|LOG|DATA|WORK|BUILD|CALL)\\s+(?i)FROM\\s+(.+)\\s*$";

	/**
	 * 
	 */
	public ShowSiteParser() {
		super();
	}

	/**
	 * 解析节点类型
	 * @param tag
	 * @return
	 */
	private int resolveSite(String tag) {
		if ("ALL".equalsIgnoreCase(tag)) {
			return 0;
		} else if ("HOME".equalsIgnoreCase(tag)) {
			return Site.HOME_SITE;
		} else if ("LOG".equalsIgnoreCase(tag)) {
			return Site.LOG_SITE;
		} else if ("DATA".equalsIgnoreCase(tag)) {
			return Site.DATA_SITE;
		} else if ("WORK".equalsIgnoreCase(tag)) {
			return Site.WORK_SITE;
		} else if ("BUILD".equalsIgnoreCase(tag)) {
			return Site.BUILD_SITE;
		} else if ("CALL".equalsIgnoreCase(tag)) {
			return Site.CALL_SITE;
		} else {
			throw new SQLSyntaxException("unknown family:%s", tag);
		}
	}

	/**
	 * split "show site [home|log|data|..] from [home_ip1, home_ip2, ]"
	 * return value: 1. site id; 2. home ip address (string)
	 * 
	 * @param sql
	 * @return
	 */
	public ShowSiteResult split(String sql) {
		Pattern pattern = Pattern.compile(ShowSiteParser.SHOW_SITE1);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String tag = matcher.group(1);
			int site = this.resolveSite(tag);
			return new ShowSiteResult(site);
		}

		pattern = Pattern.compile(ShowSiteParser.SHOW_SITE2);
		matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}
		String tag = matcher.group(1);
		String from = matcher.group(2);
		int site = this.resolveSite(tag);

		ShowSiteResult result = new ShowSiteResult(site);
		try {
			String[] items = from.split(",");
			for(String item : items) {
				Address address = new Address(item);
				result.addAddress(address);
			}
		} catch (java.net.UnknownHostException e) {
			throw new SQLSyntaxException(e);
		}
		
		return result;
	}

}
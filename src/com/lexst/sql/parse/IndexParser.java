/**
 * 
 */
package com.lexst.sql.parse;

import java.util.*;
import java.util.regex.*;

import com.lexst.sql.parse.result.*;
import com.lexst.sql.schema.*;
import com.lexst.util.host.*;

/**
 * 加载/卸载索引的解析类
 */
public class IndexParser extends SQLParser {

	/** 加载索引键: LOAD INDEX schema.table [TO ip_address, ...] **/
	private final static String LOAD_INDEX1 = "^\\s*(?i)(?:LOAD\\s+INDEX)\\s+(\\w+)\\.(\\w+)\\s*$";
	private final static String LOAD_INDEX2 = "^\\s*(?i)(?:LOAD\\s+INDEX)\\s+(\\w+)\\.(\\w+)\\s+(?i)TO\\s+(.+?)\\s*$";
	
	/** 卸载索引键 : STOP INDEX|UNLOAD INDEX schema.table [FROM ip_address,...] **/
	private final static String STOP_INDEX1 = "^\\s*(?i)(?:STOP\\s+INDEX|UNLOAD\\s+INDEX)\\s+(\\w+)\\.(\\w+)\\s*$";
	private final static String STOP_INDEX2 = "^\\s*(?i)(?:STOP\\s+INDEX|UNLOAD\\s+INDEX)\\s+(\\w+)\\.(\\w+)\\s+(?i)FROM\\s+(.+?)\\s*$";;

	/**
	 * default
	 */
	public IndexParser() {
		super();
	}

	/**
	 * 解析 "LOAD INDEX ..."
	 * @param sql
	 * @return
	 */
	public RebuildHostResult splitLoadIndex(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(IndexParser.LOAD_INDEX2);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(IndexParser.LOAD_INDEX1);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("syntax error or missing!");
		}
		
		Space space = new Space(matcher.group(1), matcher.group(2));
		if (online && !chooser.onTable(space)) {
			throw new SQLSyntaxException("cannot find %s", space);
		}

		RebuildHostResult host = new RebuildHostResult(space);

		if (matcher.groupCount() > 2) {
			String suffix = matcher.group(3);
			List<Address> list = splitIP(suffix);
			host.addAddresses(list);
		}
		return host;
	}
	
	/**
	 * 解析"STOP INDEX|UNLOAD INDEX ..."
	 * @param sql
	 * @return
	 */
	public RebuildHostResult splitStopIndex(String sql, boolean online, SQLChooser chooser) {
		Pattern pattern = Pattern.compile(IndexParser.STOP_INDEX2);
		Matcher matcher = pattern.matcher(sql);
		boolean match = matcher.matches();
		if (!match) {
			pattern = Pattern.compile(IndexParser.STOP_INDEX1);
			matcher = pattern.matcher(sql);
			match = matcher.matches();
		}
		if (!match) {
			throw new SQLSyntaxException("syntax error or missing!");
		}

		Space space = new Space(matcher.group(1), matcher.group(2));
		if (online && !chooser.onTable(space)) {
			throw new SQLSyntaxException("cannot find:%s", space);
		}

		RebuildHostResult host = new RebuildHostResult(space);

		if (matcher.groupCount() > 2) {
			String suffix = matcher.group(3);
			List<Address> list = splitIP(suffix);
			host.addAddresses(list);
		}
		
		return host;
	}

}
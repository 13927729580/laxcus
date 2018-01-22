/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.parse;

import java.util.*;

import com.lexst.sql.schema.*;

/**
 * @author scott.liang
 *
 */
public class JoinParser extends SQLParser {
	
	private final static String SQL_JOIN_ALL = "^\\s*(?i)SELECT\\s+(.+?)\\s+(?i)FROM\\s+(.+?)\\s+(?i)(INNER\\s+JOIN|LEFT\\s+JOIN|JOIN)\\s+(.+?)\\s+(?i)ON\\s+(.+?)\\s+(?i)(?:ORDER\\s+BY)\\s+(.+)$";

	/**
	 * 
	 */
	public JoinParser() {
		super();
	}

	public String split(Map<Space, Table> tables, String sql) {
		return null;
	}
}

/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.site.build;

import java.io.*;
import java.util.*;

import com.lexst.sql.schema.*;

/**
 * 新旧重构对应照表
 *
 */
public class BuildPair implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/** 所属数据库表 */
	private Space space;
	
	private Set<Long> history = new TreeSet<Long>();
	
	private Set<Long> refresh = new TreeSet<Long>();
	
	/**
	 * 
	 */
	public BuildPair() {
		super();
	}

}

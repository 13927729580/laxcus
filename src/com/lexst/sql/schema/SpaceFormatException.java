/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.schema;

import java.io.*;

/**
 * 解析数据库表名称时发生错误
 *
 */
public class SpaceFormatException extends IOException {

	private static final long serialVersionUID = -400918822478992624L;

	/**
	 * 
	 */
	public SpaceFormatException() {
		super();
	}

	/**
	 * @param arg0
	 */
	public SpaceFormatException(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}



}

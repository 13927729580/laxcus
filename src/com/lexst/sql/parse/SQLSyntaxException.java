/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com. All rights reserved
 * 
 * sql syntax error
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 5/27/2009
 * 
 * @see com.lexst.sql.parse
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.parse;


public class SQLSyntaxException extends RuntimeException {

	private static final long serialVersionUID = -7864174066831278185L;

	/**
	 *
	 */
	public SQLSyntaxException() {
		super();
	}

	/**
	 * @param message
	 */
	public SQLSyntaxException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public SQLSyntaxException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public SQLSyntaxException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SQLSyntaxException(String message, Throwable cause) {
		super(message, cause);
	}

}

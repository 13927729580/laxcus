/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com, All rights reserved
 * 
 * row parse error
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 11/18/2011
 * @see com.lexst.sql.row
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.row;


public class RowParseException extends RuntimeException {

	private static final long serialVersionUID = -4576692280461210243L;

	/**
	 *
	 */
	public RowParseException() {
		super();
	}

	/**
	 * @param message
	 */
	public RowParseException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public RowParseException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public RowParseException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public RowParseException(String message, Throwable cause) {
		super(message, cause);
	}

}

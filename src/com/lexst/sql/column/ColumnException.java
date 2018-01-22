/**
 * 
 */
package com.lexst.sql.column;


public class ColumnException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * default
	 */
	public ColumnException() {
		super();
	}

	/**
	 * @param arg0
	 */
	public ColumnException(String arg0) {
		super(arg0);
	}

	/**
	 * @param format
	 * @param args
	 */
	public ColumnException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param arg0
	 */
	public ColumnException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public ColumnException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
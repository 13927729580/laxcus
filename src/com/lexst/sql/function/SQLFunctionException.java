/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

/**
 * @author scott.liang
 *
 */
public class SQLFunctionException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 646509957515473734L;

	/**
	 * 
	 */
	public SQLFunctionException() {
		super();
	}

	/**
	 * @param arg0
	 */
	public SQLFunctionException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public SQLFunctionException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public SQLFunctionException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param format
	 * @param args
	 */
	public SQLFunctionException(String format, Object... args) {
		super(String.format(format, args));
	}

}
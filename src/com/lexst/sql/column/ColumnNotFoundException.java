
package com.lexst.sql.column;

public class ColumnNotFoundException extends RuntimeException {

	private static final long serialVersionUID = -6284887058127650215L;

	/**
	 *
	 */
	public ColumnNotFoundException() {
		super();
	}

	/**
	 * @param message
	 */
	public ColumnNotFoundException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public ColumnNotFoundException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public ColumnNotFoundException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ColumnNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

}

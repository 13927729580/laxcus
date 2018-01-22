
package com.lexst.sql.column.attribute;

public class ColumnAttributeException extends RuntimeException {

	private static final long serialVersionUID = 3733314010308508101L;

	/**
	 *
	 */
	public ColumnAttributeException() {
		super();
	}

	/**
	 * @param message
	 */
	public ColumnAttributeException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public ColumnAttributeException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public ColumnAttributeException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ColumnAttributeException(String message, Throwable cause) {
		super(message, cause);
	}

}

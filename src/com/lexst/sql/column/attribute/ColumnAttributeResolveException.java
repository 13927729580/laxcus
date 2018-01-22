
package com.lexst.sql.column.attribute;

public class ColumnAttributeResolveException extends RuntimeException {

	private static final long serialVersionUID = 3733314010308508101L;

	/**
	 *
	 */
	public ColumnAttributeResolveException() {
		super();
	}

	/**
	 * @param message
	 */
	public ColumnAttributeResolveException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public ColumnAttributeResolveException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public ColumnAttributeResolveException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ColumnAttributeResolveException(String message, Throwable cause) {
		super(message, cause);
	}

}

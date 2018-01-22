
package com.lexst.sql.charset;

public class CharsetException extends RuntimeException {

	private static final long serialVersionUID = -8918839013400232434L;

	/**
	 *
	 */
	public CharsetException() {
		super();
	}

	/**
	 * @param message
	 */
	public CharsetException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public CharsetException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public CharsetException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public CharsetException(String message, Throwable cause) {
		super(message, cause);
	}

}

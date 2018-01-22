
package com.lexst.util;

/**
 * 当数据类型不匹配或者未定义时，抛出此错误声明<br>
 * 
 */
public class InvalidTypeException extends RuntimeException {

	private static final long serialVersionUID = -4283852562541772852L;

	/**
	 *
	 */
	public InvalidTypeException() {
		super();
	}

	/**
	 * @param message
	 */
	public InvalidTypeException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public InvalidTypeException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public InvalidTypeException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InvalidTypeException(String message, Throwable cause) {
		super(message, cause);
	}

}
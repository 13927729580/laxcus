
package com.lexst.util;

/**
 * 检测数据长度(如数组、字符串、向量集合)超过或未达到指定范围时，抛出异常
 */
public class SizeOutOfBoundsException extends RuntimeException {

	private static final long serialVersionUID = 3322512177991249529L;

	/**
	 *
	 */
	public SizeOutOfBoundsException() {
		super();
	}

	/**
	 * @param message
	 */
	public SizeOutOfBoundsException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public SizeOutOfBoundsException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public SizeOutOfBoundsException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SizeOutOfBoundsException(String message, Throwable cause) {
		super(message, cause);
	}

}
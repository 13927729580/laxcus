/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm;

import java.io.*;

/**
 * 分步计算过程中某一阶段发生的异常<br>
 *
 */
public class TaskException extends IOException {

	private static final long serialVersionUID = 6938685063629949047L;

	/**
	 * default
	 */
	public TaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public TaskException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public TaskException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public TaskException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public TaskException(String message, Throwable cause) {
		super(message, cause);
	}

}

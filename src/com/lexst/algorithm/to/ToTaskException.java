/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.to;

import com.lexst.algorithm.*;

/**
 * AGGREGATE阶段发生的异常
 *
 */
public class ToTaskException extends TaskException {

	private static final long serialVersionUID = -420121000002040653L;

	/**
	 * default
	 */
	public ToTaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public ToTaskException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public ToTaskException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public ToTaskException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ToTaskException(String message, Throwable cause) {
		super(message, cause);
	}

}

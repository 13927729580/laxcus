/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.build;

import com.lexst.algorithm.*;

/**
 * build过程异常
 *
 */
public class BuildTaskException extends TaskException {

	private static final long serialVersionUID = -1743911363346260595L;

	/**
	 * default
	 */
	public BuildTaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public BuildTaskException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public BuildTaskException(String format, Object... args) {
		super(format, args);
	}

	/**
	 * @param cause
	 */
	public BuildTaskException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public BuildTaskException(String message, Throwable cause) {
		super(message, cause);
	}

}

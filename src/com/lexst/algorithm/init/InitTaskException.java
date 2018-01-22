/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.init;

import com.lexst.algorithm.*;

/**
 * 初始化操作时发生的异常。<br>
 *
 */
public class InitTaskException extends TaskException {

	private static final long serialVersionUID = 2500883046408482075L;

	/**
	 * default
	 */
	public InitTaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public InitTaskException(String message) {
		super(message);
	}

	/**
	 * @param format
	 * @param args
	 */
	public InitTaskException(String format, Object... args) {
		super(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public InitTaskException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InitTaskException(String message, Throwable cause) {
		super(message, cause);
	}

}

/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.from;

import com.lexst.algorithm.*;

/**
 * DIFFUSE阶段发生的异常
 *
 */
public class FromTaskException extends TaskException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6923311343421072950L;

	/**
	 * 
	 */
	public FromTaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public FromTaskException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param format
	 * @param args
	 */
	public FromTaskException(String format, Object... args) {
		super(format, args);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public FromTaskException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public FromTaskException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}

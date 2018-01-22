/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.collect;

import com.lexst.algorithm.*;

/**
 * COLLECT阶段发生的异常
 *
 */
public class CollectTaskException extends TaskException {

	private static final long serialVersionUID = -8356815290217897062L;

	/**
	 * 
	 */
	public CollectTaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public CollectTaskException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param format
	 * @param args
	 */
	public CollectTaskException(String format, Object... args) {
		super(format, args);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public CollectTaskException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public CollectTaskException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}

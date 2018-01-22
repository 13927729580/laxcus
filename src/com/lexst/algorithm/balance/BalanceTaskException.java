/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm.balance;

import com.lexst.algorithm.*;

/**
 * 平衡分布任务时发生的异常
 *
 */
public class BalanceTaskException extends TaskException {

	private static final long serialVersionUID = 7391428765968769248L;

	/**
	 * 
	 */
	public BalanceTaskException() {
		super();
	}

	/**
	 * @param message
	 */
	public BalanceTaskException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param format
	 * @param args
	 */
	public BalanceTaskException(String format, Object... args) {
		super(format, args);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public BalanceTaskException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public BalanceTaskException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}

 
package com.lexst.security;

import java.io.*;

public class SecureException extends IOException { 

	private static final long serialVersionUID = 7755345203788706746L;

	/**
	 *
	 */
	public SecureException() {
		super();
	}

	/**
	 * @param msg
	 */
	public SecureException(String msg) {
		super(msg);
	}
	
	/**
	 * @param format
	 * @param args
	 */
	public SecureException(String format, Object ... args) {
		this(String.format(format, args));
	}

	/**
	 * @param cause
	 */
	public SecureException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SecureException(String message, Throwable cause)  {
		super(message, cause);
	}
}
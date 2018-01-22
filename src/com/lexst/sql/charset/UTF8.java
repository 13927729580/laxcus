/**
 *
 */
package com.lexst.sql.charset;

import java.io.*;

/**
 * @author scott.liang
 *
 */
public final class UTF8 extends Charset {
	
	private static final long serialVersionUID = -2716233945038898416L;

	private final static String NAME = "UTF-8";

	/**
	 * UTF8 charset class
	 */
	public UTF8() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.charset.Charset#describe()
	 */
	@Override
	public String describe() {
		return UTF8.NAME;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.charset.Charset#decode(byte[], int, int)
	 */
	@Override
	public String decode(byte[] b, int off, int len) {
		try {
			return new String(b, off, len, UTF8.NAME);
		} catch (UnsupportedEncodingException e) {
			throw new CharsetException(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.charset.Charset#encode(java.lang.String)
	 */
	@Override
	public byte[] encode(String s) {
		try {
			return s.getBytes(UTF8.NAME);
		} catch (UnsupportedEncodingException e) {
			throw new CharsetException(e);
		}
	}

}
/**
 *
 */
package com.lexst.sql.charset;

import java.io.*;


public final class UTF32 extends Charset {

	private static final long serialVersionUID = -1889311916277846483L;
	
	private final static String NAME = "UTF-32BE";

	/**
	 * default constricator
	 */
	public UTF32() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.charset.Charset#describe()
	 */
	@Override
	public String describe() {
		return UTF32.NAME;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.charset.Charset#decode(byte[], int, int)
	 */
	@Override
	public String decode(byte[] b, int off, int len) {
		try {
			return new String(b, off, len, UTF32.NAME);
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
			return s.getBytes(UTF32.NAME);
		} catch (UnsupportedEncodingException e) {
			throw new CharsetException(e);
		}
	}

}
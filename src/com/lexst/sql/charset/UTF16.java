/**
 *
 */
package com.lexst.sql.charset;

import java.io.*;

/**
 * UTF16字符集，采用BIG-ENDIAN排列
 * 
 */
public final class UTF16 extends Charset {

	private static final long serialVersionUID = -1454722631065766077L;
	
	private final static String NAME = "UTF-16BE";

	/**
	 * default
	 */
	public UTF16() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.charset.Charset#describe()
	 */
	@Override
	public String describe() {
		return UTF16.NAME;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.charset.Charset#decode(byte[])
	 */
	@Override
	public String decode(byte[] b, int off, int len) {
		try {
			return new String(b, off, len, UTF16.NAME);
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
			return s.getBytes(UTF16.NAME);
		} catch (UnsupportedEncodingException e) {
			throw new CharsetException(e);
		}
	}

}
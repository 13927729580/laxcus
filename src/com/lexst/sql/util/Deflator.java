/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com, All rights reserved
 * 
 * compress data
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/12/2011
 * @see com.lexst.sql
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.util;

import java.io.*;
import java.util.zip.*;

/**
 * 多种算法的数据解压器。<br>
 * 包括GZIP、ZIP、deflate。<br>
 *
 */
public class Deflator {

	private static int fmsize(int len) {
		int left = len % 32;
		if (left != 0) len = len - left + 32;
		return len;
	}

	/**
	 * GZIP算法解压数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] gzip(byte[] b, int off, int len) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(Deflator.fmsize(len));
		byte[] data = new byte[1024];

		ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
		GZIPInputStream gzip = new GZIPInputStream(in);
		do {
			int size = gzip.read(data, 0, data.length);
			if (size < 1) break;
			buff.write(data, 0, size);
		} while (true);
		gzip.close();
		in.close();

		return buff.toByteArray();
	}
	
//	/**
//	 * GZIP算法解压数据
//	 * 
//	 * @param b
//	 * @return
//	 */
//	public static byte[] gzip(byte[] b) {
//		try {
//		return Deflator.gzip(b, 0, b.length);
//		} catch (IOException e) {
//			
//		}
//		return null;
//	}

	/**
	 * ZIP算法解压数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] zip(byte[] b, int off, int len) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(Deflator.fmsize(len));
		byte[] data = new byte[1024];

		ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
		ZipInputStream zip = new ZipInputStream(in);

		while (zip.getNextEntry() != null) {
			while (true) {
				int size = zip.read(data, 0, data.length);
				if (size <= 0) break;
				buff.write(data, 0, size);
			}
		}

		zip.close();
		in.close();
		return buff.toByteArray();
	}
	
//	/**
//	 * ZIP算法解压数据
//	 * 
//	 * @param b
//	 * @return
//	 */
//	public static byte[] zip(byte[] b) {
//		try {
//		return Deflator.zip(b, 0, b.length);
//} catch (IOException e) {
//			
//		}
//		return null;
//	}

	/**
	 * INFLATE 算法解压数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] deflate(byte[] b, int off, int len) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(Deflator.fmsize(len));
		byte[] data = new byte[1024];

		ByteArrayInputStream in = new ByteArrayInputStream(b, off, len);
		InflaterInputStream inflate = new InflaterInputStream(in);
		do {
			int size = inflate.read(data, 0, data.length);
			if (size <= 0) break;
			buff.write(data, 0, size);
		} while (true);
		inflate.close();
		in.close();

		return buff.toByteArray();
	}

//	/**
//	 * INFLATE 算法解压数据
//	 * 
//	 * @param b
//	 * @return
//	 */
//	public static byte[] deflate(byte[] b) {
//		try {
//			return Deflator.deflate(b, 0, b.length);
//		} catch (IOException e) {
//
//		}
//		return null;
//	}

}
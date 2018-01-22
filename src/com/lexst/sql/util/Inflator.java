/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com, All rights reserved
 * 
 * uncompress data
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
 * 多种算法的数据压缩器。<br>
 * 包括GZIP、ZIP、deflate。<br>
 * 
 */
public class Inflator {

	/**
	 * @param len
	 * @return
	 */
	private static int fmsize(int len) {
		int left = len % 32;
		if (left != 0) len = len - left + 32;
		return len;
	}

//	public static byte[] gzip2(byte[] b, int off, int len) {
//		try {
//			ByteArrayOutputStream buff = new ByteArrayOutputStream(
//					Inflator.fmsize(len));
//
//			GZIPOutputStream gzip = new GZIPOutputStream(buff);
//			gzip.write(b, off, len);
//			gzip.finish();
//			gzip.close();
//
//			return buff.toByteArray();
//		} catch (IOException e) {
//			Logger.error(e);
//		}
//		return null;
//	}

	/**
	 * GZIP算法压缩数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] gzip(byte[] b, int off, int len) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(Inflator.fmsize(len));
		GZIPOutputStream gzip = new GZIPOutputStream(buff);
		gzip.write(b, off, len);
		gzip.finish();
		gzip.close();

		return buff.toByteArray();
	}

//	/**
//	 * GZIP算法压缩数据
//	 * 
//	 * @param b
//	 * @return
//	 */
//	public static byte[] gzip(byte[] b) {
//		try {
//			return Inflator.gzip(b, 0, b.length);
//		} catch (IOException e) {
//
//		}
//		return null;
//	}

	/**
	 * ZIP算法压缩数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] zip(byte[] b, int off, int len) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(Inflator.fmsize(len));
		ZipOutputStream zip = new ZipOutputStream(buff);
		ZipEntry entry = new ZipEntry("default");
		zip.putNextEntry(entry);

		zip.write(b, off, len);
		zip.finish();
		zip.close();

		return buff.toByteArray();
	}

//	/**
//	 * ZIP算法压缩数据
//	 * 
//	 * @param b
//	 * @return
//	 */
//	public static byte[] zip(byte[] b) {
//		try {
//			return Inflator.zip(b, 0, b.length);
//		} catch (IOException e) {
//
//		}
//		return null;
//	}

	/**
	 * INFLATE压缩数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] inflate(byte[] b, int off, int len) throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(Inflator.fmsize(len));

		DeflaterOutputStream zip = new DeflaterOutputStream(buff);
		zip.write(b, off, len);
		zip.finish();
		zip.close();

		return buff.toByteArray();
	}

//	/**
//	 * INFLATE压缩数据
//	 * 
//	 * @param b
//	 * @return
//	 */
//	public static byte[] inflate(byte[] b) {
//		try {
//			return Inflator.inflate(b, 0, b.length);
//		} catch (IOException e) {
//
//		}
//		return null;
//	}
}
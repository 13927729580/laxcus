/**
 * 
 */
package com.lexst.debug;

import java.io.*;
import java.util.zip.*;

/**
 * @author siven
 *
 */
public class FileEncoder {

	/**
	 * 
	 */
	public FileEncoder() {
		// TODO Auto-generated constructor stub
	}
	
	byte[] b = new byte[] {};

	private void printFile(String filename) throws IOException {
		File file = new File(filename);
		if(!file.exists()) return ;
		
		byte[] b = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(file);
		in.read(b, 0, b.length);
		in.close();
		
		StringBuilder buff = new StringBuilder();
		for(int i = 0; i <b.length; i++) {
			if(buff.length()>0) buff.append(",");
			String s = String.format("(byte)0x%x", b[i] & 0xff);
			buff.append(s);
		}
		
		System.out.printf("size is:%d\n", b.length);
		
		System.out.printf("{%s}", buff.toString());
		
	}
	
	private void testGZIP(String filename) throws IOException {
		File file = new File(filename);
		if(!file.exists()) return ;
		byte[] b = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(file);
		in.read(b, 0, b.length);
		in.close();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(b, 0, b.length);
		gzip.finish();
		
		byte[] data = out.toByteArray();
		
		System.out.printf("\n\nsrc len:%d, gzip len:%d\n", b.length, data.length);
	}
	
	public static void main(String[] args) {
		FileEncoder fn = new FileEncoder();
		String filename = "F:\\gif\\allgif\\10-03\\02.png";
		filename = "F:\\gif\\allgif\\48-06\\emoticon8.png";
		filename = "F:\\gif\\allgif\\48-06\\emoticon4.png";
		try {
			fn.printFile(filename);
			fn.testGZIP(filename);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}
}

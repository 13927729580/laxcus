/**
 *
 */
package com.lexst.sql.charset;

import java.io.Serializable;

public abstract class Charset implements Serializable {

	private static final long serialVersionUID = 715431151880419297L;

	/**
	 * default
	 */
	public Charset() {
		super();
	}
	
	/**
	 * 字符集的名字，如UTF8、UTF16、UTF32
	 * @return
	 */
	public abstract String describe();

	/**
	 * 字符串编码成字节流
	 * @param s
	 * @return
	 */
	public abstract byte[] encode(String s);

	/**
	 * 字节流解码为字符串
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract String decode(byte[] b, int off, int len);

	/**
	 * 找到指定下标处的代码点。<br>
	 * 代码位是一个16位或者32位的整数。<br>
	 * 
	 * @param index
	 * @param text
	 * @return
	 */
	public int codePointAt(int index, String text) {
		int seek = 0;
		char[] chars = text.toCharArray();
		for(int scan = 0; seek < chars.length; scan++) {
			int code = java.lang.Character.codePointAt(chars, seek);
			// 扫描到指定的下标位置,返回
			if(scan == index) return code;
			// 判断是BMP(基本多语言平面)还是辅助平面.BMP是1个字符,辅助平面2字符
			seek += java.lang.Character.charCount(code); 
		}
		return -1; // 没找到
	}

	/**
	 * 返回指定下标位置的码位值 (输入数据是编码状态,在使用前需要解码生成字符串)
	 * 
	 * @param index
	 * @param origin
	 * @param off
	 * @param len
	 * @return
	 */
	public int codePointAt(int index, byte[] origin, int off, int len) {
		return codePointAt(index, decode(origin, off, len));
	}

	/**
	 * 以代码位为单元，统计实际的字符数。
	 * 新版UTF16中，存在两个字符表示一个字。旧版UCS-2，一个字符一个字
	 * @param str
	 * @return
	 */
	public int codePointCount(String str) {
		char[] chars = str.toCharArray();
		int count = 0;
		for(int i = 0; i < chars.length; count++) {
			int code = java.lang.Character.codePointAt(chars, i);
			i += java.lang.Character.charCount(code);
		}
		return count;
	}

	/**
	 * 新版JAVA字符串采用UTF16编码，UTF16存在占用两个字符(一个字符双字节)的可能(区别与UCS-2)。
	 * 这个函数就是以代码位为标准(一个字符或者两个字符)，得到真实的字符串
	 * 
	 * @param start
	 * @param size
	 * @param str
	 * @return
	 */
	public String subCodePoints(int start, int size, String str) {
		char[] chars = str.toCharArray();
		int begin = -1, end = -1;
		for (int scan = 0, seek = 0; seek < chars.length; scan++) {
			if (scan == start) {
				begin = seek;
				if (size == -1) { // 如果未指定长度,移到最后位置
					end = chars.length;
					break;
				}
			} else if (scan > start && scan - start == size) {
				end = seek;
				break;
			}

			// 计算代码位是占一个字符还是两个字符
			int code = java.lang.Character.codePointAt(chars, seek);
			seek += java.lang.Character.charCount(code); // 移到下一个坐标点
		}

		// 错误
		if (start < 0) {
			throw new IndexOutOfBoundsException("start offset:" + start);
		} else if (begin == -1) {
			throw new IndexOutOfBoundsException("string length:" + chars.length + ", split offset:" + start);
		}

		// 如果最后结束未达到指定点，以实际位置为准
		if (end == -1) end = chars.length;
		
		// 返回截取结果
		return new String(chars, begin, end - begin);
	}
	
	/**
	 * 同上
	 * @param start
	 * @param str
	 * @return
	 */
	public String sugCodePoints(int start, String str) {
		return subCodePoints(start, -1, str);
	}

	/**
	 * 同上
	 * @param start
	 * @param size
	 * @param origin
	 * @param off
	 * @param len
	 * @return
	 */
	public String subCodePoints(int start, int size, byte[] origin, int off, int len) {
		return subCodePoints(start, size, decode(origin, off, len));
	}
	
	/**
	 * 同上
	 * @param start
	 * @param origin
	 * @param off
	 * @param len
	 * @return
	 */
	public String subCodePoints(int start, byte[] origin, int off, int len) {
		return subCodePoints(start, -1, decode(origin, off, len));
	}
	
}
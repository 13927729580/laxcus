/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.index.section;

import java.io.*;
import java.util.regex.*;

import com.lexst.log.client.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.util.*;
import com.lexst.util.range.*;

/**
 * @author scott.liang
 * 
 */
abstract class WordSector extends Bit32Sector {
	
	private static final long serialVersionUID = 7415310567965974546L;

	private final static String STYLE = "^\\s*(?i)(?:PACKING\\:)(.+);(?i)(?:CHARSET\\:)(.+);(?i)(?:RANGES\\:)(.+)\\s*$";

	/** 列的字符集实例  **/
	protected Charset charset;
	
	/** 列的压缩和加密参数，见com.lexst.sql.column.attribute.Packing **/
	protected Packing packing = new Packing();

	/**
	 * default
	 */
	protected WordSector(Charset charset) {
		super();
		this.setCharset(charset);
	}

	/**
	 * 打包参数
	 * @param i
	 */
	public void setPacking(Packing i) {
		this.packing = new Packing(i);
	}
	
	/**
	 * 打包参数
	 * @param compress
	 * @param encrypt
	 * @param password
	 */
	public void setPacking(int compress, int encrypt, byte[] password) {
		this.packing.setPacking(compress, encrypt, password);
	}
	
	/**
	 * 打包参数
	 * @return
	 */
	public Packing getPacking() {
		return this.packing;
	}

	/**
	 * 所在列的字符集，由子类定义
	 * 
	 * @param charset
	 */
	protected void setCharset(Charset charset) {
		this.charset = charset;
	}

	/**
	 * 列字符集
	 * @return
	 */
	public Charset getCharset() {
		return this.charset;
	}

	/**
	 * 从字节流中取出首字符代码位，根据代码位，分配文字对应的存放位置. 
	 * 输入的数据是原始字节流(有打包和编码）。
	 * 执行顺序是:
	 * <1> 解包(解密，解压)
	 * <2> 解码，生成String
	 * <3> 从String中取出首字符代码位(兼容UTF-16，UTC-2，包括基本多语言平面BMP和辅助平面)
	 * <4> 根据首字代码位，取得对应下标位置
	 * 
	 * @param value
	 * @return
	 */
	protected int indexOf(byte[] value) {
		// 没有范围定义是错误
		if(ranges.isEmpty()) return -1;
		// 如果变长类型是"EMPTY"状态，返回0下标
		if (value == null || value.length == 0) return 0;

		// 解包
		if(packing.isEnabled()) {
			try {
				value = VariableGenerator.depacking(packing, value, 0, value.length);
			} catch (IOException e) {
				Logger.error(e);
				return 0;
			}
		}
		// 取首字符代码位
		int codePoint = charset.codePointAt(0, value, 0, value.length);
		// 找到代码位对应的分区,返回分区下标
		int index = 0;
		for (IntegerRange range : ranges) {
			if (range.inside(codePoint)) return index;
			index++;
		}
		// 如果没有找到分区,默认为最后
		return index - 1;
	}

	/**
	 * 解析数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected int split(Charset charset, String value) {
		Pattern pattern = Pattern.compile(WordSector.STYLE);
		Matcher matcher = pattern.matcher(value);
		if(!matcher.matches()) {
			throw new IllegalArgumentException("invalid sector! " + value);	
		}
		
		String s1 = matcher.group(1);
		String s2 = matcher.group(2);
		String s3 = matcher.group(3);
		
		if(!charset.describe().equals(s2)) {
			throw new IllegalArgumentException("invalid charset: " + s2);
		}
		
		// 字符编码
		this.charset = charset;
		// 打包参数
		byte[] decodes = com.lexst.util.Base64.decode(s1.getBytes());
		packing.resolve(decodes, 0, decodes.length);
		// 代码位分片范围
		int size = ranges.size();
		String[] elements = s3.split(";");
		for (int i = 1; i < elements.length; i++) {
			pattern = Pattern.compile(WordSector.SYNTAX);
			matcher = pattern.matcher(elements[i]);
			if (!matcher.matches()) {
				throw new IllegalArgumentException("invalid value:" + elements[i]);
			}
			
			int begin = super.toInteger(matcher.group(1));
			int end = super.toInteger(matcher.group(2));
			ranges.add(new IntegerRange(begin, end));
		}

		return ranges.size() - size;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.index.section.Bit32Sector#assemble()
	 */
	@Override
	protected String assemble() {
		byte[] b = packing.build();
		String s = new String(com.lexst.util.Base64.encode(b));

		StringBuilder buff = new StringBuilder();
		for (IntegerRange range : ranges) {
			if (buff.length() > 0) buff.append('|');
			buff.append(String.format("%X,%X", range.getBegin(), range.getEnd()));
		}

		return String.format("PACKING:%s;CHARSET:%s;RANGES:%s", s, charset.describe(), buff.toString());
	}
}
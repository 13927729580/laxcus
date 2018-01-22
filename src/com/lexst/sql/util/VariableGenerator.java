/**
 * 
 */
package com.lexst.sql.util;

import java.io.*;
import java.util.*;

import com.lexst.security.*;
import com.lexst.sql.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;

/**
 * 可变长数据类型的数据生成器
 * 
 *
 */
public class VariableGenerator {

	/**
	 * default
	 */
	public VariableGenerator() {
		super();
	}
	
	/**
	 * 压缩数据
	 * 
	 * @param attribute
	 * @param b
	 * @return
	 */
	public static byte[] compress(int compress, byte[] b, int off, int len) throws IOException {
		switch (compress) {
		case Packing.GZIP:
			return Inflator.gzip(b, off, len);
		case Packing.ZIP:
			return Inflator.zip(b, off, len);
		}
		return Arrays.copyOfRange(b, off, off + len);
	}
	
	/**
	 * 解压缩数据
	 * 
	 * @param attribute
	 * @param b
	 * @return
	 */
	public static byte[] uncompress(int compress, byte[] b, int off, int len) throws IOException {
		switch (compress) {
		case Packing.GZIP:
			return Deflator.gzip(b, off, len);
		case Packing.ZIP:
			return Deflator.zip(b, off, len);
		}
		return Arrays.copyOfRange(b, off, off + len);
	}

	/**
	 * 加密数据
	 * 
	 * @param encrypt
	 * @param pwd
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] encrypt(int encrypt, byte[] pwd, byte[] b, int off, int len) throws SecureException {
		switch (encrypt) {
		case Packing.DES:
			return SecureEncryptor.des(pwd, b, off, len);
		case Packing.DES3:
			return SecureEncryptor.des3(pwd, b, off, len);
		case Packing.AES:
			return SecureEncryptor.aes(pwd, b, off, len);
		case Packing.BLOWFISH:
			return SecureEncryptor.blowfish(pwd, b, off, len);
		}
		return Arrays.copyOfRange(b, off, off + len);
	}
	
	/**
	 * 解密数据
	 * 
	 * @param encrypt
	 * @param password
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] decrypt(int encrypt, byte[] password, byte[] b, int off, int len) throws SecureException {
		switch (encrypt) {
		case Packing.DES:
			return SecureDecryptor.des(password, b, off, len);
		case Packing.DES3:
			return SecureDecryptor.des3(password, b, off, len);
		case Packing.AES:
			return SecureDecryptor.aes(password, b, off, len);
		case Packing.BLOWFISH:
			return SecureDecryptor.blowfish(password, b, off, len);
		}
		return Arrays.copyOfRange(b, off, off + len);
	}

	/**
	 * 压缩和加密。先压缩后加密
	 * 
	 * @param packing
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] enpacking(Packing packing, byte[] b, int off, int len) throws IOException {
		int compress = packing.getCompress();
		int encrypt = packing.getEncrypt();
		if(compress != 0) {
			byte[] s = VariableGenerator.compress(compress, b, off, len);
			if (encrypt != 0) {
				return VariableGenerator.encrypt(encrypt, packing.getEncryptPassword(), s, 0, s.length);
			} else {
				return s;
			}
		} else if(encrypt != 0) {
			return VariableGenerator.encrypt(encrypt, packing.getEncryptPassword(), b, off, len);
		}
		return Arrays.copyOfRange(b, off, off + len);
	}
	
	/**
	 * 同上
	 * @param attribute
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] enpacking(VariableAttribute attribute, byte[] b, int off, int len) throws IOException {
		return VariableGenerator.enpacking(attribute.getPacking(), b, off, len);
	}

	/**
	 * 解密和解压缩。解密，然后解压缩
	 * 
	 * @param packing
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] depacking(Packing packing, byte[] b, int off, int len) throws IOException {
		int compress = packing.getCompress();
		int encrypt = packing.getEncrypt();
		if (encrypt != 0) {
			byte[] s = VariableGenerator.decrypt(encrypt, packing.getEncryptPassword(), b, off, len);
			if (compress != 0) {
				return VariableGenerator.uncompress(compress, s, 0, s.length);
			} else {
				return s;
			}
		} else if (compress != 0) {
			return VariableGenerator.uncompress(compress, b, off, len);
		}
		return Arrays.copyOfRange(b, off, off + len);
	}
	
	/**
	 * 同上
	 * 
	 * @param attribute
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] depacking(VariableAttribute attribute, byte[] b, int off, int len) throws IOException {
		return VariableGenerator.depacking(attribute.getPacking(), b, off, len);
	}
	
	/**
	 * 转换为字符串
	 * 
	 * @param attribute
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static String toString(WordAttribute attribute, byte[] b, int off, int len) throws IOException {
		Charset charset = VariableGenerator.getCharset(attribute);
		if (attribute.getPacking().isEnabled()) {
			byte[] result = VariableGenerator.depacking(attribute, b, off, len);
			return charset.decode(result, 0, result.length);
		} else {
			return charset.decode(b, off, len);
		}
	}
	
//	/**
//	 * 切割字符串，产生VWord子类，保存到集合
//	 * 
//	 * @param charset
//	 * @param attribute
//	 * @param index
//	 * @param array
//	 */
//	private static void splitVWord(Charset charset, WordAttribute attribute, String index, List<VWord> array) {
//		// 以代码位为单位，统计实际字符数
//		int len = charset.codePointCount(index);
//		for(int begin = 0; begin < len; begin++) {
//			for(int end = len; begin < end; end--) {
//				String sub = charset.subCodePoints(begin, end - begin, index);
//				short left = (short) begin;
//				short right = (short)(end - begin);
//
//				byte[] origin = charset.encode(sub);
//				byte[] value = VariableGenerator.enpacking(attribute, origin, 0, origin.length);
//				
//				// LIKE id 区别与 column id
//				short likeId = (short)(attribute.getColumnId() | 0x8000);
//				
//				switch (attribute.getType()) {
//				case Type.CHAR:
//					array.add(new VChar(likeId, left, right, value)); break;
//				case Type.SCHAR:
//					array.add(new VSChar(likeId, left, right, value)); break;
//				case Type.WCHAR:
//					array.add(new VWChar(likeId, left, right, value)); break;
//				default:
//					throw new IllegalArgumentException("invalid column!");
//				}			
//			}
//		}
//	}
	
	/**
	 * 根据文本内容，生成数值、索引、LIKE索引数组
	 * 
	 * @param attribute
	 * @param text
	 * @param word
	 */
	private static void split(boolean dsm, WordAttribute attribute, String text, Word word) throws IOException {
		word.setId(attribute.getColumnId());
		
		//1. 如果是字符串不包含数据，设为"EMPTY"状态，用于"IS EMPTY, NOT EMPTY"检索
		if(text.isEmpty()) {
			word.setValue(new byte[0]);
			word.setIndex(new byte[0]);
			return;
		}

		// 生成编码和打包后的数据值
		byte[] b = VariableGenerator.toValue(attribute, text);
		word.setValue(b);
		
		// 如果是索引键(主键或者从键)，生成索引值，在索引基础上，生成模糊检索
		if (attribute.isKey()) {
			// 生成索引，可能是NULL
			b = VariableGenerator.toIndex(dsm, attribute, text);
			word.setIndex(b);
			// 在索引基础上，分割字符串，保存“LIKE”查询的字符串(默认最大限制16字符)
			List<VWord> array = VariableGenerator.toVWord(attribute, text);
			if (array != null) word.addVWords(array);
		}
	}

	/**
	 * 根据列属性生成Char对象
	 * @param dsm
	 * @param attribute
	 * @param text
	 * @return
	 */
	public static Char toChar(boolean dsm, CharAttribute attribute, String text) throws IOException {
		Char utf8 = new Char();
		VariableGenerator.split(dsm, attribute, text, utf8);
		return utf8;
	}
	
	/**
	 * 根据列属性生成SChar对象
	 * @param dsm
	 * @param attribute
	 * @param text
	 * @return
	 */
	public static SChar toSChar(boolean dsm, SCharAttribute attribute, String text) throws IOException {
		SChar schar = new SChar();
		VariableGenerator.split(dsm, attribute, text, schar);
		return schar;
	}

	/**
	 * 生成WChar对象
	 * @param dsm
	 * @param attribute
	 * @param text
	 * @return
	 */
	public static WChar toWChar(boolean dsm, WCharAttribute attribute, String text) throws IOException {
		WChar wchar = new WChar();
		VariableGenerator.split(dsm, attribute, text, wchar);
		return wchar;
	}
	
	/**
	 * 生成Raw对象
	 * @param dsm
	 * @param attribute
	 * @param data
	 * @return
	 */
	public static Raw toRaw(boolean dsm, RawAttribute attribute, byte[] data) throws IOException {
		Raw raw = new Raw(attribute.getColumnId());
		
		// 设置"EMPTY"状态, 用于"IS EMPTY, IS NOT EMPTY"检索
		if (data != null && data.length == 0) {
			raw.setValue(new byte[0]);
			raw.setIndex(new byte[0]);
			return raw;
		}
		
		// 生成打包后的数组
		byte[] b = VariableGenerator.toValue(attribute, data, 0, data.length);
		raw.setValue(b);
		
		// 如果是索引键，截取一段数据，生成索引
		if(attribute.isKey()) {
			b = VariableGenerator.toIndex(dsm, attribute, data);
			raw.setIndex(b);
		}

		return raw;
	}
	
	/**
	 * 根据文字类型,返回对应的字符集
	 * 
	 * @param type
	 * @return
	 */
	public static Charset getCharset(byte type) {
		switch (type) {
		case Type.CHAR:
			return new com.lexst.sql.charset.UTF8();
		case Type.SCHAR:
			return new com.lexst.sql.charset.UTF16();
		case Type.WCHAR:
			return new com.lexst.sql.charset.UTF32();
		}
		return null;
	}

	/**
	 * 同上
	 * 
	 * @param attribute
	 * @return
	 */
	public static Charset getCharset(ColumnAttribute attribute) {
		return VariableGenerator.getCharset(attribute.getType());
	}

	/**
	 * change to like word
	 * 
	 * @param attribute
	 * @param left
	 * @param right
	 * @param text
	 * @param word
	 */
	private static void split(WordAttribute attribute, short left, short right, String text, VWord word) throws IOException {
		if(!attribute.isSentient()) {
			text = text.toLowerCase(); 
		}
	
		// 选择一个字符集
		Charset charset = VariableGenerator.getCharset(attribute);
		
		byte[] origin = charset.encode(text);
		byte[] index = VariableGenerator.enpacking(attribute, origin, 0, origin.length);
		
		short columnId = attribute.getColumnId();
		short likeId = (short)(columnId | 0x8000);
		
		word.setId(likeId);
		word.setRange(left, right);
		word.setIndex(index);
	}
	
	/**
	 * change to VChar
	 * @param attribute
	 * @param left
	 * @param right
	 * @param text
	 * @return
	 */
	public static VChar toVChar(CharAttribute attribute, short left, short right, String text) throws IOException {
		VChar word = new VChar();
		VariableGenerator.split(attribute, left, right, text, word);
		return word;
	}
	
	/**
	 * change to VSChar
	 * @param attribute
	 * @param left
	 * @param right
	 * @param text
	 * @return
	 */
	public static VSChar toVSChar(SCharAttribute attribute, short left, short right, String text) throws IOException {
		VSChar word = new VSChar();
		VariableGenerator.split(attribute, left, right, text, word);
		return word;
	}

	/**
	 * change to VWChar
	 * @param attribute
	 * @param left
	 * @param right
	 * @param text
	 * @return
	 */
	public static VWChar toVWChar(WCharAttribute attribute, short left, short right, String text) throws IOException {
		VWChar word = new VWChar();
		VariableGenerator.split(attribute, left, right, text, word);
		return word;
	}

	/**
	 * 针对字符类型，根据字符值，返回经过编码和打包的值
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public static byte[] toValue(WordAttribute attribute, String value) throws IOException {
		Charset charset = VariableGenerator.getCharset(attribute);

		byte[] result = charset.encode(value);
		result = VariableGenerator.enpacking(attribute, result, 0, result.length);
		return result;
	}

	/**
	 * 针对字符类型，根据字符值，返回经过截取，大小写忽略转换，编码，打包的索引
	 * 
	 * @param dsm
	 * @param attribute
	 * @param value
	 * @return
	 */
	public static byte[] toIndex(boolean dsm, WordAttribute attribute, String value) throws IOException {
		Charset charset = VariableGenerator.getCharset(attribute);

		// 截取字符值的前面一段做为索引
		String index = charset.subCodePoints(0, attribute.getIndexSize(), value);

		// 大小写不敏感(NOT CASE)，转为小写
		if (!attribute.isSentient()) index = index.toLowerCase();

		// 如果是列存储模式，并且索引和数值一样，不需要保留索引
		if (dsm && index.equals(value)) {
			return null;
		}

		// 编码
		byte[] encodes = charset.encode(index);
		// 打包(压缩、加密)
		return VariableGenerator.enpacking(attribute, encodes, 0, encodes.length);
	}
	
	/**
	 * 生成模糊检索的VWord集合
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public static List<VWord> toVWord(WordAttribute attribute, String value) throws IOException {
		// 如果不支持模糊检索，不需要以下处理
		if(!attribute.isLike()) return null;
		
		Charset charset = VariableGenerator.getCharset(attribute);

		// 截取字符值的前面一段做为索引
		String index = charset.subCodePoints(0, attribute.getIndexSize(), value);

		// 大小写不敏感(NOT CASE)，转为小写
		if (!attribute.isSentient()) index = index.toLowerCase();

		// 分割字符串
		List<VWord> array = new ArrayList<VWord>();
		// 以代码位为单位，统计实际字符数
		int codePints = charset.codePointCount(index);
		for (int begin = 0; begin < codePints; begin++) {
			for (int end = codePints; begin < end; end--) {
				String sub = charset.subCodePoints(begin, end - begin, index);
				short left = (short) begin;
				short right = (short)(end - begin);

				byte[] encodes = charset.encode(sub);
				byte[] b = VariableGenerator.enpacking(attribute, encodes, 0, encodes.length);

				//like id区别与 column id
				short likeId = (short)(attribute.getColumnId() | 0x8000);
				
				if (attribute.isChar()) {
					array.add(new VChar(likeId, left, right, b));
				} else if (attribute.isSChar()) {
					array.add(new VSChar(likeId, left, right, b));
				} else if (attribute.isWChar()) {
					array.add(new VWChar(likeId, left, right, b));
				}
			}
		}

		return array;
	}
	
	/**
	 * 针对字节数组类型(RAW)，生成经过打包(压缩+加密)的数值
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public static byte[] toValue(VariableAttribute attribute, byte[] value, int off, int len) throws IOException {
		return VariableGenerator.enpacking(attribute, value, off, len);
	}

	/**
	 * 针对可变长非字符数组类型(RAW)，截取数值数组的一段前缀，经过打包(压缩+加密)，生成索引
	 * 
	 * @param dsm
	 * @param attribute
	 * @param value
	 * @return
	 */
	public static byte[] toIndex(boolean dsm, VariableAttribute attribute, byte[] value) throws IOException {
		int size = (attribute.getIndexSize() < value.length ? attribute.getIndexSize() : value.length);
		byte[] index = Arrays.copyOfRange(value, 0, size);
		// 如果是列存储模式，并且索引和数值完全一致，不生成索引
		if (dsm && Arrays.equals(value, index)) {
			return null;
		}
		return VariableGenerator.enpacking(attribute, index, 0, index.length);
	}

}
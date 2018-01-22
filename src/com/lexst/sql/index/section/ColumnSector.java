/**
 * 
 */
package com.lexst.sql.index.section;

import java.io.*;
import java.util.regex.*;

import com.lexst.sql.charset.*;
import com.lexst.sql.column.Column;
import com.lexst.util.*;

/**
 * 列分割器
 * 
 */
public abstract class ColumnSector implements Serializable {
	
	private static final long serialVersionUID = 937141995677481384L;

	/**
	 * 列分割器名称标记
	 * 
	 * @return
	 */
	public abstract String getTag();

	/**
	 * 计算"列(column)"在分区集合的下标位置
	 * 
	 * @param column
	 * @return
	 */
	public abstract int indexOf(Column column);

	/**
	 * 生成描述分片的数据流
	 * @return
	 */
	protected abstract String assemble();
	
	/**
	 * 解析数据流，返回解析的成员数
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected abstract int split(String s);
	

	/**
	 * 生成数据流
	 * @return
	 */
	public byte[] build() {
		String s = String.format("%s='%s'", getTag(), assemble());
		
		byte[] data = new com.lexst.sql.charset.UTF8().encode(s);
		byte[] b = Numeric.toBytes(data.length);
		byte[] buff = new byte[4 + data.length];
		System.arraycopy(b, 0, buff, 0, b.length);
		System.arraycopy(data, 0, buff, 4, data.length);
		return buff;
	}
	
	/**
	 * 解析数据流，返回解析的字节流尺寸
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		if (seek + 4 > end) {
			throw new SizeOutOfBoundsException("sector sizeout!");
		}
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (seek + size > end) {
			throw new SizeOutOfBoundsException("sector data sizeout!");
		}
		// 解码
		String s = new UTF8().decode(b, seek, size);
		seek += size;
		
		// 分析标记是否匹配
		String regex = "^\\s*(\\w+)=\\'(.+?)\\'\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(s);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("invalid short sector! " + s);
		}
		String tag = matcher.group(1);
		String suffix = matcher.group(2);

		// 比较，如果不匹配，返回-1
		if (!tag.equals(getTag())) {
			return -1;
		}
		// 解析数据
		split(suffix);
		// 解析字节长度
		return seek - off;
	}
	
	/**
	 * 比较TAG名称是否一致
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public boolean match(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		if (seek + 4 > end) {
			return false;
		}
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (seek + size > end) {
			return false;
		}
		// 解码
		String s = new UTF8().decode(b, seek, size);
		seek += size;

		// 分析标记是否匹配
		String regex = "^\\s*(\\w+)=\\'(.+?)\\'\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(s);
		if (!matcher.matches()) {
			return false;
		}
		String tag = matcher.group(1);
		return tag.equals(getTag());
	}
	
}
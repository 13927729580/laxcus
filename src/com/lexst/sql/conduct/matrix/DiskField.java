/**
 * 
 */
package com.lexst.sql.conduct.matrix;

import java.io.*;

import com.lexst.util.*;

/**
 * 数据检索/定制化生成结果后，生成的数据区域图谱的子图谱。<br>
 * 每个子图谱由一个模值(mod)表示它在数据区域中的唯一性。<br><br>
 * 
 * 检索/定制化结果以一个文件的形式存在由DATA节点上，在读取或者超时后由DATA节点删除。<br><br>
 * 
 * 注：<br>
 * 模值(mod)的隐性含义: 相邻的模值，它们对应的实际数据也是相邻的。这一点是分片的基本规则。
 */
public class DiskField implements Serializable, Cloneable, Comparable<DiskField> {

	private static final long serialVersionUID = -3488600975075175146L;

	/** 模值(在一组DiskArea中唯一。在DATA/WORK节点数产生，保证相同模值的对应数据位于一个区域内 ) **/
	private int mod;

	/** 本片区域的数据成员数 ，如SELECT检索后的记录数 **/
	private int items;

	/** data节点上文件数据范围，下标从0开始 **/
	private long begin, end;

	/**
	 * default
	 */
	public DiskField() {
		super();
		this.mod = 0;
		this.items = 0;
		this.begin = this.end = 0;
	}

	/**
	 * 复制Field
	 * 
	 * @param object
	 */
	public DiskField(DiskField object) {
		this();
		this.mod = object.mod;
		this.items = object.items;
		this.begin = object.begin;
		this.end = object.end;
	}

	/**
	 * @param mod
	 * @param begin
	 * @param end
	 */
	public DiskField(int mod, long begin, long end) {
		this();
		this.setMod(mod);
		this.setRange(begin, end);
	}

	/**
	 * 设置模值
	 * 
	 * @param i
	 */
	public void setMod(int i) {
		this.mod = i;
	}

	/**
	 * 返回模值
	 * 
	 * @return
	 */
	public int getMod() {
		return this.mod;
	}

	/**
	 * 数据成员数(SELECT检索记录数或者其它数值)
	 * 
	 * @param i
	 */
	public void setItems(int i) {
		this.items = i;
	}

	/**
	 * 返回数据中的成员数
	 * 
	 * @return
	 */
	public int getItems() {
		return this.items;
	}

	/**
	 * 设置范围
	 * 
	 * @param b
	 * @param e
	 */
	public void setRange(long b, long e) {
		if (b > e) {
			throw new IllegalArgumentException("invalid range: " + b + "-" + e);
		}
		this.begin = b;
		this.end = e;
	}

	/**
	 * 返回磁盘开始下标(下标从0开始)
	 * @return
	 */
	public long getBegin() {
		return this.begin;
	}

	/**
	 * 返回磁盘结束下标
	 * @return
	 */
	public long getEnd() {
		return this.end;
	}

	/**
	 * 磁盘文件中数据块的总长度
	 * @return
	 */
	public long length() {
		return this.end - this.begin + 1;
	}

	/**
	 * 生成数据流，返回数据流长度(固定24字节)
	 * 
	 * @param buff
	 * @return
	 */
	public int build(ByteArrayOutputStream buff) {
		byte[] b = Numeric.toBytes(mod);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(items);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(begin);
		buff.write(b, 0, b.length);
		b = Numeric.toBytes(end);
		buff.write(b, 0, b.length);
		// 输入24个字节
		return 24;
	}

	/**
	 * 生成数据流，返回数据流字节数组
	 * 
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(24);
		build(buff);
		return buff.toByteArray();
	}

	/**
	 * 解析数据流
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		if (seek + 24 > off + len) {
			throw new SizeOutOfBoundsException("field size missing, < 24");
		}
		mod = Numeric.toInteger(b, seek, 4);
		seek += 4;
		items = Numeric.toInteger(b, seek, 4);
		seek += 4;
		begin = Numeric.toLong(b, seek, 8);
		seek += 8;
		end = Numeric.toLong(b, seek, 8);
		seek += 8;

		return seek - off;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || object.getClass() != DiskField.class) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((DiskField) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return (int) (begin ^ end ^ mod);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DiskField field) {
		int ret = (mod < field.mod ? -1 : (mod > field.mod ? 1 : 0));
		if (ret == 0) {
			ret = (begin < field.begin ? -1 : (begin > field.begin ? 1 : 0));
		}
		if (ret == 0) {
			ret = (end < field.end ? -1 : (end > field.end ? 1 : 0));
		}
		return ret;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("mod:%d range:[%d - %d]", mod, begin, end);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new DiskField(this);
	}

}
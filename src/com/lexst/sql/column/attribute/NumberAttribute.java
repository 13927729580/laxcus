/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.util.*;

/**
 * @author scott.liang
 *
 */
public abstract class NumberAttribute extends ColumnAttribute {

	private static final long serialVersionUID = -1767563033064598020L;

	/**
	 * default
	 */
	protected NumberAttribute() {
		super();
	}

	/**
	 * @param type
	 */
	protected NumberAttribute(byte type) {
		super(type);
	}

	/**
	 * @param attribute
	 */
	protected NumberAttribute(NumberAttribute attribute) {
		super(attribute);
	}
	
	/**
	 * 生成数据类型列前缀
	 * @param buff
	 */
	protected void buildPrefix(ByteArrayOutputStream buff) {
		// 列类型
		buff.write( this.getType() );
		// 列ID
		byte[] b = Numeric.toBytes( this.getColumnId() );
		buff.write(b, 0, b.length);

		// 列名字节度度(<=64)和列名
		b = this.getName().getBytes();
		buff.write((byte) (b.length & 0xFF));
		buff.write(b, 0, b.length);

		// 键类型
		buff.write(key);
		
		// NULL状态(yes or no)
		buff.write(buildNullable());

		// 默认函数
		buildFunction(buff);
	}
	
	/**
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	protected int resolvePrefix(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		if(seek + 4 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,4,%d", seek, end);
		}
		// 列类型
		this.setType(b[seek]);
		seek += 1;
		// 列ID
		setColumnId(Numeric.toShort(b, seek, 2));
		seek += 2;
		// 列名长度
		int size = b[seek] & 0xFF;
		seek += 1;
		// 列名
		if(seek + size > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,%d,%d", seek, size, end);
		}
		setName(b, seek, size);
		seek += size;

		if(seek + 2 > end) {
			throw new ColumnAttributeResolveException("sizeout! %d,2,%d", seek, end);
		}
		// 索引键类型
		super.key = b[seek];
		seek += 1;
		// 是否允许NULL状态
		resolveNullable(b[seek]);
		seek += 1;

		// 默认函数
		size = resolveFunction(b, seek, end - seek);
		seek += size;

		return seek - off; 
	}

}
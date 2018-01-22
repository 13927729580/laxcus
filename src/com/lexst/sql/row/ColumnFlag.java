/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.row;

import com.lexst.util.*;

/**
 * @author scott.liang
 * 
 */
final class ColumnFlag {

	int blockSize;

	int item_count;

	short columnId;

	/**
	 * 
	 */
	public ColumnFlag() {
		super();
	}
	
	/**
	 * 列单元头部长度
	 * @return
	 */
	public final int presize() {
		return 10;
	}

	/**
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		if (seek + 10 > end) {
			throw new RowParseException("buffer data missing!");
		}

		// total size of column's block
		blockSize = Numeric.toInteger(b, seek, 4);
		if (seek + blockSize > end) {
			throw new RowParseException("buffer sizeout!");
		}
		seek += 4;
		// column count
		item_count = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// column identity
		columnId = Numeric.toShort(b, seek, 2);
		seek += 2;

		return seek - off;
	}
}

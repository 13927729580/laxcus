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
final class VariableFlag {

	int itemCount;

	byte state;

	long crc32;

	int wordlen;

	/**
	 * 
	 */
	public VariableFlag() {
		super();
	}
	
	public final int length() {
		return 13;
	}

	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		// check block head size
		if (seek + length() > end) {
			throw new RowParseException("block sizeout!");
		}

		// block's head information
		itemCount = Numeric.toInteger(b, seek, 4); // column count
		seek += 4;
		state = b[seek];
		seek += 1;
		crc32 = Numeric.toLong(b, seek, 4);
		seek += 4;
		wordlen = Numeric.toInteger(b, seek, 4);
		seek += 4;
		
		return seek - off;
	}
}
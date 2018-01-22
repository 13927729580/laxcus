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
final class ChunkFlag {

	int chunk_size;

	int row_count;

	short column_count;

	/**
	 * 
	 */
	public ChunkFlag() {
		super();
	}

	public int resolve(byte[] b, int off, int len) {
		if (len < 10) {
			throw new RowParseException("chunk size missing!");
		}

		int seek = off;
		int end = off + len;
		// chunk size
		chunk_size = Numeric.toInteger(b, seek, 4);
		if (seek + chunk_size > end) return -1; // size missing, exit
		seek += 4;
		// 2. row count
		row_count = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 3. column count
		column_count = Numeric.toShort(b, seek, 2);
		seek += 2;

		return seek - off;
	}

}

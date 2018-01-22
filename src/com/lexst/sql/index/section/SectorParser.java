/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.index.section;

import com.lexst.log.client.*;

/**
 * 根据字节流，生成一个分片类
 *
 */
public class SectorParser {

	/** 全部分片类 **/
	private static Class<?>[] sections = new Class<?>[] { CharSector.class,
			SCharSector.class, WCharSector.class, ShortSector.class,
			IntegerSector.class, LongSector.class, FloatSector.class,
			DoubleSector.class, DateSector.class, TimeSector.class,
			TimestampSector.class };
	
	/**
	 * 
	 */
	public SectorParser() {
		super();
	}
	
	/**
	 * 根据字节流判断并且解析成一个ColumnSector子类
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public static ColumnSector split(byte[] b, int off, int len) {
		for (Class<?> clsz : SectorParser.sections) {
			try {
				ColumnSector sector = (ColumnSector) clsz.newInstance();
				// 不匹配，检查下一个
				if (!sector.match(b, off, len)) {
					continue;
				}
				// 解析分片信息
				int size = sector.resolve(b, off, len);
				if (size > 0) return sector;
			} catch (InstantiationException e) {
				Logger.error(e);
			} catch (IllegalAccessException e) {
				Logger.error(e);
			} catch (Exception e) {
				Logger.error(e);
			}
		}
		// NOT FOUND
		return null;
	}

	
}

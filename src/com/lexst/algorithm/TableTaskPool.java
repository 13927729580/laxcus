/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.algorithm;

import java.util.*;

import com.lexst.log.client.*;
import com.lexst.sql.schema.*;

/**
 * 提供数据库表接口的配置池
 */
public class TableTaskPool extends TaskPool {

	/** 数据库表配置集合  */
	private Map<Space, Table> mapTable = new TreeMap<Space, Table>();

	/**
	 * default
	 */
	protected TableTaskPool() {
		super();
	}

	/**
	 * 保留一个数据库表
	 * 
	 * @param table
	 * @return
	 */
	public boolean addTable(Table table) {
		Space space = (Space) table.getSpace().clone();
		boolean success = false;
		super.lockSingle();
		try {
			mapTable.put(space, table);
			success = true;
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockSingle();
		}
		return success;
	}

	/**
	 * 删除一个数据库表
	 * @param space
	 * @return
	 */
	public boolean removeTable(Space space) {
		boolean success = false;
		super.lockSingle();
		try {
			success = (mapTable.remove(space) != null);
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockSingle();
		}
		return success;
	}

	/**
	 * 查找一个数据库表
	 * @param space
	 * @return
	 */
	public Table findTable(Space space) {
		super.lockMulti();
		try {
			return mapTable.get(space);
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

}

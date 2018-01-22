/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.function;

import com.lexst.sql.schema.*;

/**
 * @author scott.liang
 * 
 */
public class SQLFunctionCreator {

	private static Class<?>[] classes = new Class<?>[] { Sum.class, Count.class,
		Now.class,
			Format.class, Mid.class};

	/**
	 * default
	 */
	public SQLFunctionCreator() {
		super();
	}

	/**
	 * 建立一个SQLFunction实例
	 * 
	 * @param table
	 * @param sql
	 * @return
	 */
	public static SQLFunction create(Table table, String sql) {
		for (int i = 0; i < classes.length; i++) {
			try {
				SQLFunction function = (SQLFunction) classes[i].newInstance();
				SQLFunction instance = function.create(table, sql);
				if (instance != null) return instance;
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}

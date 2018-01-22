/**
 * @email admin@wigres.com
 *
 */
package com.lexst.algorithm.collect;

import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;

/**
 * 信息打印终端接口。由SQLive 和 SQLive Terminal实现
 */
public interface PrintTerminal {

	/**
	 * 打印错误信息
	 * @param s
	 */
	void showFault(String s);
	
	/**
	 * 打印错误堆栈信息
	 * @param e
	 */
	void showFault(Throwable t);

	/**
	 * 打印正常信息
	 * @param s
	 */
	void showMessage(String s);

	/**
	 * 显示标题(表格信息)
	 * @param sheet
	 * @return
	 */
	int showTitle(Sheet sheet);

	/**
	 * 根据属性表，显示一条记录
	 * @param sheet
	 * @param row
	 * @return
	 */
	int showRow(Sheet sheet, Row row);

}

/**
 * 
 */
package com.lexst.algorithm.collect;

import java.io.*;
import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.algorithm.*;

/**
 * 匹配"collect naming"语句的终端处理接口。包括保存数据和显示数据
 *
 */
public abstract class CollectTask extends BasicTask {
	
	/**
	 * default
	 */
	public CollectTask() {
		super();
	}

	/**
	 * 数据写入指定位置
	 * 
	 * @param filename
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public boolean writeTo(PrintTerminal terminal, String filename, byte[] b, int off, int len) {
		File file = new File(filename);
		boolean success = false;
		try {
			FileOutputStream writer = new FileOutputStream(file);
			writer.write(b, off, len);
			writer.close();
			success = true;
		} catch (IOException e) {
			terminal.showFault(e);
		}
		return success;
	}
			
	/**
	 * 结合conduct和全部表的配置集合，显示数据结果
	 * @param conduct
	 * @param tables
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public abstract int display(Conduct conduct, Map<Space, Table> tables,
			PrintTerminal terminal, byte[] b, int off, int len)
			throws CollectTaskException;
}
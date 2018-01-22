/**
 * @email admin@wigres.com
 *
 */
package com.lexst.algorithm.collect;

import java.util.*;

import com.lexst.site.call.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;

/**
 * 默认的信息显示器
 *
 */
public class DefaultCollectTask extends CollectTask {

	/**
	 * default
	 */
	public DefaultCollectTask() {
		super();
	}

	/**
	 * 数据写入磁盘
	 * @param collect
	 * @param terminal
	 * @param b
	 * @param off
	 * @param len
	 */
	private void writeTo(CollectObject collect, PrintTerminal terminal, byte[] b, int off, int len) {
		String filename = collect.getWriteTo();
		boolean success = super.writeTo(terminal, filename, b, off, len);
		if (success) {
			terminal.showMessage(String.format("data write to %s", filename));
		} else {
			terminal.showFault(String.format("write %s error!", filename));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.algorithm.collect.CollectTask#display(com.lexst.sql.statement.Conduct, java.util.Map, com.lexst.algorithm.collect.PrintTerminal, byte[], int, int)
	 */
	@Override
	public int display(Conduct conduct, Map<Space, Table> tables,
			PrintTerminal terminal, byte[] b, int off, int len) throws CollectTaskException {

		this.writeTo(conduct.getCollect(), terminal, b, off, len);
		terminal.showMessage(String.format("conduct record size:%d", len));
		
		int seek = off;
		int end = off + len;
		ReturnTag tag = new ReturnTag();
		int size = tag.resolve(b, seek, end - seek);
		terminal.showMessage(String.format("return tag size:%d", size));
		seek += size;
		
		int fields = tag.getFields();
		terminal.showMessage(String.format("field count:%d", fields));
		int index = 0;
		for(int fieldsize : tag.getFieldSizes()) {
			// 调用相关接口解析数据域
			terminal.showMessage(String.format("field:%d, size:%d", index+1, fieldsize));
			
			// 移到下一个位置
			seek += fieldsize;
		}

		// 返回解析的数据流长度
		return seek - off;
	}

}
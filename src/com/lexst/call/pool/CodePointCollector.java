/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.call.pool;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.pool.site.*;
import com.lexst.remote.client.home.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.charset.codepoint.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.util.*;

import java.io.*;
import java.util.*;

/**
 * 收集字符列的第一个字符(UTF16的代码位，整型值)，做为排序之用
 */
public class CodePointCollector extends JobPool {

	private static CodePointCollector selfHandle = new CodePointCollector();

//	private CallLauncher callInstance;
	
	/** 只记录有字符索引的表，其它表过滤  **/
	private Map<Space, Table> mapTables = new TreeMap<Space, Table>();
	
	/** 表空间 -> UTF16代码位集合 **/
	private Map<Docket, CodePointCounter> mapCounter = new TreeMap<Docket, CodePointCounter>();

	/** 保存传入的数据  **/
	private LinkedList<Inject> stack = new LinkedList<Inject>();

	/**
	 * default
	 */
	private CodePointCollector() {
		super();
	}
	
	/**
	 * 静态句柄
	 * 
	 * @return
	 */
	public static CodePointCollector getInstance() {
		return CodePointCollector.selfHandle;
	}

//	/**
//	 * @param s
//	 */
//	public void setLauncher(CallLauncher s) {
//		this.callInstance = s;
//	}
	
//	/**
//	 * 保存有关键字索引配置的表
//	 * 
//	 * @param table
//	 * @return
//	 */
//	public boolean addTable(Table table) {
//		Space space = table.getSpace();
//		if (mapTables.containsKey(space)) {
//			return false;
//		}
//		
//		// 保存条件：必须是索引键(包括主键或者从键)，且是字符类型
//		for (ColumnAttribute attribute : table.values()) {
//			if(attribute.isKey() && attribute.isWord()) {
//				return mapTables.put(space, table) == null;
//			}
//		}
//		return false;
//	}
	
	public boolean removeTable(Space space) {
		boolean success = false;
		super.lockSingle();
		try {
			success = (mapTables.remove(space) != null);
		} catch (java.lang.Throwable e) {
			
		} finally {
			super.unlockSingle();
		}
		return success;
	}
	
	/**
	 * 保存一批数据
	 * @param inject
	 * @return
	 */
	public boolean push(Inject inject) {
		Space space = inject.getSpace();
		super.lockSingle();
		try {
			// 如果有此表配置，证明有代码位
			Table table = mapTables.get(space);
			// 如果没有数据表配置，去FromPool找
			if (table == null) {
				table = FromPool.getInstance().findTable(space);
				if (table == null) return false;
				// 保存条件：必须是索引键(包括主键或者从键)，且是字符类型
				for (ColumnAttribute attribute : table.values()) {
					boolean found = (attribute.isKey() && attribute.isWord());
					if (found) {
						mapTables.put(space, table);
						break;
					}
				}
			}
		} catch (Throwable e) {
			Logger.fatal(e);
		} finally {
			super.unlockSingle();
		}

		// 数据压入堆栈
		boolean success = false;
		super.lockSingle();
		try {
			success = this.stack.add(inject);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		if (success) {
			super.wakeup();
		}
		return success;
	}

	/**
	 * 保存一个
	 * @param insert
	 * @return
	 */
	public boolean push(Insert insert) {
		Space space = insert.getSpace();
		Table table = FromPool.getInstance().findTable(space); // mapTables.get(insert.getSpace());
		if (table != null) {
			Inject inject = new Inject(table);
			inject.add(insert.getRow());
			return this.push(inject);
		}
		return false;
	}

	/**
	 * 从记录集中的字符列中取代码位
	 * 
	 * @param inject
	 */
	private void translate(Inject inject) {
		//1. 取出字符类的属性信息
		Space space = inject.getSpace();
		Table table = FromPool.getInstance().findTable(space); // mapTables.get(space);
		List<java.lang.Short> array = new ArrayList<java.lang.Short>();
		for (ColumnAttribute attribute : table.values()) {
			// 是索引并且是字符串类型,保留它的列ID
			if (attribute.isKey() && attribute.isWord()) {
				array.add(attribute.getColumnId());
			}
		}

		//2. 逐一提取列中的首字符代码位
		for (short columnId : array) {
			Docket deck = new Docket(space, columnId);
			CodePointCounter counter = mapCounter.get(deck);
			if (counter == null) {
				counter = new CodePointCounter(deck);
				mapCounter.put(deck, counter);
			}
			
			ColumnAttribute attribute = table.find(columnId);

			// 根据属性返回对应的字符集
			WordAttribute variable = (WordAttribute) table.find(columnId);
			Charset charset = VariableGenerator.getCharset(attribute);

			// 逐一提取字符列的信息
			for (Row row : inject.list()) {
				Column column = row.find(columnId);
				if (!column.isWord()) continue;

				Word string = (Word) column;
				// 如果有索引，就返回索引值，否则返回数据值
				byte[] value = string.getValid();

				// 如果数据被打包（加密、压缩），先执行反操作还原数据
				if (variable.getPacking().isEnabled()) {
					try {
						value = VariableGenerator.depacking(variable, value, 0, value.length);
					} catch (IOException e) {
						Logger.error(e);
						continue;
					}
				}
				// 调用字符集取出首字符的代码位(先解码，再取代码位)
				int codePoint = charset.codePointAt(0, value, 0, value.length);
				// 保存这个代码位
				counter.add(codePoint);
			}
		}
	}

	/**
	 * 从对队中取出记录集，逐一解析，提取关键字的首字符代码位
	 */
	private void subprocess() {
		while (!stack.isEmpty()) {
			Inject inject = null;
			super.lockSingle();
			try {
				inject = stack.poll();
			} catch (Throwable exp) {
				Logger.fatal(exp);
			} finally {
				super.unlockSingle();
			}
			if (inject != null) {
				this.translate(inject);
			}
		}
	}
	
	/**
	 * 定时向服务器上传数据
	 */
	private void send() {
		if(mapCounter.isEmpty()) return;
		
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024000);
		for (Docket docket : mapCounter.keySet()) {
			CodePointCounter counter = mapCounter.get(docket);
			byte[] b = counter.build();
			buff.write(b, 0, b.length);
		}
		byte[] b = buff.toByteArray();
		// 压缩数据流
		try {
			b = Inflator.gzip(b, 0, b.length);
		} catch (IOException e) {
			Logger.error(e);
			return;
		}
		
		// 采用GZIP压缩数据后，发送数据流
		boolean success = false;
		HomeClient client = super.bring();
		try {
			if(client != null) {
				success = client.sendCodePoints("GZIP", b);
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} 
		// 关闭连接
		super.complete(client);

		// 打印日志
		if (success) {
			mapCounter.clear();
			Logger.info("CodePointCollector.send, send codepoint success!");
		} else {
			Logger.error("CodePointCollector.send, send codepoint failed!");
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// List<Space> list = callInstance.getSpaces();
		// for(Space space : list) {
		// Table table = callInstance.findTable(space);
		// if(table != null) this.addTable(table);
		// }
		
		// 1分钟自动唤醒一次
		this.setSleep(60);
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("CodePointCollector.process, into...");
		// 30 分钟发送一次
		final long interval = 1800 * 1000;
		long endtime = System.currentTimeMillis() + interval;
		
		// 循环处理发送数据
		while (!super.isInterrupted()) {
			this.subprocess();
			if (System.currentTimeMillis() >= endtime) {
				endtime = System.currentTimeMillis() + interval; // 下一次发送时间
				this.send();
			}
			this.sleep();
		}
		
		this.subprocess();
		this.send();
		Logger.info("CodePointCollector.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		
	}
	
}
/**
 * 
 */
package com.lexst.data.pool;

import java.io.*;
import java.util.*;

import com.lexst.data.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.sql.schema.*;

/**
 * SQL INSERT异步数据存储池。<br>
 * 操作过程是：数据首先保存磁盘临时目录，数据操作号进入内存的存储队列，线程逐一读取进入存储队列的操作号，通过操作号读取磁盘文件，将实际数据写入JNI存储集中
 */
public class StayPool extends Pool {
		
	/** 临时文件最大存储单位: 64m **/
	private final static long MAX_SIZE = 0x4000000L;

	/** 实例句柄 **/
	private static StayPool selfHandle = new StayPool();

	/** 当前数据实体块 **/
	private StayEntity entity;

	/** 异步写入索引序号 */
	private int sequenceIndex;

	/** 保存异步写入数据的目录 **/
	private File tempPath;

	/** 磁盘序列号 -> 数据实体 **/
	private Map<Integer, StayEntity> entities = new TreeMap<Integer, StayEntity>();

	/**
	 * default
	 */
	private StayPool() {
		super();
		this.setSleep(5);
		sequenceIndex = 1;
	}

	/**
	 * 分配操作号
	 * @return
	 */
	private int nextIdnetity() {
		if(sequenceIndex == Integer.MAX_VALUE) {
			this.sequenceIndex = 1;
		}
		return this.sequenceIndex++;
	}

	/**
	 * StayPool的静态句柄
	 * @return
	 */
	public static StayPool getInstance() {
		return StayPool.selfHandle;
	}

	/**
	 * 建立临时目录
	 * @param dir
	 * @return
	 */
	public boolean createTempPath(File dir) {
		boolean success = (dir.exists() && dir.isDirectory());
		// 如果目录不存在，建立它
		if (!success) {
			success = dir.mkdirs();
		}
		if (success) {
			try {
				tempPath = dir.getCanonicalFile();
			} catch (IOException e) {
				tempPath = dir.getAbsoluteFile();
			}
		}
		Logger.note(success, "StayPool.createTempPath, directory is '%s'", tempPath);
		return success;
	}
	
	/**
	 * 建立临时目录
	 * @param path
	 * @return
	 */
	public boolean createTempPath(String path) {
		return this.createTempPath(new File(path));
	}

	/**
	 * 返回临时目录
	 * @return
	 */
	public File getTempPath() {
		return this.tempPath;
	}

	/* (non-Javadoc)
	 * @see com.dwms.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		if (tempPath == null) {
			String path = System.getProperty("java.io.tmpdir");
			if (!createTempPath(path)) return false;
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see com.dwms.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("StayPool.process, into...");
		while (!isInterrupted()) {
			this.sleep();
			this.execute();
		}
		// 关闭文件并且设置完成标记
		this.complete();
		// 添加剩余数据磁盘存储
		this.execute();

		Logger.info("StayPool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.dwms.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		
	}
	
	/**
	 * 给当前存储块设置完成标记
	 */
	public void complete() {
		if (entity != null) {
			entity.setCompleted(true);
			entity = null;
		}
	}

	/**
	 * 申请异步存储块
	 * @return
	 */
	private StayEntity apply() {
		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			int diskid = this.nextIdnetity();
			String name = String.format("%X.lxas", diskid);
			// 定义一个临时目录下的文件
			File file = new File(this.tempPath, name);
			if (!file.exists()) {
				Logger.info("StayPool.apply, filename is '%s'", file);
				return new StayEntity(diskid, file.getAbsolutePath());
			}
		}
		return null;
	}
	
	/**
	 * 写数据到磁盘上
	 * @param data
	 * @return
	 */
	public boolean write(byte[] data) {
		boolean success = false;
		super.lockSingle();
		try {
			// 如果当前文件未完成时
			if (entity == null) {
				entity = this.apply(); 
				entities.put(entity.getDiskid(), entity);
			}
			
			String filename = entity.getFilename();
			// 调用JNI接口，写数据到磁盘上
			long time = System.currentTimeMillis();
			long fileoff = Install.append(filename.getBytes(), data, 0, data.length);
			Logger.note(fileoff > 0, "StayPool.write, insert %s offset %d, usedtime:%d", 
					filename, fileoff, System.currentTimeMillis() - time);
			
			if (fileoff >= 0L) {
				StayNode node = new StayNode(fileoff, data.length);
				// 保存它
				entity.add(node);
				// 文件尺寸溢出时建立新文件
				if (fileoff + data.length >= StayPool.MAX_SIZE) {
					entity.setCompleted(true);
					entity = null;
				}
				success = true;
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		// 通知线程写数据
		if(success) wakeup();
		return success;
	}
	
	private void remove(int diskid) {
		super.lockSingle();
		try {
			entities.remove(diskid);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
	}

	private StayEntity get(int diskid) {
		super.lockMulti();
		try {
			return entities.get(diskid);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	private List<Integer> list() {
		ArrayList<Integer> array = new ArrayList<Integer>();
		super.lockMulti();
		try {
			if (entities.size() > 0) {
				array.addAll(entities.keySet());
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockMulti();
		}
		if (array.size() > 0) {
			Collections.sort(array);
		}
		return array;
	}

	/**
	 * inset buffer data to lexst db
	 */
	private void execute() {
		if (entities.isEmpty()) return;
		// get identity
		for (int id : list()) {
			StayEntity bunket = get(id);
			if (bunket.size() > 0) {
				insert(bunket);
			}
			if (bunket.isCompleted() && bunket.isEmpty()) {
				String filename = bunket.getFilename();
				File file = new File(filename);
				boolean success = file.delete();
				Logger.note(success, "StayPool.execute, delete file %s", filename);
				remove(id);
			}
		}
	}

	/**
	 * 调用JNI接口，追加数据到磁盘存储器
	 * @param bunket
	 */
	private void insert(StayEntity bunket) {
		String filename = bunket.getFilename();
		while(true) {
			StayNode node = null;
			super.lockSingle();
			try {
				node = bunket.poll();
			} catch (Throwable exp) {
				Logger.fatal(exp);
			} finally {
				super.unlockSingle();
			}
			if(node == null) break;

			// 调用JNI接口读取数据
			boolean success = false;
			long time = System.currentTimeMillis();
			byte[] data = Install.read(filename.getBytes(), node.getOffset(), node.getLength());
			
			Logger.debug("StayPool.insert, read [%s %d - %d | %d], usedtime:%d",
					filename, node.getOffset(), node.getLength(),
					(data == null ? -1 : data.length), System.currentTimeMillis() - time);
			if (data != null && data.length > 0) {
				success = this.jniInsert(data);
			}
			Logger.note(success, "StayPool.insert, insert [%d - %d] to lexst db", node.getOffset(), node.getLength());
			data = null;
		}
	}
	
	/**
	 * 调用JNI接口，追加数据进入磁盘中
	 * @param data
	 * @return
	 */
	private boolean jniInsert(byte[] data) {
		Logger.info("StayPool.jniInsert, data len:%d", data.length);
		long time = System.currentTimeMillis();
		
		int stamp = Install.insert(data);
		if(stamp < 0) { // occur error
			return false;
		}
		
		// 根据日志发送更新数据到快点节点做备份
		byte[] log = Install.findReflexLog(stamp);

		int rows = 0;
		for (int seek = 0; seek < log.length;) {
			ReflexLog reflex = new ReflexLog();
			int len = reflex.resolve(log, seek, log.length - seek);
			seek += len;

			rows += reflex.getRows();

			Space space = reflex.getSpace();
			byte[] schema = space.getSchema().getBytes();
			byte[] table = space.getTable().getBytes();

			for (long cacheid : reflex.listCacheIdentity()) {
				// get cache data
				byte[] entity = Install.getCacheReflex(schema, table, cacheid);
				if(entity == null) continue;
				// 数据保存到CachePool，再传递到快点节点备份
				CachePool.getInstance().update(space, cacheid, entity);
			}
		}
		
		Logger.info("StayPool.jniInsert, data len:%d, item count:%d, insert time:%d",
				data.length, rows, System.currentTimeMillis() - time);
		return rows > 0;
	}

}
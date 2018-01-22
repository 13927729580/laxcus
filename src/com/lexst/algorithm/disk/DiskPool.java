/**
 * 
 */
package com.lexst.algorithm.disk;

import java.io.*;
import java.util.*;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.util.host.*;

/**
 * 管理conduct计算过程中产生的数据(包括diffuse和aggregate阶段)。<br><br>
 * 
 * 工作范围：<br>
 * <1> 任务号分配 <br>
 * <2> 数据的写入和读取(每次都是先写后读) <br>
 * <3> 对托管数据时间监控(时间到期删除) <br><br>
 * 
 * 注: 任务号标识数据在磁盘的唯一。数据最开始写入返回一个任务号，读取时通过任务号读取磁盘数据。<br>
 */
public class DiskPool extends Pool implements DiskTrustor {
	
	private static DiskPool selfHandle = new DiskPool();
	
	/** 当前主机地址 */
	private SiteHost local;

	/** 任务标识号产生器，初始化是0 */
	private long jobidIncrment;

	/** 任务号 -> 磁盘数据图谱  */
	private Map<Long, DiskChart> mapArea = new TreeMap<Long, DiskChart>();
	
	/** 任务号 -> 数据到期时间(时间到期即删除) */
	private Map<Long, Long> mapTime = new TreeMap<Long, Long>();

	/** conduct数据存储目录 */
	private File root;

	/** conduct服务超时时间 */
	private long timeout;

	/**
	 * default
	 */
	private DiskPool() {
		super();
		this.jobidIncrment = 0L;
		this.setTimeout(30 * 60 * 1000); // 30 minute
	}

	/**
	 * @return
	 */
	public static DiskPool getInstance() {
		return DiskPool.selfHandle;
	}

	/**
	 * 检查并且建立conduct数据存储目录
	 * @param dir
	 * @return
	 */
	public boolean setRoot(File dir) {
		boolean success = (dir.exists() && dir.isDirectory());
		if (!success) {
			success = dir.mkdirs();
		}
		if (success) {
			try {
				this.root = dir.getCanonicalFile();
			} catch (IOException e) {
				this.root = dir.getAbsoluteFile();
			}
		}
		Logger.note(success, "DiskPool.setPath, create path '%s'", root);
		return success;
	}
	
	/**
	 * 检查并且建立conduct数据存储目录
	 * 
	 * @param dir
	 * @return
	 */
	public boolean setRoot(String dir) {
		return this.setRoot(new File(dir));
	}

	/**
	 * 返回conduct数据存储目录
	 * @return
	 */
	public File getRoot() {
		return this.root;
	}
	
	/**
	 * 设置磁盘数据超时时间(通常为30分钟，或用户自定义)
	 * @param millisecond
	 */
	public void setTimeout(long millisecond) {
		this.timeout = millisecond;
	}
	
	/**
	 * 返回磁盘数据超时时间
	 * @return
	 */
	public long getTimeout() {
		return this.timeout;
	}
	
	/**
	 * 根据任务号生成对应的文件名
	 * @param jobid
	 * @return
	 */
	private String buildJobFile(long jobid) {
		StringBuilder b = new StringBuilder();
		b.append(String.format("%x", jobid));
		while (b.length() < 16) {
			b.insert(0, '0');
		}

		String s = String.format("%s.conduct", b.toString());
		return new File(this.root, s).getAbsolutePath();
	}
	
	/**
	 * 设置当前主机地址
	 * 
	 * @param host
	 */
	public void setLocal(SiteHost host) {
		this.local = new SiteHost(host);
	}

	/*
	 * 返回当前主机绑定地址
	 * @see com.lexst.algorithm.disk.DiskTrustor#getLocal()
	 */
	@Override
	public SiteHost getLocal() {
		return this.local;
	}

	/*
	 * 产生任务号(此方法提供给调用者)
	 * @see com.lexst.algorithm.disk.DiskTrustor#nextJobid()
	 */
	@Override
	public synchronized long nextJobid() {
		if (jobidIncrment >= Long.MAX_VALUE) jobidIncrment = 0L;
		return jobidIncrment++;
	}

	/*
	 * 超时时间
	 * @see com.lexst.algorithm.diffuse.ConductTrustor#timeout()
	 */
	@Override
	public int timeout() {
		return (int) (this.timeout / 1000);
	}

	/*
	 * 数据写入，返回数据在文件中的下标范围
	 * @see com.lexst.algorithm.diffuse.ConductTrustor#write(long, int, byte[], int, int)
	 */
	@Override
	public long[] write(long jobid, int mod, byte[] data, int off, int len) {
		String filename = buildJobFile(jobid);
		
		super.lockSingle();
		try {
			// 移到到磁盘文件的最后位置(开始是0)
			long begin = 0;
			File file = new File(filename);
			if (file.exists() && file.isFile()) begin = file.length();

			// 写数据到磁盘上
			FileOutputStream out = new FileOutputStream(file);
			out.write(data, off, len);
			out.close();

			// write end, base for 0
			file = new File(filename);
			long end = file.length() - 1;

			DiskField field = new DiskField(mod, begin, end);
			DiskChart chart = mapArea.get(jobid);
			if (chart == null) {
				chart = new DiskChart(jobid);
				mapArea.put(jobid, chart);
			}
			chart.add(field);
			
			// 数据最后到期时间
			mapTime.put(jobid, System.currentTimeMillis() + timeout);
						
			return new long[] { begin, end };
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		
		return null; // failed
	}
	
	/*
	 * 根据任务号、模值、数据范围，找到文件并且读取数据
	 * @see com.lexst.algorithm.diffuse.ConductTrustor#read(long, int, long, long)
	 */
	@Override
	public byte[] read(long jobid, int mod, long begin, long end) {
		String filename = buildJobFile(jobid);
		byte[] b = null;

		super.lockSingle();
		try {
			// check file
			File file = new File(filename);
			
			if (!file.exists() || !file.isFile()) return null;
			if (file.length() < begin || file.length() < end) return null;

			DiskField field = new DiskField(mod, begin, end);
			DiskChart area = mapArea.get(jobid);
			if (area == null) return null;
			boolean success = area.remove(field);
			if (!success) return null;

			int size = (int) field.length();
			b = new byte[size];

			FileInputStream in = new FileInputStream(file);
			in.read(b, 0, b.length);
			in.close();

			if (area.isEmpty()) {
				mapArea.remove(jobid);
				mapTime.remove(jobid);
				file.delete(); // delete disk file
			}
		} catch (IOException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
		
		return b;
	}

	/**
	 * 检查磁盘上超时的数据(超时即删除)
	 */
	private void check() {
		int size = mapTime.size();
		if (size == 0) return;

		ArrayList<Long> array = new ArrayList<Long>(size);
		long now = System.currentTimeMillis();

		super.lockSingle();
		try {
			for (long jobid : mapTime.keySet()) {
				long time = mapTime.get(jobid);
				if (now >= time) array.add(jobid);
			}

			for (long jobid : array) {
				mapTime.remove(jobid);
				mapArea.remove(jobid);

				String filename = buildJobFile(jobid);
				File file = new File(filename);
				if (file.exists() && file.isFile()) {
					file.delete();
					Logger.info("DiskPool.check, delete timeout file %s", filename);
				}
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			super.unlockSingle();
		}
	}

	/**
	 * clear all
	 */
	private void clear() {
		for(long jobid : mapArea.keySet()) {
			String filename = buildJobFile(jobid);
			File file = new File(filename);
			if(file.exists() && file.isFile()) file.delete();
		}
		
		mapArea.clear();
		mapTime.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// 初始化目录，如果目录无效，在用户目录下建立一个"conduct"目录
		if(root == null) {
			// "bin" directory
			String bin = System.getProperty("user.dir");
			setRoot(new File(bin, "conduct"));
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("DiskPool.process, into...");
		while (!isInterrupted()) {
			check();
			this.delay(5000);
		}
		Logger.info("DiskPool.process, exit");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		this.clear();
	}

}
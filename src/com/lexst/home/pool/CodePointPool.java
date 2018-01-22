/**
 * @email admin@lexst.com
 *
 */
package com.lexst.home.pool;

import java.util.*;
import java.io.*;
import java.nio.*;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.sql.charset.codepoint.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.util.*;


/**
 * UTF16码位存储集合
 * 
 * 接受从DATA节点传来的码位数据并且解析保存，CALL节点从这里提取所需要的码位
 *
 */
public class CodePointPool extends JobPool {

	private static CodePointPool selfHandle = new CodePointPool();
	
	/** 存储代码位的目录 **/
	private File root;
	
	/** 表区间 -> 代码位集合(UTF16) **/
	private Map<Docket, CodePointRegion> regions = new TreeMap<Docket, CodePointRegion>();
	
	/** 等待被 解析数据队列 **/
	private LinkedList<ByteBuffer> stack = new LinkedList<ByteBuffer>();
	
	/**
	 * default
	 */
	private CodePointPool() {
		super();
	}
	
	/**
	 * 返回静态句柄
	 * 
	 * @return
	 */
	public static CodePointPool getInstance() {
		return CodePointPool.selfHandle;
	}
	
	/**
	 * 设置代码位文件存储目录
	 * 
	 * @param dir
	 */
	public boolean setRoot(File dir) {
		boolean success = (dir.exists() && dir.isDirectory());
		// 不存在就建立一个
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
		Logger.note(success, "CodePointPool.setRoot, directory is '%s'", this.root);
		return success;
	}

	/**
	 * 设置代码位存储目录
	 * @param path
	 * @return
	 */
	public boolean setRoot(String path) {
		return this.setRoot(new File(path));
	}
	
	/**
	 * 存储目录
	 * @return
	 */
	public File getRoot() {
		return this.root;
	}
	
	/**
	 * 存储文件名
	 * @return
	 */
	private final File getFile() {
		return new File(root, "codepoing.bin");
	}
	
	/**
	 * 先解压缩，再保存数据
	 * 
	 * @param compress - 压缩算法名称
	 * @param stream - 数据流
	 * @return
	 */
	public boolean save(String compress, byte[] stream) {
		try {
			if ("GZIP".equalsIgnoreCase(compress)) {
				stream = Deflator.gzip(stream, 0, stream.length);
			} else if ("ZIP".equalsIgnoreCase(compress)) {
				stream = Deflator.zip(stream, 0, stream.length);
			}
		} catch (IOException e) {
			Logger.error(e);
			return false;
		}
		
		// 保存数据流
		boolean success = false;
		super.lockSingle();
		try {			
			success = stack.add(ByteBuffer.wrap(stream, 0, stream.length ));
		} catch (Throwable exp) {
			Logger.error(exp);
		} finally {
			super.unlockSingle();
		}

		// 保存成功,通知线程处理
		if (success) super.wakeup();
		return success;
	}
	
	/**
	 * 根据表空间名，查找对应的首字符代码位集合
	 * 
	 * @param deck
	 * @return
	 */
	public int[] find(Docket deck) {
		super.lockMulti();
		try {
			CodePointRegion region = regions.get(deck);
			if (region != null) {
				return region.paris();
			}
		} catch (Throwable exp) {
			Logger.error(exp);
		} finally {
			super.unlockMulti();
		}
		return null;
	}

	/**
	 * 解析从DATA节点传来的码位数据流
	 * @param b
	 * @param off
	 * @param len
	 */
	private void resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		while (seek < end) {
			CodePointCounter counter = new CodePointCounter();
			int size = counter.resolve(b, seek, end - seek);
			if (size == -1) { // error
				Logger.error("CodePointPool.resolve, error position %d", seek);
				break;
			}
			seek += size;
			
			int[] paris = counter.paris();
			if (paris.length % 2 != 0) {
				Logger.error("CodePointPool.resolve, paris error! size:%d", paris.length);
				continue;
			}
			
			Docket deck = counter.getDocket();
			CodePointRegion region = regions.get(deck);
			if (region == null) {
				region = new CodePointRegion(deck);
				regions.put(deck, region);
			}
			// 保存代码位和统计值
			for (int i = 0; i < paris.length; i += 2) {
				region.add(paris[i], paris[i + 1]);
			}
		}
	}

	/**
	 * 从队列中取出码位数据流，进行解析
	 */
	private void subprocess() {
		while (!stack.isEmpty()) {
			ByteBuffer buff = null;
			super.lockSingle();
			try {
				buff = stack.poll();
			} catch (Throwable exp) {
				Logger.error(exp);
			} finally {
				super.unlockSingle();
			}
			if (buff != null) {
				byte[] b = buff.array();
				this.resolve(b, 0, b.length);
			}
		}
	}
	
	/**
	 * 从磁盘读代码位数据
	 * 
	 */
	private void readLog() {
		File file = getFile();
		if(!file.exists()) return;
		
		byte[] b = new byte[(int) file.length()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(b, 0, b.length);
			in.close();
		} catch (IOException e) {
			Logger.error(e);
			return;
		}
		
		int seek = 0;
		while(seek < b.length) {
			CodePointRegion sector = new CodePointRegion();
			int size = sector.resolve(b, seek, b.length - seek);
			if(size == -1) {
				break; // 出错
			}
			seek += size;
			
			Docket deck = sector.getDocket();
			regions.put(deck, sector);
		}
	}
	
	/**
	 * 写数据到磁盘上
	 */
	private void writeLog() {
		if (regions.isEmpty()) return;
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		for (CodePointRegion region : regions.values()) {
			byte[] b = region.build();
			buff.write(b, 0, b.length);
		}
		byte[] b = buff.toByteArray();
		// 写入磁盘
		try {
			File file = getFile();
			FileOutputStream out = new FileOutputStream(file);
			out.write(b, 0, b.length);
			out.flush();
			out.close();
		} catch (IOException e) {
			Logger.error(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// 确定存储目录
		if (root == null) {
			// "bin" directory
			String bin = System.getProperty("user.dir");
			this.setRoot(new File(bin, "CodePoint"));
		}
		
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("CodePointPool.process, into...");
		// 读数据
		this.readLog();
		while (!super.isInterrupted()) {
			subprocess();
			super.sleep();
		}
		subprocess();
		// 写数据
		this.writeLog();
		Logger.info("CodePointPool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		
	}

}
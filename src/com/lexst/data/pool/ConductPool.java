/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.data.pool;

import java.io.*;

import com.lexst.algorithm.disk.*;
import com.lexst.algorithm.from.*;
import com.lexst.data.*;
import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.sql.row.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;
import com.lexst.util.naming.*;

/**
 * 分布计算"diffuse"阶段任务分派接口
 *
 */
public class ConductPool extends JobPool {
	
	/** ConductPool静态句柄(只有一个) */
	private static ConductPool selfHandle = new ConductPool();

	/**
	 * default
	 */
	private ConductPool() {
		super();
	}
	
	/**
	 * 返回静态句柄
	 * @return
	 */
	public static ConductPool getInstance() {
		return ConductPool.selfHandle;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		while(!super.isInterrupted()) {
			this.delay(10000);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// TODO Auto-generated method stub

	}

	/**
	 * 分布计算的"diffuse"阶段，返回数据分片后的数据图谱(Area)
	 * 
	 * @param phase
	 * @param resp
	 * @throws IOException
	 */
	public void conduct(FromPhase phase, OutputStream resp) throws IOException {
		// 存在两种可能: 有SELECT或者没有
		if (phase.getSelect() != null) {
			this.conductSelect(phase, resp);
		} else {
			this.conductFlush(null, phase, resp);
		}
	}
	
	/**
	 * SELECT检索
	 * @param phase
	 * @param resp
	 * @throws IOException
	 */
	private void conductSelect(FromPhase phase, OutputStream resp) throws IOException {
		Select select = phase.getSelect();
		byte[] meta = select.build();
		
		// 进行SELECT检索，返回检索标记(小于是错误码，等于0未找到，大于是标记码。根据标记码取对应的数据)
		int stamp = Install.select(meta);
		if (stamp < 0) {
			flushConductResult(resp, Response.CONDUCT_SERVERERR, Numeric.toBytes(stamp));
			return;
		} else if (stamp == 0) {
			flushConductResult(resp, Response.CONDUCT_FAILED, null);
			return;
		}
		
		byte[] data = Install.nextSelect(stamp, 0x100000); // 1M
		// 分析报头，确定全部数据流长度(标记头和检索数据)
		AnswerFlag flag = new AnswerFlag();
		int size = flag.resolve(data, 0, data.length);
		long allsize = size + flag.getSize();

		// 读取检索结果数据
		ByteArrayOutputStream buff = new ByteArrayOutputStream((int) allsize);
		buff.write(data, 0, data.length);
		// 从JNI接口，继续读取下一段数据
		long seek = data.length;
		while (seek < allsize) {
			data = Install.nextSelect(stamp, 0x100000);
			buff.write(data, 0, data.length);
			seek += data.length;
		}
		
		// 输出结果
		conductFlush(buff, phase, resp);
	}
	
	/**
	 * @param buff
	 * @param phase
	 * @param resp
	 * @throws IOException
	 */
	private void conductFlush(ByteArrayOutputStream buff, FromPhase phase, OutputStream resp) throws IOException {
		// check table

//		// 进入from的SELECT只能有一个
//		FromObject from = conduct.getFrom();
//		if(from.countSelect() == 1) {
//			Select select = from.getSelects().get(0);
//			Space space = select.getSpace();
//			Naming naming = conduct.getFrom().getTaskNaming();
//			Project project = FromTaskPool.getInstance().findProject(naming);
//			Table table = project.getTable(space);
//			if (table == null) {
//				table = com.lexst.data.Launcher.getInstance().findTable(space);
//				if (table == null) {
//					Logger.error("SQLPool.conduct_flush, cannot find table: '%s'", space);
//					flushDistributeResult(resp, Response.DISTRIBUTE_FAILED, null);
//					return;
//				}
//				project.setTable(space, table);
//			}
//		}
		
		// 根据命名，找到对应的对象实例
		Naming naming = phase.getNaming();
		FromTask task = FromTaskPool.getInstance().find(naming);
		if (task == null) {
			Logger.error("SQLPool.conduct_flush, cannot find conduct-task '%s'", naming);
			flushConductResult(resp, Response.CONDUCT_FAILED, null);
			return;
		}
		
//		// 给FromTask设置数据库表配置
//		if(phase.getSelect() != null) {
//			Space space = phase.getSelect().getSpace();
//			Table table = Launcher.getInstance().findTable(space);
//			if(table == null) {
//				Logger.error("ConductPool.conduct_flush, cannot find %s", space);
//				flushConductResult(resp, Response.CONDUCT_FAILED, null);
//				return;
//			}
//			task.setTable(table);
//		}

//		byte[] b = (buff != null ? buff.toByteArray() : null);
//		// 对数据进行分片处理，返回分片图谱
//		Area area = task.divideup( DiskPool.getInstance(), phase, b, 0, b.length);
//		b = null;
//		if (buff != null) buff.reset();
//		 build Area to bytes
//		ByteArrayOutputStream mem = new ByteArrayOutputStream();
//		for (int i = 0; results != null && i < results.length; i++) {
//			byte[] b = results[i].build();
//			mem.write(b, 0, b.length);
//		}
//		byte[] b = area.build();
//		mem.write(b, 0, b.length);
//		data = mem.toByteArray();

		// 对数据进行分片处理，返回分片图谱
		byte[] b = (buff != null ? buff.toByteArray() : null);
		DiskArea area = task.divideup(DiskPool.getInstance(), phase, b, 0, b.length);
		// 数据图谱转成字节流输出
		b = area.build();

		Logger.debug("SQLPool.conduct_flush,  conduct byte size:%d", b.length);

		this.flushConductResult(resp, Response.CONDUCT_OKAY, b);
	}
	
//	private Stream buildReply(short code) {
//		Command cmd = new Command(code);
//		return new Stream(cmd);
//	}
	
	/**
	 * 输出分布计算结果的信息流
	 * @param resp
	 * @param code
	 * @param b
	 * @throws IOException
	 */
	private void flushConductResult(OutputStream resp, short code, byte[] b) throws IOException {
//		Command cmd = new Command(code);
//		Stream reply = new Stream(new Command(code));
//		Stream reply = buildReply(code);
		
		Stream reply = new Stream(new Command(code));
		byte[] bytes = null;
		if (b != null && b.length > 0) {
			bytes = reply.buildHead(b.length);
		} else {
			bytes = reply.build();
		}
		resp.write(bytes, 0, bytes.length);
		if (b != null && b.length > 0) {
			resp.write(b, 0, b.length);
		}
		resp.flush();
	}
}

/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.conduct;

import com.lexst.sql.index.section.*;
import com.lexst.util.host.*;

/**
 * 远程连接配置接口
 */
public abstract class OutputPhase extends NamingObject implements Comparable<OutputPhase> {

	private static final long serialVersionUID = -3192230956676374499L;

	/** 下一级分片信息的标准标记名称(其它名称不可以与它重复) */
	private final static String DEFNEXT_SECTOR = "LAXCUS_SYSTEM_SLAVE_SECTOR";

	/** DATA/WORK节点的主机地址 **/
	private SiteHost remote;

	/**
	 * default
	 */
	protected OutputPhase() {
		super();
	}

	/**
	 * 复制对象
	 * 
	 * @param task
	 */
	protected OutputPhase(OutputPhase task) {
		super(task);
		this.setRemote(task.remote);
	}

	/**
	 * 设置服务器主机地址
	 * 
	 * @param s
	 */
	public void setRemote(SiteHost s) {
		this.remote = (SiteHost) s.clone();
	}

	/**
	 * 返回服务器主机地址
	 * 
	 * @return
	 */
	public SiteHost getRemote() {
		return this.remote;
	}

	/**
	 * 设置分片区域(名称是固定的，其它接口不可以重复)
	 * 
	 * @param s
	 */
	public void setSlaveSector(ColumnSector s) {
		this.addSector(OutputPhase.DEFNEXT_SECTOR, s);
	}

	/**
	 * 返回分片区域
	 * 
	 * @return
	 */
	public ColumnSector getSlaveSector() {
		return this.findSector(OutputPhase.DEFNEXT_SECTOR);
	}

	/**
	 * 是否有子级分片定义(判断当前数据是否需要分片时，调用这个方法)
	 * 
	 * @return
	 */
	public boolean hasSlaveSector() {
		return this.getSlaveSector() != null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(OutputPhase object) {
		if (remote == null) {
			return -1;
		} else if (object.remote == null) {
			return -1;
		}

		return remote.compareTo(object.remote);
	}

}
/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.conduct;

import com.lexst.util.host.*;

/**
 * "aggregate"阶段计算任务实例
 * 
 */
public class ToPhase extends OutputPhase {

	private static final long serialVersionUID = 4695265597984801770L;

	/** "to - subto"的链表排列序号，下标从0开始(0是根序号，以后依次递增) */
	private int linkIndex;

	/**
	 * default
	 */
	public ToPhase() {
		super();
		this.linkIndex = 0;
	}

	/**
	 * @param remote
	 */
	protected ToPhase(SiteHost remote) {
		this();
		this.setRemote(remote);
	}

	/**
	 * @param phase
	 */
	public ToPhase(ToPhase phase) {
		super(phase);
		this.linkIndex = phase.linkIndex;
	}

	/**
	 * 设置排列序号
	 * 
	 * @param i
	 */
	public void setLinkIndex(int i) {
		this.linkIndex = i;
	}

	/**
	 * 返回排列序号
	 * 
	 * @return
	 */
	public int getLinkIndex() {
		return this.linkIndex;
	}

	/**
	 * 判断是不是子链(大于0即子链)
	 * 
	 * @return
	 */
	public boolean isSlaveLink() {
		return this.linkIndex > 0;
	}

	/**
	 * 当前阶段是否还有下一级数据分片
	 * 
	 * @return
	 */
	public boolean hasNext() {
		return hasSlaveSector();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.distribute.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new ToPhase(this);
	}

}
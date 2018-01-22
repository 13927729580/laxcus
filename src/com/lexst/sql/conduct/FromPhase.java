/**
 * @email admin@laxcus.com
 *
 */
package com.lexst.sql.conduct;

import com.lexst.sql.statement.*;
import com.lexst.util.host.*;

/**
 * "diffuse"阶段任务实例
 * 
 */
public class FromPhase extends OutputPhase {

	private static final long serialVersionUID = 2481125298551355880L;

	/** SELECT检索 */
	private Select select;

	/**
	 * default
	 */
	public FromPhase() {
		super();
	}

	/**
	 * @param remote
	 */
	protected FromPhase(SiteHost remote) {
		this();
		this.setRemote(remote);
	}

	/**
	 * @param phase
	 */
	public FromPhase(FromPhase phase) {
		super(phase);
		this.setSelect(phase.select);
	}

	/**
	 * 设置SELECT检索
	 * 
	 * @param s
	 */
	public void setSelect(Select s) {
		this.select = s;
	}

	/**
	 * 返回SELECT检索
	 * 
	 * @return
	 */
	public Select getSelect() {
		return this.select;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.sql.distribute.NamingObject#duplicate()
	 */
	@Override
	public Object duplicate() {
		return new FromPhase(this);
	}

}
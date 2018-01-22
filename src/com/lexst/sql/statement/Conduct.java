/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com. All rights reserved
 * 
 * conduct command
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 6/12/2011
 * 
 * @see com.lexst.sql.statement
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.statement;

import com.lexst.sql.conduct.*;

/**
 * "diffuse/aggregate"算法的实现类，用于多数据、多层次的分布式计算。<br>
 * 在终端输入语句时，init、from、to三项是必须的，balance、collect两项为可选。<br>
 * init阶段是对后续阶段计算进行参数配置，<br>
 * from(diffuse)阶段实现原始数据生成(包括数据库检索数据和用户按照需求产生数据两种)，<br>
 * to(aggregate)阶段是来自上一级数据的整合，生成新的数据。在to阶段允许多次迭化，前一阶段的数据输出是后一阶段性数据输入<br> 
 * balance阶段对from、to阶段产生的数据进行平衡分配。<br>
 * collect阶段对最后to阶段产生的数据流进行整理、显示。<br><br>
 * 
 * conduct非SQL标准，是自定义的语法结构。<br><br>
 * 
 * 格式:<br>
 * CONDUCT <br>
 * 		INIT naming:[init-task], selfkey1(type)=value1, selfkey2(raw)=0x[0-9a-f], selfkey3(type)='value2'..." <br>
 * 		FROM naming:[diffuse-task], blocks:[column-name%digit], sites:[digit], query:"select * from * where *", selfkey1(type)=value1, selfkey2(type)='value2'..." <br>
 * 		BALANCE naming:[balance-task], selfkey1(type)=value1, selfkey2(type)='value2'..." <br>
 * 		TO naming:[aggregate-task], sites:[digit], selfkey1(type)=value1, selfkey2(type)='value2'... <br>
 *			SUBTO naming:[aggregate-task], selfkey(type)=value1, selfkey2(type)='value2'....  <br>
 * 		COLLECT naming:[collect-task], writeto:"[local file]", show:"[class-name]" <br>
 */
public class Conduct extends Compute {

	private static final long serialVersionUID = 5074189841970239108L;

	/** init naming 命名对象 (可选) */
	private InitObject init;

	/** from naming 命名对象 (diffuse) **/
	private FromObject from;

	/** 平衡计算接口 */
	private BalanceObject balance;

	/** to naming 命名对象 (aggreagate) **/
	private ToObject to;

	/** collect naming 命名对象(可选) */
	private CollectObject collect;

	/**
	 * default
	 */
	public Conduct() {
		super(Compute.CONDUCT_METHOD);
	}

	/**
	 * 复制对象
	 * 
	 * @param conduct
	 */
	public Conduct(Conduct conduct) {
		this();

		if (conduct.init != null) {
			this.init = (InitObject) conduct.init.clone();
		}
		if (conduct.collect != null) {
			this.collect = (CollectObject) conduct.collect.clone();
		}
		if (conduct.from != null) {
			this.from = (FromObject) conduct.from.clone();
		}
		if (conduct.to != null) {
			this.to = (ToObject) conduct.to.clone();
		}
		if (conduct.balance != null) {
			this.balance = (BalanceObject) conduct.balance.clone();
		}
	}

	/**
	 * 设置INIT命名接口
	 * 
	 * @param object
	 */
	public void setInit(InitObject object) {
		this.init = object;
	}

	/**
	 * 返回INIT命名接口
	 * 
	 * @return
	 */
	public InitObject getInit() {
		return this.init;
	}

	/**
	 * 设置FROM命名接口
	 * 
	 * @param object
	 */
	public void setFrom(FromObject object) {
		this.from = object;
	}

	/**
	 * 返回FROM命名接口
	 * 
	 * @return
	 */
	public FromObject getFrom() {
		return this.from;
	}

	/**
	 * 设置TO命名接口
	 * 
	 * @param object
	 */
	public void setTo(ToObject object) {
		this.to = object;
	}

	/**
	 * 返回TO命名接口
	 * 
	 * @return
	 */
	public ToObject getTo() {
		return this.to;
	}

	/**
	 * 设置COLLECT接口
	 * 
	 * @param object
	 */
	public void setCollect(CollectObject object) {
		this.collect = object;
	}

	/**
	 * 返回COLLECT接口
	 * 
	 * @return
	 */
	public CollectObject getCollect() {
		return this.collect;
	}

	/**
	 * 设置BALANCE命名接口
	 * 
	 * @param object
	 */
	public void setBalance(BalanceObject object) {
		this.balance = object;
	}

	/**
	 * 返回BALANCE命名接口
	 * 
	 * @return
	 */
	public BalanceObject getBalance() {
		return this.balance;
	}

	/*
	 * 克隆CONDUCT
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Conduct(this);
	}

}
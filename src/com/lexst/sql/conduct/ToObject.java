/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

import java.io.*;

import com.lexst.util.naming.*;

/**
 * diffuse/aggregate分布计算模型的aggregate阶段的SQL语法表述<br>
 * <br>
 * 该阶段发生在WORK节点，允许有一次或者多次计算(对应self-to，self-slave的关系)<br>
 * 最初的数据源自DATA节点，WORK节点从DATA节点上提取，在这里计算并且产生最终的结果。此后的数据将源自WORK节点<br>
 * <br>
 * 返回结果有两种:<br>
 * <1> 如果有"slave"，返回的是Area，即分布数据映射图。需要由CALL解析，然后交由slave继续处理。<br>
 * <2> 如果没有"slave"，返回的是计算结果数据。<br>
 * <br>
 */
public class ToObject implements Serializable, Cloneable {

	private static final long serialVersionUID = 9005313308888957743L;

	/** 命名对象 **/
	private Naming naming;

	/** 输入接口 */
	private ToInputObject input;

	/** 输出接口 */
	private ToOutputObject output;

	/**
	 * default
	 */
	public ToObject() {
		super();
	}

	/**
	 * 初始化对象并且设置aggregate阶段任务名称
	 * 
	 * @param name
	 */
	public ToObject(String name) {
		this();
		this.setNaming(name);
	}

	/**
	 * @param object
	 */
	public ToObject(ToObject object) {
		this();
		this.setNaming(object.naming);
		this.setInput(object.input);
		this.setOutput(object.output);
	}

	/**
	 * 设置任务命名。命名可以忽略大小写，必须在TOP集群唯一。
	 * 
	 * @param s
	 */
	public void setNaming(Naming s) {
		this.naming = (Naming) s.clone();
	}

	/**
	 * 设置命名
	 * 
	 * @param s
	 */
	public void setNaming(String s) {
		this.naming = new Naming(s);
	}
	
	/**
	 * 设置输入接口
	 * 
	 * @param i
	 */
	public void setInput(ToInputObject i) {
		this.input = i;
	}

	/**
	 * 返回输入接口
	 * 
	 * @return
	 */
	public ToInputObject getInput() {
		return this.input;
	}

	/**
	 * 设置输出接口
	 * 
	 * @param i
	 */
	public void setOutput(ToOutputObject i) {
		this.output = i;
	}

	/**
	 * 返回输出接口
	 * 
	 * @return
	 */
	public ToOutputObject getOutput() {
		return this.output;
	}

	/*
	 * 克隆对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new ToObject(this);
	}

	// /** 指定的WORK节点数，此参数通常由用户确定 **/
	// private int sites;
	//
	// /** WORK节点服务器地址集合 **/
	// private Set<SiteHost> nodes = new TreeSet<SiteHost>();
	//
	// /** ToObject是级联关系，从首ToObject开始，通过slave ToObject连接下去，完成迭化操作。参数从0开始 **/
	// private int focusIndex;
	//
	// /** 下一级"aggregate"操作对象 */
	// private ToObject slave;
	//
	// /**
	// * default
	// */
	// public ToObject() {
	// super();
	// this.sites = 0;
	// this.focusIndex = 0;
	// }
	//
	// /**
	// * 初始化对象并且设置aggregate阶段任务名称
	// * @param name
	// */
	// public ToObject(String name) {
	// this();
	// this.setNaming(name);
	// }
	//
	// /**
	// * @param object
	// */
	// public ToObject(ToObject object) {
	// super(object);
	// this.sites = object.sites;
	// this.focusIndex = object.focusIndex;
	// this.nodes.addAll(object.nodes);
	// if (object.slave != null) {
	// this.slave = new ToObject(object.slave);
	// }
	// }
	//
	// /**
	// * 指定WORK节点数量
	// * @param size
	// */
	// public void setSites(int size) {
	// this.sites = size;
	// }
	//
	// /**
	// * 返回WORK节点数量
	// * @return
	// */
	// public int getSites() {
	// return this.sites;
	// }
	//
	// /**
	// * 保存一个WORK节点址
	// * @param site
	// * @return
	// */
	// public boolean addSite(SiteHost site) {
	// return this.nodes.add(site);
	// }
	//
	// /**
	// * 保存一组WORK节点地址
	// *
	// * @param sites
	// * @return
	// */
	// public boolean addSites(Collection<SiteHost> sites) {
	// return this.nodes.addAll(sites);
	// }
	//
	// /**
	// * 删除一个WORK节点地址
	// * @param site
	// * @return
	// */
	// public boolean removeSite(SiteHost site) {
	// return this.nodes.remove(site);
	// }
	//
	// /**
	// * 返回WORK节点地址列表
	// * @return
	// */
	// public List<SiteHost> listSites() {
	// return new ArrayList<SiteHost>(nodes);
	// }
	//
	// /**
	// * 设置最后后的对象
	// * @param object
	// */
	// public void setLast(ToObject object) {
	// if(slave != null) {
	// slave.setLast(object);
	// } else {
	// slave = object;
	// }
	// }
	//
	// /**
	// * 是否有子级对象
	// * @return
	// */
	// public boolean hasNext() {
	// return this.slave != null;
	// }
	//
	// /**
	// * 返回子级ToObject对象
	// *
	// * @return
	// */
	// public ToObject next() {
	// return this.slave;
	// }
	//
	// /**
	// * 设置执行焦点下标(参数设置通常是首ToObject，子级不处理)
	// * @param i
	// */
	// public void setFocusIndex(int i) {
	// this.focusIndex = i;
	// }
	//
	// /**
	// * 返回当前执行焦点下标
	// * @return
	// */
	// public int getFocusIndex() {
	// return this.focusIndex;
	// }
	//
	// /**
	// * 焦点索引自增一级
	// */
	// public void incrementFocuIndex() {
	// this.focusIndex++;
	// }
	//
	// /**
	// * 从首节点开始计算，找到当前焦点状态的ToObject对象
	// * @return
	// */
	// public ToObject getFocusTo() {
	// if (this.focusIndex == 0) {
	// return this;
	// }
	// // 从子级递增查找
	// ToObject sub = this.next();
	// for (int index = 1; sub != null; index++) {
	// if (index == this.focusIndex) {
	// return sub;
	// }
	// sub = sub.next();
	// }
	// return null;
	// }
	//
	// /*
	// * 复制ToObject对象
	// *
	// * @see com.lexst.sql.statement.dc.NamingObject#duplicate()
	// */
	// @Override
	// public Object duplicate() {
	// return new ToObject(this);
	// }

}
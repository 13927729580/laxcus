/**
 * @email admin@wigres.com
 *
 */
package com.lexst.sql.conduct;

import java.io.*;

import com.lexst.util.naming.*;

/**
 * diffuse/aggregate分布计算模型的diffuse阶段SQL语法表述。<br>
 * <br>
 * diffuse位于DATA节点，是数据产生阶段。<br>
 * 数据产生有两种:<br>
 * <1> 根据SQL SELECT语句，从数据存储层提取 (允许多个SELECT同时进行)。<br>
 * <2> 根据用户定义参数，从对应的命名接口中产生(用户自定义参数由用户自己解析)。<br>
 * 
 */
public class FromObject implements Serializable, Cloneable {

	private static final long serialVersionUID = 3972374214867206750L;

	/** 命名对象 **/
	private Naming naming;

	/** 输入接口 */
	private FromInputObject input;

	/** 输出接口 */
	private FromOutputObject output;

	/**
	 * default
	 */
	public FromObject() {
		super();
	}

	/**
	 * 初始化对象并且设置diffuse阶段任务名称
	 * 
	 * @param naming
	 */
	public FromObject(String naming) {
		this();
		this.setNaming(naming);
	}

	/**
	 * 复制对象
	 * 
	 * @param object
	 */
	public FromObject(FromObject object) {
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
	public void setInput(FromInputObject i) {
		this.input = i;
	}

	/**
	 * 返回输入接口
	 * 
	 * @return
	 */
	public FromInputObject getInput() {
		return this.input;
	}

	/**
	 * 设置输出接口
	 * 
	 * @param i
	 */
	public void setOutput(FromOutputObject i) {
		this.output = i;
	}

	/**
	 * 返回输出接口
	 * 
	 * @return
	 */
	public FromOutputObject getOutput() {
		return this.output;
	}

	/*
	 * 克隆 FromObject对象
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new FromObject(this);
	}

	// /** 系统分配的序列值 **/
	// private int index;
	//
	// /** 指定的DATA节点数，默认是0不指定。此参数一般在命名时定义 **/
	// private int sites;
	//
	// /** DATA节点服务器地址集合 **/
	// private Set<SiteHost> nodes = new TreeSet<SiteHost>();
	//
	// /** 被检索的SQL SELECT集合(允许一次"diffuse"操作分别执行多个SELECT) */
	// private List<Select> array = new ArrayList<Select>(3);
	//
	// /**
	// * default
	// */
	// public FromObject() {
	// super();
	// this.index = 0;
	// this.sites = 0;
	// }
	//
	// /**
	// * 初始化对象并且设置diffuse阶段任务名称
	// *
	// * @param naming
	// */
	// public FromObject(String naming) {
	// this();
	// this.setNaming(naming);
	// }
	//
	// /**
	// * 复制对象
	// * @param object
	// */
	// public FromObject(FromObject object) {
	// super(object);
	// this.index = object.index;
	// this.sites = object.sites;
	// this.nodes.addAll(object.nodes);
	// this.array.addAll(object.array);
	// }
	//
	// /**
	// * 设置序列号 (序列号由CALL节点在运行时给出)
	// * @param i
	// */
	// public void setIndex(int i) {
	// this.index = i;
	// }
	//
	// /**
	// * 返回序列号
	// * @return
	// */
	// public int getIndex() {
	// return this.index;
	// }
	//
	// /**
	// * 指定DATA节点数
	// * @param i
	// */
	// public void setSites(int i) {
	// this.sites = i;
	// }
	//
	// /**
	// * 返回DATA节点数
	// * @return
	// */
	// public int getSites() {
	// return this.sites;
	// }
	//
	// /**
	// * 保存一个DATA节点地址
	// * @param site
	// * @return
	// */
	// public boolean addSite(SiteHost site) {
	// return this.nodes.add(site);
	// }
	//
	// /**
	// * 保存一组DATA节点地址
	// * @param sites
	// * @return
	// */
	// public boolean addSites(Collection<SiteHost> sites) {
	// return this.nodes.addAll(sites);
	// }
	//
	// /**
	// * 删除一个DATA节点地址
	// * @param site
	// * @return
	// */
	// public boolean removeSite(SiteHost site) {
	// return this.nodes.remove(site);
	// }
	//
	// /**
	// * 取DATA节点地址列表
	// *
	// * @return
	// */
	// public List<SiteHost> listSites() {
	// return new ArrayList<SiteHost>(this.nodes);
	// }
	//
	// /**
	// * 保存一个SELECT命令
	// * @param select
	// * @return
	// */
	// public boolean addSelect(Select select) {
	// return this.array.add(select);
	// }
	//
	// /**
	// * 返回SELECT集合
	// * @return
	// */
	// public List<Select> getSelects() {
	// return this.array;
	// }
	//
	// /**
	// * 返回SELECT的表名集合
	// * @return
	// */
	// public List<Space> getSelectSpaces() {
	// Set<Space> set = new TreeSet<Space>();
	// for (Select select : array) {
	// set.add(select.getSpace());
	// }
	// return new ArrayList<Space>(set);
	// }
	//
	// /**
	// * 返回指定下标的"SQL SELECT"
	// * @param index
	// * @return
	// */
	// public Select getSelect(int index) {
	// if(index < 0 || index >= this.array.size()) {
	// return null;
	// }
	// return this.array.get(index);
	// }
	//
	// /**
	// * 清除全部"SQL SELECT"
	// */
	// public void clearSelects() {
	// this.array.clear();
	// }
	//
	// /**
	// * 统计"SQL SELECT"成员数
	// * @return
	// */
	// public int countSelect() {
	// return this.array.size();
	// }
	//
	// /*
	// * 复制 FromObject对象
	// * @see com.lexst.sql.statement.dc.NamingObject#duplicate()
	// */
	// @Override
	// public Object duplicate() {
	// return new FromObject(this);
	// }

}
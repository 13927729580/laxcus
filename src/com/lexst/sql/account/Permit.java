/**
 *
 */
package com.lexst.sql.account;

import java.io.Serializable;
import java.util.*;

import org.w3c.dom.Element;

/**
 * 权限表定义，从高到低分为三级：用户级、数据库表、数据库表级。<br>
 * 上级可以拥有下级的操作权限，但是下级不能拥有上级的操作。<br>
 * 比如:用户级能够执行属于数据库表的SELECT的操作，但是数据库表不能执行数据库的CREATE SCHEMA操作。<br><br>
 * 
 * 另：数据库管理员(DBA)默认拥有所有操作权限，普通账号用户的权限来自数据库管理员的授权。<br>
 * 每一种操作都是独立的，不存在兼容的问题。即如果允许建表(CREATE SCHEMA)，不等于可以查询(SELECT)。可以查询(SELECT)，不等于可以删除(DELETE)。
 */
public abstract class Permit implements Serializable {
	
	private static final long serialVersionUID = 7771684235463244846L;

	/** 权限级别，等级排列从高到低依次是:USER、SCHEMA、TABLE */
	public final static int USER_PERMIT = 1;

	public final static int SCHEMA_PERMIT = 2;

	public final static int TABLE_PERMIT = 3;

	/** 当前操作权限类型 **/
	private int priority;

	/** 注册用户账号集合 **/
	private Set<User> users = new TreeSet<User>();

	/**
	 * 初始化
	 */
	protected Permit() {
		super();
		priority = 0;
	}

	/**
	 * 初始化并且设置优先级
	 * @param priority
	 */
	protected Permit(int priority) {
		this();
		this.setPriority(priority);
	}

	/**
	 * 设置权限分类标识
	 * 
	 * @param i
	 */
	public void setPriority(int i) {
		if (i != Permit.USER_PERMIT && i != Permit.SCHEMA_PERMIT && i != Permit.TABLE_PERMIT) {
			throw new java.lang.IllegalArgumentException("invalid permit family!");
		}
		this.priority = i;
	}

	/**
	 * 返回权限分类标识
	 * 
	 * @return
	 */
	public int getPriority() {
		return this.priority;
	}
	
	/**
	 * 账号级操作权限
	 * @return
	 */
	public boolean isUserPermit() {
		return this.priority == Permit.USER_PERMIT;
	}

	/**
	 * 数据库级操作权限
	 * @return
	 */
	public boolean isSchemaPrimit() {
		return this.priority == Permit.SCHEMA_PERMIT;
	}

	/**
	 * 数据库表级操作权限
	 * @return
	 */
	public boolean isTablePermit() {
		return this.priority == Permit.TABLE_PERMIT;
	}

	/**
	 * 增加一个用户账号
	 * @param username
	 * @return
	 */
	public boolean addUser(String username) {
		if (username == null || username.trim().isEmpty()) {
			return false;
		}

		User user = new User(username);
		if (users.contains(user)) {
			return false;
		}
		return users.add(user);
	}

	/**
	 * 设置一组用户账号
	 * @param usernames
	 * @return
	 */
	public int setUsers(String[] usernames) {
		int size = this.users.size();
		for (int i = 0; usernames != null && i < usernames.length; i++) {
			addUser(usernames[i]);
		}
		return this.users.size() - size;
	}

	/**
	 * 返回账号名称集合
	 * @return
	 */
	public List<String> getUsers() {
		ArrayList<String> a = new ArrayList<String>();
		for (User user : users) {
			a.add(user.getHexUsername());
		}
		return a;
	}

	/**
	 * 增加某个级别的操作权限
	 * @param permit
	 * @return
	 */
	public abstract boolean add(Permit permit);

	/**
	 * 删除某个级别的部分或者全部操作权限
	 * @param permit
	 * @return
	 */
	public abstract boolean remove(Permit permit);

	/**
	 * 集合是否空
	 * @return
	 */
	public abstract boolean isEmpty();

	/**
	 * 是否允许某类种操作
	 * @param id
	 * @return
	 */
	public abstract boolean isAllow(int id);

	/**
	 * 生成XML文档
	 * @return
	 */
	public abstract String buildXML();

	/**
	 * 解析XML文档转成类中参数
	 * @param elem
	 * @return
	 */
	public abstract boolean parseXML(Element elem);
}

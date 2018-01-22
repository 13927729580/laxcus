/**
 *
 */
package com.lexst.sql.account;

/**
 * 
 * 普通用户账号，参数包括SHA1散列的用户名和密码，以及可用的最大磁盘空间(默认是0，可以任意使用已知的磁盘空间)
 *
 */
public class User extends SHA1User implements Cloneable {

	private static final long serialVersionUID = 6021083807008309680L;

	/** 允许的最大可用磁盘空间尺寸。默认是0，不限制  **/
	private long maxsize;

	/**
	 * default
	 */
	public User() {
		super();
		maxsize = 0L;
	}

	/**
	 * 复制用户账号
	 * 
	 * @param user
	 */
	public User(User user) {
		super(user);
		this.setMaxSize(user.maxsize);
	}

	/**
	 * @param username
	 */
	public User(String username) {
		this();
		setTextUsername(username);
	}

	/**
	 * @param username
	 * @param password
	 */
	public User(String username, String password) {
		this();
		this.setTextUsername(username);
		this.setTextPassword(password);
	}

	/**
	 * 复制用户账号
	 * @param user
	 */
	public void set(User user) {
		super.set(user);
		this.setMaxSize(user.maxsize);
	}

	/**
	 * 设置可使用最大空间
	 * @param i
	 */
	public void setMaxSize(long i) {
		this.maxsize = i;
	}

	/**
	 * 返回最大空间
	 * @return
	 */
	public long getMaxSize() {
		return this.maxsize;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new User(this);
	}
}
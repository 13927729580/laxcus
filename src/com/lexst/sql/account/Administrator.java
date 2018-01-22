/**
 *
 */
package com.lexst.sql.account;

/**
 * 数据库管理员账号。<br>
 * 管理员账号文件配置在TOP节点的local.xml中指定，也是经过SHA1散列后的密文。<br>
 * 明文数据只能是管理员账号建立者拥有，建立管理员账号的程序在附件包中。<br>
 */
public class Administrator extends SHA1User implements Cloneable {

	private static final long serialVersionUID = 2078997523043571126L;

	/**
	 * default
	 */
	public Administrator() {
		super();
	}

	/**
	 * @param admin
	 */
	public Administrator(Administrator admin) {
		super(admin);
	}

	/**
	 * @param username
	 * @param password
	 */
	public Administrator(String username, String password) {
		this();
		super.setTextUsername(username);
		super.setTextPassword(password);
	}

//	/**
//	 * 检查是否匹配
//	 * 
//	 * @param username
//	 * @param password
//	 * @return
//	 */
//	public boolean match(String username, String password) {
//		Administrator admin = new Administrator(username, password);
//		return this.compareTo(admin) == 0;
//	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Administrator(this);
	}
}
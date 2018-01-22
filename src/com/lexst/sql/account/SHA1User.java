/**
 * 
 */
package com.lexst.sql.account;

import java.io.*;
import java.security.*;
import java.util.*;

/**
 * 
 * 用户登录账号(用户名和密码)<br>
 * <br>
 * 特别注意: <br>
 * <1> 用户的账号明文由用户在其它环境中持有，这里不保存。<br>
 * <2> 系统所有环节中，账号的用户名和密码是SHA1算法散列后的字节数组，存储是字节的16进制字符串。<br><br>
 * 
 * 这种方式保证即使用户账号被窃取，窃密者也不可能使用逆向方式获得账号原文。<br>
 * 加上在登录开始需要RSA校验，而RSA证书是由管理员签发，<br>
 * 用户只有登录成功才能进行操作，这一系列流程保证用户账号和基于账号操作的安全性。<br>
 */
class SHA1User implements Serializable, Comparable<SHA1User> {

	private static final long serialVersionUID = 6775997897261604349L;

	/** 账号用户名(SHA1码，20个字节) **/
	private byte[] username;
	
	/** 账号用户密码(SHA1码，20个字节) **/
	private byte[] password;

	/** 哈希码 */
	private int hash;
	
	/**
	 * default
	 */
	protected SHA1User() {
		super();
		hash = 0;
	}
	
	/**
	 * 复制
	 * @param user
	 */
	protected SHA1User(SHA1User user) {
		this();
		this.set(user);
	}

	/**
	 * @param user
	 */
	protected void set(SHA1User user) {
		this.setUsername(user.username);
		this.setPassword(user.password);
		this.hash = user.hash;
	}

	/**
	 * 字节转成十六进制字符串
	 * 
	 * @param b
	 * @return
	 */
	private String itoh(byte[] b) {
		StringBuilder hex = new StringBuilder();
		for (int i = 0; b != null && i < b.length; i++) {
			String s = String.format("%X", b[i] & 0xFF);
			if (s.length() == 1) hex.append('0');
			hex.append(s);
		}
		return hex.toString();
	}
	
	/**
	 * 十六进制字符串转成字节
	 * 
	 * @param hex
	 */
	private byte[] htoi(String hex) {
		if (hex == null || hex.length() != 40) {
			throw new IllegalArgumentException("invalid sha1 code");
		}

		byte[] b = new byte[20];
		for (int i = 0, n = 0; i < hex.length(); i += 2) {
			String s = hex.substring(i, i + 2);
			b[n++] = (byte) Integer.parseInt(s, 16);
		}
		return b;
	}
	
	/**
	 * 将字符串转成SHA1字节码
	 * 
	 * @param s
	 * @return
	 */
	private byte[] digest(String s) {
		try {
			MessageDigest sha = MessageDigest.getInstance("SHA-1");
			sha.update(s.getBytes());
			return sha.digest();
		} catch (NoSuchAlgorithmException exp) {
			exp.printStackTrace();
		}
		return null;
	}

	/**
	 * 设置用户名(20字节)
	 * 
	 * @param b
	 */
	protected void setUsername(byte[] b) {
		if (b != null && b.length == 20) {
			this.username = Arrays.copyOfRange(b, 0, b.length);
		}
	}

	/**
	 * 设置密码(20个字节)
	 * 
	 * @param b
	 */
	protected void setPassword(byte[] b) {
		if (b != null && b.length == 20) {
			this.password = Arrays.copyOfRange(b, 0, b.length);
		}
	}
	
	/**
	 * 返回SHA1编码的用户名
	 * @return
	 */
	public byte[] getUsername() {
		return this.username;
	}
	
	/**
	 * 返回SHA1编码的密码
	 * @return
	 */
	public byte[] getPassword() {
		return this.password;
	}

	/**
	 * 设置用户名(转化为小写后生成SHA1字节流)
	 * 
	 * @param s
	 */
	public void setTextUsername(String s) {
		byte[] b = digest(s.toLowerCase());
		setUsername(b);
	}

	/**
	 * 设置用户名(16进制字符串)
	 * @param hex
	 */
	public void setHexUsername(String hex) {
		this.setUsername(this.htoi(hex));
	}
	
	/**
	 * 返回16进制的字符串
	 * @return
	 */
	public String getHexUsername() {
		return itoh(username);
	}

	/**
	 * 设置密码(生成SHA1字节流保存)
	 * @param text
	 */
	public void setTextPassword(String text) {
		byte[] b = digest(text);
		setPassword(b);
	}

	/**
	 * set password (hex string)
	 * @param hex
	 */
	public void setHexPassword(String hex) {
		this.setPassword(this.htoi(hex));
	}

	/**
	 * 返回SHA1的密码字符串
	 * @return
	 */
	public String getHexPassword() {
		return itoh(password);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof SHA1User)) {
			return false;
		} else if (object == this) {
			return true;
		}
		
		return this.compareTo((SHA1User) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == 0) {
			hash = Arrays.hashCode(username);
		}
		return hash;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s:%s", getHexUsername(), getHexPassword());
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SHA1User user) {
		if (username == null) {
			return -1;
		} else if (user == null || user.username == null) {
			return 1;
		}

		if (username.length != user.username.length) {
			return username.length < user.username.length ? -1 : 1;
		}

		for (int i = 0; i < username.length; i++) {
			if (username[i] < user.username[i]) return -1;
			else if (username[i] > user.username[i])return 1;
		}
		return 0;
	}

}
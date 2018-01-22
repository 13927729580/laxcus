/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 lexst.com. All rights reserved
 * 
 * fixp command (request and response, head information)
 * 
 * @author scott.liang
 * 
 * @version 1.0 10/7/2011
 * 
 * @see com.lexst.fixp
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.fixp;

import java.io.Serializable;
import java.util.*;

import com.lexst.security.*;

public class Cipher implements Serializable {

	private static final long serialVersionUID = 5187687106854261113L;

	/** 算法名称列表 */
	public final static int DES = 1;

	public final static int DES3 = 2;
	
	public final static int AES = 3;
	
	public final static int BLOWFISH = 4;

	public final static int MD5 = 5;

	public final static int SHA1 = 6;

	/** 算法名称 algorithm name */
	private int algo;

	/** 密码 */
	private byte[] password;

	/* cipher hashcode */
	private int hash;
	
	/**
	 * 
	 */
	public Cipher() {
		super();
		algo = 0;
		hash = 0;
	}
	
	/**
	 * @param algo
	 * @param pwd
	 */
	public Cipher(int algo, byte[] pwd) {
		this();
		this.set(algo, pwd);
	}
	
	/**
	 * @param algo
	 * @param pwd
	 */
	public Cipher(String algo, byte[] pwd) {
		this();
		int num = Cipher.translate(algo);
		if (num == -1) {
			throw new IllegalArgumentException("invalid algorithm " + algo);
		}
		this.set(num, pwd);
	}

	/**
	 * @param cipher
	 */
	public Cipher(Cipher cipher) {
		this();
		this.set(cipher.algo, cipher.password);
		this.hash = cipher.hash;
	}

	/**
	 * @param algo
	 * @param pwd
	 */
	public void set(int algo, byte[] pwd) {
		this.setAlgorithm(algo);
		this.setPassword(pwd);
	}

	public void setAlgorithm(int value) {
		if (!(Cipher.DES <= value && value <= Cipher.SHA1)) {
			throw new IllegalArgumentException("invalid algorithm value: " + value);
		}
		this.algo = value;
	}

	public int getAlgorithm() {
		return this.algo;
	}
	
	public String getAlgorithmText() {
		return Cipher.translate(algo);
	}
	
	/**
	 * @param name
	 * @return
	 */
	public static String translate(int name) {
		switch (name) {
		case Cipher.DES:
			return "DES";
		case Cipher.DES3:
			return "DES3";
		case Cipher.AES:
			return "AES";
		case Cipher.BLOWFISH:
			return "Blowfish";
		case Cipher.MD5:
			return "MD5";
		case Cipher.SHA1:
			return "SHA1";
		}
		return null;
	}

	/**
	 * translate to number 
	 * @param name
	 * @return
	 */
	public static int translate(String name) {
		if ("DES".equalsIgnoreCase(name)) {
			return Cipher.DES;
		} else if ("DES3".equalsIgnoreCase(name) || "3DES".equalsIgnoreCase(name)) {
			return Cipher.DES3;
		} else if("AES".equalsIgnoreCase(name)) {
			return Cipher.AES;
		} else if("Blowfish".equalsIgnoreCase(name)) {
			return Cipher.BLOWFISH;
		} else if ("MD5".equalsIgnoreCase(name)) {
			return Cipher.MD5;
		} else if ("SHA1".equalsIgnoreCase(name)) {
			return Cipher.SHA1;
		}
		return -1;
	}

	public void setPassword(byte[] b) {
		password = new byte[b.length];
		System.arraycopy(b, 0, password, 0, b.length);
	}

	public byte[] getPassword() {
		return password;
	}
	
	/**
	 * 加密数据
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public byte[] encrypt(byte[] b, int off, int len) throws SecureException {
		byte[] raws = null;
		switch (this.algo) {
		case Cipher.DES:
			raws = SecureEncryptor.des(password, b, off, len);
			break;
		case Cipher.DES3:
			raws = SecureEncryptor.des3(password, b, off, len);
			break;
		case Cipher.AES:
			raws = SecureEncryptor.aes(password, b, off, len);
			break;
		case Cipher.BLOWFISH:
			raws = SecureEncryptor.blowfish(password, b, off, len);
			break;
		case Cipher.MD5:
			raws = SecureEncryptor.md5(b, off, len);
			break;
		case Cipher.SHA1:
			raws = SecureEncryptor.sha1(b, off, len);
			break;
		default:
			raws = Arrays.copyOfRange(b, off, off + len);
			break;
		}
		return raws;
	}

	/**
	 * 解密数据
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public byte[] decrypt(byte[] b, int off, int len) throws SecureException {
		byte[] raws = null;
		switch (this.algo) {
		case Cipher.DES:
			raws = SecureDecryptor.des(password, b, off, len);
			break;
		case Cipher.DES3:
			raws = SecureDecryptor.des3(password, b, off, len);
			break;
		case Cipher.AES:
			raws = SecureDecryptor.aes(password, b, off, len);
			break;
		case Cipher.BLOWFISH:
			raws = SecureDecryptor.blowfish(password, b, off, len);
			break;
		case Cipher.MD5:
			raws = SecureDecryptor.md5(b, off, len);
			break;
		case Cipher.SHA1:
			raws = SecureDecryptor.sha1(b, off, len);
			break;
		default:
			raws = Arrays.copyOfRange(b, off, off + len);
			break;
		}
		return raws;
	}

	/*
	 * 判断是否一致
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof Cipher)) {
			return false;
		} else if (object == this) {
			return true;
		}

		Cipher cipher = (Cipher) object;
		return algo == cipher.algo && Arrays.equals(password, cipher.password);
	}

	/*
	 * 返回哈希码
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == 0) {
			if (algo != 0 && password != null) {
				hash = algo ^ Arrays.hashCode(password);
			}
		}
		return hash;
	}

}
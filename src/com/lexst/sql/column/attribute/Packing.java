/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.column.attribute;

import java.io.*;

import com.lexst.util.*;

/**
 * @author scott.liang
 *
 */
public class Packing implements Serializable {

	private static final long serialVersionUID = 3860684003587922405L;

	/** 压缩算法 **/
	public final static int GZIP = 0x1;
	public final static int ZIP = 0x2;

	/** 加密算法 (des, des3, aes, blowfish) **/
	public final static byte DES = 0x1;
	public final static byte DES3 = 0x2;
	public final static byte AES = 0x3;
	public final static byte BLOWFISH = 0x4;
	
	/** 打包标记(包含压缩和加密两种方式，允许同时存在)，格式: 压缩|加密。默认是0，不打包 **/
	private int packing;
	/** 加密算法密码 **/
	private byte[] password;

	/**
	 * 数据打包参数
	 */
	public Packing() {
		super();
		this.packing = 0;
	}
	
	/**
	 * 数据打包
	 * @param compress
	 * @param encrypt
	 * @param password
	 */
	public Packing(int compress, int encrypt, byte[] password) {
		this();
		this.setPacking(compress, encrypt, password);
	}
	
	/**
	 * @param packing
	 */
	public Packing(Packing packing) {
		this();
		this.set(packing);
	}

	/**
	 * 设置打包参数(压缩算法、密码算法、密码)
	 * @param packing
	 */
	public void set(Packing packing) {
		this.packing = packing.packing;
		this.setEncryptPassword(packing.password);
	}

	
	/**
	 * 打包参数
	 * @param compress
	 * @param encrypt
	 */
	public void setPacking(int compress, int encrypt, byte[] pwd) {
		// 压缩算法和加密算法标识合并为一个值
		this.packing = ((compress & 0xFF) << 8) | (encrypt & 0xFF);
		// 加密算法的密码
		if (encrypt == 0) {
			password = null;
		} else {
			this.setEncryptPassword(pwd, 0, pwd.length);
		}
	}
	
	/**
	 * 打包参数
	 * @param packing
	 * @param pwd
	 */
	public void setPacking(int packing, byte[] pwd) {
		this.packing = packing;
		this.setEncryptPassword(pwd);
	}
	
	/**
	 * 压缩算法
	 * @param packing
	 * @return
	 */
	public static int getCompress(int packing) {
		return (packing >> 8) & 0xFF;
	}

	/**
	 * 加密算法
	 * @param packing
	 * @return
	 */
	public static int getEncrypt(int packing) {
		return packing & 0xFF;
	}

	/**
	 * 压缩算法
	 * @return
	 */
	public int getCompress() {
		return Packing.getCompress(this.packing);
	}

	/**
	 * 加密算法
	 * @return
	 */
	public int getEncrypt() {
		return Packing.getEncrypt(this.packing);
	}

	/**
	 * 是否打包
	 * @return
	 */
	public boolean isEnabled() {
		return this.packing != 0;
	}

	/**
	 * 加密算法密码
	 * 
	 * @param b
	 * @param off
	 * @param len
	 */
	public void setEncryptPassword(byte[] b, int off, int len) {
		if (b == null || len < 1) {
			password = null;
		} else {
			password = new byte[len];
			System.arraycopy(b, off, password, 0, len);
		}
	}
	
	/**
	 * 保存加密密码
	 * 
	 * @param b
	 */
	public void setEncryptPassword(byte[] b) {
		this.setEncryptPassword(b, 0, (b == null ? 0 : b.length));
	}

	/**
	 * 加密算法密码
	 * 
	 * @return
	 */
	public byte[] getEncryptPassword() {
		return this.password;
	}

	/**
	 * 生成打包域数据
	 * 
	 * @return
	 */
	public byte[] build() {
		int seek = 0;
		int size = (password == null ? 0 : password.length);
		byte[] buff = new byte[8 + size];
		
		// 打包标记(压缩,加密)
		byte[] b = Numeric.toBytes(this.packing);
		System.arraycopy(b, 0, buff, seek, b.length);
		seek += b.length;

		// 密码长度
		b = Numeric.toBytes(size);
		System.arraycopy(b, 0, buff, seek, b.length);
		seek += b.length;
		// 加密密码
		if (size > 0) {
			System.arraycopy(password, 0, buff, seek, size);
			seek += size;
		}
		return buff;
	}
	
	/**
	 * 解析打包域数据
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		if (seek + 8 > end) {
			throw new ColumnAttributeResolveException("packing sizeout! %d,4,%d", seek, end);
		}

		this.packing = Numeric.toInteger(b, seek, 4);
		seek += 4;
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
			if (seek + size > end) {
				throw new ColumnAttributeResolveException("packing sizeout! %d,%d,%d", seek, size, end);
			}
			this.setEncryptPassword(b, seek, size);
			seek += size;
		}
		return seek - off;
	}
	
}
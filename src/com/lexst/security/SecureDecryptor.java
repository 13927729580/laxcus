/**
 * 
 */
package com.lexst.security;

import java.security.*;
import java.security.interfaces.*;
import javax.crypto.*;

/**
 * 
 * 安全解密器
 *
 */
public class SecureDecryptor extends SecureGenerator {

	/**
	 * default constructor
	 */
	public SecureDecryptor() {
		super();
	}

	/**
	 * RSA解密数据流
	 * 
	 * @param key
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * @throws SecureException
	 */
	public static byte[] rsa(RSAPrivateKey key, byte[] data, int off, int len) throws SecureException {
		try {
			Cipher cipher = Cipher.getInstance("RSA");
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(data, off, len);
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		} catch (NoSuchPaddingException e) {
			throw new SecureException(e);
		} catch (InvalidKeyException e) {
			throw new SecureException(e);
		} catch (IllegalBlockSizeException e) {
			throw new SecureException(e);
		} catch (BadPaddingException e) {
			throw new SecureException(e);
		}
	}
	
	/**
	 * 产生AES解密数据流
	 * 
	 * @param pwd
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * @throws SecureException
	 */
	public static byte[] aes(byte[] pwd, byte[] data, int off, int len) throws SecureException {
		SecretKey key = SecureGenerator.buildAESKey(pwd);
		try {
			Cipher cipher = Cipher.getInstance(SecureGenerator.AES_ALGO);
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(data, off, len);
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		} catch (NoSuchPaddingException exp) {
			throw new SecureException(exp);
		} catch (InvalidKeyException exp) {
			throw new SecureException(exp);
		} catch (BadPaddingException exp) {
			throw new SecureException(exp);
		} catch (IllegalBlockSizeException exp) {
			throw new SecureException(exp);
		}
	}
	
	/**
	 * 产生DES解密数据流
	 * 
	 * @param pwd
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * @throws SecureException
	 */
	public static byte[] des(byte[] pwd, byte[] data, int off, int len) throws SecureException {
		SecretKey key = SecureGenerator.buildDESKey(pwd);
		try {
			Cipher cipher = Cipher.getInstance(SecureGenerator.DES_ALGO);
			cipher.init(Cipher.DECRYPT_MODE, key);	
			return cipher.doFinal(data, off, len);
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		} catch (NoSuchPaddingException exp) {
			throw new SecureException(exp);
		} catch (InvalidKeyException exp) {
			throw new SecureException(exp);
		} catch (BadPaddingException exp) {
			throw new SecureException(exp);
		} catch (IllegalBlockSizeException exp) {
			throw new SecureException(exp);
		}
	}
	
	/**
	 * 产生DES3解密数据流
	 * 
	 * @param pwd
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] des3(byte[] pwd, byte[] data, int off, int len) throws SecureException {
		SecretKey key = SecureGenerator.buildDES3Key(pwd);
		try {
			Cipher cipher = Cipher.getInstance(SecureGenerator.DES3_ALGO); // "DESede");
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(data, off, len);
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		} catch (NoSuchPaddingException exp) {
			throw new SecureException(exp);
		} catch (InvalidKeyException exp) {
			throw new SecureException(exp);
		} catch (BadPaddingException exp) {
			throw new SecureException(exp);
		} catch (IllegalBlockSizeException exp) {
			throw new SecureException(exp);
		}
	}
	
	/**
	 * FLOWFISH算法解密数据流
	 * 
	 * @param pwd
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * @throws SecureException
	 */
	public static byte[] blowfish(byte[] pwd, byte[] data, int off, int len) throws SecureException {
		SecretKey key = SecureGenerator.buildBlowfishKey(pwd);
		try {
			Cipher cipher = Cipher.getInstance("Blowfish");
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(data, off, len);
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		} catch (NoSuchPaddingException exp) {
			throw new SecureException(exp);
		} catch (InvalidKeyException exp) {
			throw new SecureException(exp);
		} catch (BadPaddingException exp) {
			throw new SecureException(exp);
		} catch (IllegalBlockSizeException exp) {
			throw new SecureException(exp);
		}
	}

	/**
	 * 去掉MD5的签名，返回MD5数据流
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] md5(byte[] data, int off, int len) throws SecureException {
		if (len <= 16) {
			throw new SecureException("MD5 <= 16");
		}
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(data, off + 16, len - 16);
			byte[] hash = md.digest();

			for (int j = 0, i = off; j < hash.length; j++) {
				if (hash[j] != data[i++]) return null;
			}

			byte[] raw = new byte[len - 16];
			System.arraycopy(data, off + 16, raw, 0, raw.length);
			return raw;
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		}
	}

	/**
	 * 去掉SHA1签名，返回数据流
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 */
	public static byte[] sha1(byte[] data, int off, int len) throws SecureException {
		if (len <= 20) {
			throw new SecureException("SHA1 <= 20");
		}
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(data, off + 20, len - 20);
			byte[] hash = md.digest();

			for (int j = 0, i = off; j < hash.length; j++) {
				if (hash[j] != data[i++]) return null;
			}

			byte[] raw = new byte[len - 20];
			System.arraycopy(data, off + 20, raw, 0, raw.length);
			return raw;
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		}
	}

}
/**
 * 
 */
package com.lexst.security;

import java.io.*;
import java.security.*;
import java.security.interfaces.*;
import javax.crypto.*;

/**
 * 安全加密器
 *
 */
public class SecureEncryptor extends SecureGenerator {

	/**
	 * default constructor
	 */
	public SecureEncryptor() {
		super();
	}

	/**
	 * 生成RSA签名
	 * 
	 * @param key
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * 
	 * @throws SecureException
	 */
	public static byte[] rsa(RSAPublicKey key, byte[] data, int off, int len) throws SecureException {
		try {
			Cipher cipher = Cipher.getInstance("RSA");
			cipher.init(Cipher.ENCRYPT_MODE, key);
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
	 * 生成AES加密数据流
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
			cipher.init(Cipher.ENCRYPT_MODE, key);
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
	 * 生成DES加密数据流
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
			cipher.init(Cipher.ENCRYPT_MODE, key);
			
			ByteArrayOutputStream buff = new ByteArrayOutputStream(len - len % 8 + 8);
			CipherOutputStream cos = new CipherOutputStream(buff, cipher);
			cos.write(data, off, len);
			cos.flush();
			cos.close();
			return buff.toByteArray();
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		} catch (NoSuchPaddingException exp) {
			throw new SecureException(exp);
		} catch (InvalidKeyException exp) {
			throw new SecureException(exp);
		} catch (IOException exp) {
			throw new SecureException(exp);
		}
	}
	
	/**
	 * 生成DES3加密数据流
	 * @param pwd
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * @throws SecureException
	 */
	public static byte[] des3(byte[] pwd, byte[] data, int off, int len) throws SecureException {
		SecretKey key = SecureGenerator.buildDES3Key(pwd);
		try {
			Cipher cipher = Cipher.getInstance(SecureGenerator.DES3_ALGO);
			cipher.init(Cipher.ENCRYPT_MODE, key);

			ByteArrayOutputStream buff = new ByteArrayOutputStream(len - len % 8 + 8);
			CipherOutputStream cos = new CipherOutputStream(buff, cipher);
			cos.write(data, off, len);
			cos.flush();
			cos.close();
			return buff.toByteArray();
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		} catch (NoSuchPaddingException exp) {
			throw new SecureException(exp);
		} catch (InvalidKeyException exp) {
			throw new SecureException(exp);
		} catch (IOException exp) {
			throw new SecureException(exp);
		}
	}
	
	/**
	 * 生成FLOWFISH加密数据流
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
			cipher.init(Cipher.ENCRYPT_MODE, key);
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
	 * 生成MD5数据流，格式:MD5签名(16字节)+数据
	 * 
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * @throws SecureException
	 */
	public static byte[] md5(byte[] data, int off, int len) throws SecureException {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(data, off, len);
			byte[] hash = md.digest();
			
			ByteArrayOutputStream buff = new ByteArrayOutputStream(hash.length + len);
			buff.write(hash, 0, hash.length);
			buff.write(data, off, len);
			return buff.toByteArray();
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		}
	}

	/**
	 * 生成SHA1数据流，格式:SHA1签名(20字节)+数据
	 * 
	 * @param data
	 * @param off
	 * @param len
	 * @return
	 * 
	 * @throws SecureException
	 */
	public static byte[] sha1(byte[] data, int off, int len) throws SecureException {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(data, off, len);
			byte[] hash = md.digest();

			ByteArrayOutputStream buff = new ByteArrayOutputStream(hash.length + len);
			buff.write(hash, 0, hash.length);
			buff.write(data, off, len);
			return buff.toByteArray();
		} catch (NoSuchAlgorithmException exp) {
			throw new SecureException(exp);
		}
	}
	
//	public static void main(String[] args) {
//		Provider[] ps = Security.getProviders();
//		for(int i = 0; i < ps.length; i++) {
//			System.out.printf("provider name:%s\n", ps[i].getName());
//		}
//		
//		byte[] pwd = "www.lexst.com".getBytes();
//		
////		StringBuilder sb = new StringBuilder();
////		for(int i = 0; i < 4600; i++) {
////			sb.append('a');
////		}
////		byte[] data = sb.toString().getBytes();
////		for(int i = 0; i <data.length; i++) data[i] = (byte)65;
//		
//		byte[] data = "UnixSystem+Pentium@Lexst/SERVER".getBytes();
//		
//		byte[] raw = SecureEncryptor.desEncrypt(pwd, data);
//		System.out.printf("origin data size: %d\n", data.length);
//		System.out.printf("encrypt des3 raw size:%d, [%s]\n", raw.length, new String(raw));
//		
//		byte[] b = SecureDecryptor.desDecrypt(pwd, raw);
//		System.out.printf("decrypt string:%s\n", new String(b));
//		
////		raw = SecureEncryptor.md5Encrypt(data);
////		System.out.printf("data size:%d, md5 hash size:%d\n", data.length, raw.length);
////		
////		raw = SecureEncryptor.sha1Encrypt(data);
////		System.out.printf("data size:%d, sha1 hash size:%d", data.length, raw.length);
//	}


//	public static void main(String[] args) {
//		byte[] pwd = "www.lexst.com".getBytes();
//		byte[] data = "UNIX-SERVER".getBytes();
//
//		byte[] raw = SecureEncryptor.blowfishEncrypt(pwd, data);
//		System.out.printf("origin data size: %d\n", data.length);
//		System.out.printf("encrypt blowfish raw size:%d, String:%s\n", raw.length, new String(raw));
//		
//		byte[] b = SecureDecryptor.blowfishDecrypt(pwd, raw);
//		System.out.printf("decrypt string:%s\n", new String(b));
//	}

//	public static void main(String[] args) {
//		byte[] pwd = "www.lexst.com".getBytes();
//		byte[] data = "LEXST".getBytes();
//
//		byte[] raw = SecureEncryptor.aes(pwd, data);
//		System.out.printf("origin data size: %d\n", data.length);
//		System.out.printf("encrypt aes raw size:%d, String:%s\n", raw.length, new String(raw));
//		
//		byte[] b = SecureDecryptor.aes(pwd, raw);
//		System.out.printf("decrypt string:%s\n", new String(b));
//	}
	
//	public static void main(String[] args) {
//		String s = "remark1";
//		byte[] pwd = "pentium".getBytes();
//		try {
//			// AES算法
//			byte[] b = SecureEncryptor.aes(pwd, s.getBytes(), 0, s.getBytes().length);
//			for(int i = 0; i < b.length; i++) {
//				System.out.printf("%X ", b[i] & 0xff);
//			}
//
//			b = SecureDecryptor.aes(pwd, b, 0, b.length);
//			System.out.printf("\n%s\n\n", new String(b));
//			
//			// DES3 算法
//			b = SecureEncryptor.des3(pwd, s.getBytes(), 0, s.getBytes().length);
//			for(int i = 0; i < b.length; i++) {
//				System.out.printf("%X ", b[i] & 0xff);
//			}
//			b = SecureDecryptor.des3(pwd, b, 0, b.length);
//			System.out.printf("\n%s\n\n", new String(b));
//
//			// DES 算法
//			b = SecureEncryptor.des(pwd, s.getBytes(), 0, s.getBytes().length);
//			for(int i = 0; i < b.length; i++) {
//				System.out.printf("%X ", b[i] & 0xff);
//			}
//			b = SecureDecryptor.des(pwd, b, 0, b.length);
//			System.out.printf("\n%s\n\n", new String(b));
//			
//			// BLOWFISH算法
//			b = SecureEncryptor.blowfish(pwd, s.getBytes(), 0, s.getBytes().length);
//			for(int i = 0; i < b.length; i++) {
//				System.out.printf("%X ", b[i] & 0xff);
//			}
//			b = SecureDecryptor.blowfish(pwd, b, 0, b.length);
//			System.out.printf("\n%s\n\n", new String(b));
//
//			byte[] data = "UnixSystem".getBytes();
//			byte[] result = com.lexst.security.SecureEncryptor.sha1(data, 0, data.length);
//			System.out.printf("md5 encrypt is:%d\n", result.length);
//
//			data = com.lexst.security.SecureDecryptor.sha1(result, 0, result.length);
//			System.out.printf("md5 decrypt is:%d\n", data.length);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
}
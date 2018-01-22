/**
 * 
 */
package com.lexst.security;

import java.math.*;
import java.security.*;
import java.security.interfaces.*;
import java.security.spec.*;

import javax.crypto.*;


public class SecureGenerator {
	
	static final String DES_ALGO = "DES";
	static final String DES3_ALGO = "DESede";
	static final String AES_ALGO = "AES";

	/**
	 * 
	 */
	public SecureGenerator() {
		super();
	}

	/**
	 * 生成RSA公钥
	 * @param modulus
	 * @param exponent
	 * @return
	 * @throws SecureException
	 */
	public static RSAPublicKey buildRSAPublicKey(String modulus, String exponent) throws SecureException {
		try {
			KeyFactory keyFac = KeyFactory.getInstance("RSA");
			RSAPublicKeySpec pubKeySpec = new RSAPublicKeySpec(new BigInteger(
					modulus, 16), new BigInteger(exponent, 16));
			return (RSAPublicKey) keyFac.generatePublic(pubKeySpec);
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		} catch (InvalidKeySpecException e) {
			throw new SecureException(e);
		}
	}

	/**
	 * 生成RSA私钥
	 * @param modulus
	 * @param exponent
	 * @return
	 * @throws SecureException
	 */
	public static RSAPrivateKey buildRSAPrivateKey(String modulus, String exponent) throws SecureException {
		try {
			KeyFactory keyFac = KeyFactory.getInstance("RSA");
			RSAPrivateKeySpec priKeySpec = new RSAPrivateKeySpec(
					new BigInteger(modulus, 16), new BigInteger(exponent, 16));
			return (RSAPrivateKey) keyFac.generatePrivate(priKeySpec);
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		} catch (InvalidKeySpecException e) {
			throw new SecureException(e);
		}
	}
	
	/**
	 * 生成AES密钥
	 * @param pwd
	 * @return
	 * @throws SecureException
	 */
	public static SecretKey buildAESKey(byte[] pwd) throws SecureException {
		try {
			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
			random.setSeed(pwd);
			KeyGenerator generator = KeyGenerator.getInstance(SecureGenerator.AES_ALGO);
			generator.init(128, random);
			return generator.generateKey();
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		}
	}
	
	/**
	 * 生成DES密钥
	 * @param pwd
	 * @return
	 * @throws SecureException
	 */
	public static SecretKey buildDESKey(byte[] pwd) throws SecureException {
		try {
			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
			random.setSeed(pwd);
			KeyGenerator generator = KeyGenerator.getInstance(SecureGenerator.DES_ALGO);
			generator.init(56, random); //must be 56
			SecretKey key = generator.generateKey();
			return key;
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		}
	}
	
	/**
	 * 生成DES3密钥
	 * @param pwd
	 * @return
	 * @throws SecureException
	 */
	public static SecretKey buildDES3Key(byte[] pwd) throws SecureException {
		try {
			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
			random.setSeed(pwd);
			KeyGenerator generator = KeyGenerator.getInstance(SecureGenerator.DES3_ALGO);
			generator.init(168, random); // must be 56*3
			SecretKey key = generator.generateKey();
			return key;
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		}
	} 
	
	/**
	 * 生成Blowfish密钥
	 * @param pwd
	 * @return
	 * @throws SecureException
	 */
	public static SecretKey buildBlowfishKey(byte[] pwd) throws SecureException {
		try {
			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
			random.setSeed(pwd);
			KeyGenerator generator = KeyGenerator.getInstance("Blowfish");
			generator.init(64, random); //size must be multiple of 8
			return generator.generateKey();
		} catch (NoSuchAlgorithmException e) {
			throw new SecureException(e);
		}
	}

}
package com.lexst.debug;

import java.io.FileWriter;
import java.math.BigInteger;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.*;
import java.util.Date;

import javax.crypto.*;

//import com.lexst.master.license.License;
import com.lexst.util.MD5Encoder;

public class RSAUtil {
	
	private KeyPairGenerator kpg;
	private KeyPair kp;

	public RSAUtil() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public void initDemo() throws Exception {
		String pwd = "汉家儿郎战西北，汉将辞家破残贼。看茫茫大江来天畔，风烟几万年！";
		SecureRandom rnd = new SecureRandom();
		rnd.setSeed(pwd.getBytes());
		kpg = KeyPairGenerator.getInstance("RSA");
		kpg.initialize(2048, rnd);
		kp = kpg.generateKeyPair();
		RSAPublicKey publicKey = (RSAPublicKey) kp.getPublic();
		RSAPrivateKey privateKey = (RSAPrivateKey) kp.getPrivate();

		System.out.println("finished RSA Key Pair!");

//		License l = new License();
//		l.setUsername("demo");
//		long nowtime = System.currentTimeMillis();
//		long space = 120 * 24 * 60 * 60 * 1000L;
//		l.setBegin(new Date(nowtime));
//		l.setExprise(new Date(nowtime + space));
//		l.selectOS(License.windows);
//		l.setMaxHosts(4);	//4个节点

//		byte[] data = l.build();
		
		byte[] data = "千里江山寒色远，芦花深处泊孤舟，笛在月明楼。".getBytes();

		byte[] raw = this.encrypt(privateKey, data);

		String hexdata = MD5Encoder.toHexString(raw);
		String modulus = publicKey.getModulus().toString(16);
		String exponent = publicKey.getPublicExponent().toString(16);

		String pk = privateKey.getPrivateExponent().toString(16);
		System.out.printf("private key exponent:%s\n", pk);
		
		System.out.printf("%s\n", hexdata);
//		String pubmsg = String.format("M=%s\nE=%s\nU=%s\n",
//				modulus, exponent, l.getUsername());
		String pubmsg = String.format("M=%s\nE=%s\n", modulus, exponent);
		System.out.printf("%s", pubmsg);

		byte[] data2 = this.decrypt(publicKey, raw);
		if(data.length!=data2.length) {
			System.out.printf("message len not match!");
		}
		for(int i=0; i<data.length; i++) {
			if(data[i]!=data2[i]) {
				System.out.println("encrypt and decrypt not match data!");
				return ;
			}
		}
		System.out.println("data is match!");
		
		//////////////////
		RSAPublicKey rsaPublicKey = generateRSAPublicKey(modulus, exponent);
		byte[] data3 = this.decrypt(rsaPublicKey, raw);
		if (data3 == null) {
			System.out.println("this is a null result! two!");
		} else {
			//System.out.printf("decrypt data is:[%s]\n", new String(data3));
		}
		if(data.length!=data3.length) {
			System.out.printf("message len not match!");
		}
		for(int i=0; i<data.length; i++) {
			if(data[i]!=data3[i]) {
				System.out.println("encrypt and decrypt not match data!");
				return ;
			}
		}
		System.out.printf("data is match! data size:%d\n", hexdata.length());

//		FileWriter w = new FileWriter("c:/lexst.ac");
//		w.write( hexdata );
//		w.close();
//
//		w = new FileWriter("c:/lexst.key");
//		w.write(pubmsg);
//		w.close();
	}
	
	public void init() throws Exception {
		System.out.println("do RSA key pair!");
		SecureRandom rnd = new SecureRandom();
		rnd.setSeed("汉家儿郎战西北，汉将辞家破残贼".getBytes());
		kpg = KeyPairGenerator.getInstance("RSA");
		kpg.initialize(1024, rnd);
		kp = kpg.generateKeyPair();
		RSAPublicKey publicKey = (RSAPublicKey) kp.getPublic();
		RSAPrivateKey privateKey = (RSAPrivateKey) kp.getPrivate();
		
		System.out.println("finished RSA Key Pair!");
		
//		byte[] b1 = publicKey.getEncoded();
//		byte[] b2 = privateKey.getEncoded();
//		
//		String s1 = publicKey.getFormat();
//		String s2 = privateKey.getFormat();
//		
//		FileOutputStream out = new FileOutputStream("c:/publis.key");
//		out.write(s1.getBytes()); out.write("\r\n".getBytes());
//		out.write( publicKey.getModulus().toByteArray() );
//		out.write("\r\n".getBytes());
//		out.write( publicKey.getPublicExponent().toByteArray());
//		out.close();
//		
//		out = new FileOutputStream("c:/private.key");
//		out.write(s2.getBytes());
//		out.close();
//		System.out.println("save finished!");
		
//		ProtectionDomain  pd = new ProtectionDomain();
		
		String s = "粉坠百花洲，香残燕子楼。一团团，逐队成球。漂泊亦如人命薄，空缱绻，说风流！";//草木也知愁，韶华竟自白头，叹今生，谁舍谁收！嫁与东风春不管，凭尔去，忍淹流！";
		byte[] raw = this.encrypt(privateKey, s.getBytes()); //加密后的密文数据
		// 生成一个数字
		byte[] backup = new byte[raw.length];
		System.arraycopy(raw, 0, backup, 0, raw.length);

		BigInteger bi = new BigInteger(backup);
		System.out.printf("value is:%s\n", bi.toString(16));

		System.out.printf("encrypt size:%d\n", raw.length);
		byte[] msg = this.decrypt(publicKey, raw);
		if (msg == null) {
			System.out.println("this is a null result!");
		} else {
			System.out.printf("result is:[%s]\n", new String(msg));
		}

		String bb = this.makePublicKey(publicKey);
		System.out.println(bb);

		// 第二次产生公钥，解密
		byte[] modulus = publicKey.getModulus().toByteArray();
		byte[] exponent = publicKey.getPublicExponent().toByteArray();
		RSAPublicKey rsaPublicKey = generateRSAPublicKey(modulus, exponent);
		byte[] ms = this.decrypt(rsaPublicKey, backup);
		if (ms == null) {
			System.out.println("this is a null result! two!");
		} else {
			System.out.printf("decrypt data is:[%s]\n", new String(ms));
		}
		
		bb = this.makePublicKey(rsaPublicKey);
		System.out.println(bb);;
	}
	
	private String makePublicKey(RSAPublicKey key) {
		StringBuffer b = new StringBuffer();
		
		BigInteger modulu = key.getModulus();
		BigInteger exponent = key.getPublicExponent();
		b.append(String.format("singer:%s\n", "www.lexst.com"));
		b.append(String.format("m:%s\n", modulu.toString(16) ));
		b.append(String.format("e:%s\n", exponent.toString(16)));
		return b.toString();
	}

	private byte[] encrypt(Key key, byte[] data) throws NoSuchAlgorithmException,
			NoSuchPaddingException, InvalidKeyException,
			IllegalBlockSizeException, BadPaddingException {
		Cipher cipher = Cipher.getInstance("RSA");
		cipher.init(Cipher.ENCRYPT_MODE, key);
		int blocksize =	cipher.getBlockSize();
		int outsize =	cipher.getOutputSize( data.length );
		System.out.printf("encrypt block size is:%d, out size:%d\n", blocksize, outsize);
		byte[] raw = cipher.doFinal(data);
		return raw;
	}
	
	private byte[] decrypt(Key key, byte[] data) {
		Cipher cipher;
		try {
			cipher = Cipher.getInstance("RSA");
			cipher.init(Cipher.DECRYPT_MODE, key);
			byte[] raw = cipher.doFinal(data);
			return raw;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private RSAPublicKey generateRSAPublicKey(byte[] modulus, byte[] publicExponent) {
		try {
			KeyFactory keyFac = KeyFactory.getInstance("RSA");
			RSAPublicKeySpec pubKeySpec = new RSAPublicKeySpec(new BigInteger(
					modulus), new BigInteger(publicExponent));
			return (RSAPublicKey) keyFac.generatePublic(pubKeySpec);
		} catch (NoSuchAlgorithmException exp) {

		} catch (InvalidKeySpecException ex) {

		}
		return null;
	}
	
	private RSAPublicKey generateRSAPublicKey(String modulus, String publicExponent) {
		try {
			KeyFactory keyFac = KeyFactory.getInstance("RSA");
			RSAPublicKeySpec pubKeySpec = new RSAPublicKeySpec(new BigInteger(
					modulus, 16), new BigInteger(publicExponent, 16));
			return (RSAPublicKey) keyFac.generatePublic(pubKeySpec);
		} catch (NoSuchAlgorithmException exp) {

		} catch (InvalidKeySpecException ex) {

		}
		return null;
	}
	
	private RSAPrivateKey generateRSAPrivateKey(byte[] modulus, byte[] privateExponent) {
		try {
			KeyFactory keyFac = KeyFactory.getInstance("RSA");
			RSAPrivateKeySpec priKeySpec = new RSAPrivateKeySpec(
					new BigInteger(modulus), new BigInteger(privateExponent));
			return (RSAPrivateKey) keyFac.generatePrivate(priKeySpec);
		} catch (NoSuchAlgorithmException exp) {

		} catch (InvalidKeySpecException ex) {

		}
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RSAUtil u = new RSAUtil();
		try {
			//u.init();
			u.initDemo();
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

}
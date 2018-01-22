/**
 *
 */
package com.lexst.top.effect;

import java.util.*;

import org.w3c.dom.*;

import com.lexst.sql.account.*;
import com.lexst.util.effect.*;
import com.lexst.xml.*;

/**
 * 注册账号管理器，记录包括管理员和普通注册用户的信息
 *
 */
public class UserManager extends Effect {

	public final static String filename = "account.xml";

	/** 管理员账号(超级用户) */
	private Administrator dba = new Administrator();
	
	/** 账号用户 -> 配置参数 **/
	private Map<User, Account> mapUser = new TreeMap<User, Account>();
	
	/**
	 * 初始化
	 */
	public UserManager() {
		super();
	}

	/**
	 * 初始化并且设置管理员账号
	 * @param dba
	 */
	public UserManager(Administrator dba) {
		this();
		this.setDBA(dba);
	}

	/**
	 * 设置管理员账号
	 * @param admin
	 */
	public void setDBA(Administrator admin) {
		dba = new Administrator(admin);
	}

	/**
	 * 判断是不是管理员账号
	 * @param user
	 * @return
	 */
	public boolean isDBA(User user) {
		return dba.compareTo(user) == 0;
	}

	/**
	 * 返回管理员账号
	 * @return
	 */
	public Administrator getDBA() {
		return this.dba;
	}

	/**
	 * 根据注册用户名，查找对应的账号配置
	 * @param user
	 * @return
	 */
	public Account findAccount(User user) {
		return this.mapUser.get(user);
	}
	
	/**
	 * 根据用户名，查找对应的账号配置
	 * @param hexname - 16进制字符串
	 * @return
	 */
	public Account findAccount(String hexname) {
		User user = new User();
		user.setHexUsername(hexname);
		return this.findAccount(user);
	}
	
	/**
	 * 根据一组用户名(16进制字符串)，检查一组账号是否存在
	 * @param users
	 * @return
	 */
	public boolean exists(Collection<String> users) {
		for (String s : users) {
			if(findAccount(s) == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 增加账号配置
	 * @param account
	 * @return
	 */
	public boolean addAccount(Account account) {
		User user = (User) account.getUser().clone();
		// save account
		Account old = mapUser.get(user);
		if (old == null) {
			mapUser.put(user, account);
		} else {
			for (Permit permit : account.list()) {
				old.add(permit);
			}
		}
		return true;
	}

	/**
	 * 根据注册账号用户名(十六进制字符串)，删除配置
	 * @param username
	 * @return
	 */
	public boolean deleteAccount(String username) {
		User user = new User();
		user.setHexUsername(username);
		return mapUser.remove(user) != null;
	}

	/**
	 * 删除账号中的某些配置
	 * @param account
	 * @return
	 */
	public boolean deleteAccount(Account account) {
		User user = (User) account.getUser().clone();
		Account org = mapUser.get(user);
		if (org == null) {
			return false;
		}
		for (Permit permit : account.list()) {
			org.remove(permit);
		}
		return true;
	}

	/**
	 * 建立一个新账号，包括用户名和密码
	 * @param user
	 * @return
	 */
	public boolean create(User user) {
		Account account = mapUser.get(user);
		// 账号存在
		if (account != null) {
			return false;
		}
		account = new Account(user);
		return mapUser.put(user, account) == null;
	}

	/**
	 * 根据账号用户名(十六进制字符串)，删除内存中的配置
	 * @param username
	 * @return
	 */
	public boolean drop(String username) {
		User user = new User();
		user.setHexUsername(username);
		return mapUser.remove(user) != null;
	}

	/**
	 * 清空全部
	 */
	public void clear() {
		mapUser.clear();
	}

	/**
	 * 判断集合是否空状态
	 * @return
	 */
	public boolean isEmpty() {
		return mapUser.isEmpty();
	}

	/**
	 * 集合尺寸
	 * @return
	 */
	public int size() {
		return mapUser.size();
	}

	/**
	 * 生成账号的XML文档
	 * @return
	 */
	public byte[] buildXML() {
		StringBuilder buff = new StringBuilder(10240);
		for (Account account : mapUser.values()) {
			String text = account.buildXML();
			buff.append(text);
		}
		String body = XML.element("application", buff.toString());
		return toUTF8(Effect.xmlHead + body);
	}

	/**
	 * 解析账号集合中的参数
	 * @param bytes
	 * @return
	 */
	public boolean parseXML(byte[] bytes) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(bytes);
		if (document == null) {
			return false;
		}

		NodeList list = document.getElementsByTagName("account");
		int size = list.getLength();
		for (int i = 0; i < size; i++) {
			Element element = (Element) list.item(i);
			Account account = new Account();
			boolean success = account.parseXML(xml, element);
			if (!success) {
				return false;
			}
			User user = (User) account.getUser().clone();
			mapUser.put(user, account);
		}
		return true;
	}

}
/**
 *
 */
package com.lexst.sql.account;

import java.util.*;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.lexst.xml.XML;
import com.lexst.xml.XMLocal;

/**
 * 用户级权限控制表
 *
 */
public class UserPermit extends Permit {

	private static final long serialVersionUID = -2668835527702577668L;

	/** 账号用户名称 -> 操作控制集合  */
	private Map<User, Control> mapCtrl = new TreeMap<User, Control>();

	/**
	 *
	 */
	public UserPermit() {
		super(Permit.USER_PERMIT); 
	}

	/**
	 * 增加一个用户操作控制选项
	 * @param username - 用户名明文，需要转成SHA1码
	 * @param ctrl
	 */
	public void add(String username, Control ctrl) {
		User user = new User(username);
		mapCtrl.put(user, ctrl);
	}

	/**
	 * 删除一个用户操作配置
	 * @param username - 用户名明文，转成SHA1码
	 * @return
	 */
	public boolean remove(String username) {
		User user = new User(username);
		return mapCtrl.remove(user) != null;
	}

	/**
	 * 返回用户名集合(SHA1码)
	 * @return
	 */
	public Set<String> keys() {
		Set<String> a = new TreeSet<String>();
		for (User user : mapCtrl.keySet()) {
			a.add(user.getHexUsername());
		}
		return a;
	}

	/**
	 * 查找用户账号
	 * @param username
	 * @return
	 */
	public Control find(String username) {
		User user = new User(username);
		return mapCtrl.get(user);
	}

	/**
	 * 操作选项列表
	 * @return
	 */
	public Collection<Control> list() {
		return mapCtrl.values();
	}

	public int size() {
		return mapCtrl.size();
	}

	/**
	 *
	 */
	public boolean isAllow(int id) {
		for (Control ctrl : mapCtrl.values()) {
			if (ctrl.isAllow(id)) return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return mapCtrl.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#add(com.lexst.sql.account.Permit)
	 */
	@Override
	public boolean add(Permit object) {
		if (object == null || object.getClass() != UserPermit.class) {
			return false;
		}
		UserPermit permit = (UserPermit) object;
		for (User user : permit.mapCtrl.keySet()) {
			Control ctrl = permit.mapCtrl.get(user);
			Control previous = mapCtrl.get(user);
			if (previous == null) {
				mapCtrl.put(user, ctrl);
			} else {
				previous.add(ctrl);
			}
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#remove(com.lexst.sql.account.Permit)
	 */
	@Override
	public boolean remove(Permit object) {
		if (object == null || object.getClass() != UserPermit.class) {
			return false;
		}
		UserPermit permit = (UserPermit) object;
		for (User user : permit.mapCtrl.keySet()) {
			Control ctrl = permit.mapCtrl.get(user);
			Control previous = mapCtrl.get(user);
			if (previous != null) {
				previous.delete(ctrl);
				if (previous.isEmpty()) mapCtrl.remove(user);
			}
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#buildXML()
	 */
	@Override
	public String buildXML() {
		StringBuilder b = new StringBuilder(512);

		b.append(XML.element("rank", super.getPriority()));

		for (User username : mapCtrl.keySet()) {
			Control ctrl = mapCtrl.get(username);
			StringBuilder a = new StringBuilder(128);
			a.append(XML.cdata_element("username", username.getHexUsername()));
			for (int active : ctrl.list()) {
				String s = Control.translate(active);
				a.append(XML.cdata_element("ctrl", s));
			}
			b.append(XML.element("set", a.toString()));
		}
		return XML.element("permit", b.toString());
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#parseXML(org.w3c.dom.Element)
	 */
	@Override
	public boolean parseXML(Element element) {
		XMLocal xml = new XMLocal();

		String level = xml.getXMLValue(element.getElementsByTagName("rank"));
		int family = Integer.parseInt(level);
		if (family != Permit.USER_PERMIT) {
			return false;
		}
		super.setPriority(family);
		
		NodeList list = element.getElementsByTagName("set");
		int len = list.getLength();
		for (int i = 0; i < len; i++) {
			Element elem = (Element) list.item(i);
			User user = new User(xml.getXMLValue(elem.getElementsByTagName("username")));

			String[] all = xml.getXMLValues(elem.getElementsByTagName("ctrl"));
			Control ctrl = new Control();
			for (String s : all) {
				int id = Control.translate(s);
				if (id != -1){
					ctrl.add(id);
				}
			}
			mapCtrl.put(user, ctrl);
		}
		return true;
	}
}
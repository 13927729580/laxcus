/**
 *
 */
package com.lexst.sql.account;

import java.util.*;
import java.io.Serializable;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.lexst.sql.schema.*;
import com.lexst.util.naming.*;
import com.lexst.xml.XML;
import com.lexst.xml.XMLocal;

/**
 * 用户配置资源。<br>
 * 包括用户登录账号、可操作的数据库和数据表、三级操纵权限许可<br><br>
 * 
 * 每个账号下允许拥有多个数据库，一个数据库下允许有多个数据库。<br>
 * 用户操作权限最多只能有三级：用户级、数据库级、数据库表级(见Permit定义)。<br>
 */
public class Account implements Serializable {

	private static final long serialVersionUID = 1503020619640267408L;

	/** 用户登录账号 **/
	private User user = new User();

	/** 数据库名(忽略大小写) -> 表名集合  **/
	private Map<Naming, SpaceSet> spaces = new TreeMap<Naming, SpaceSet>();

	/** 操作权限表编号(分用户、数据库、数据库表三级，见Permit类中定义) -> 权限配置 **/
	private Map<Integer, Permit> permits = new TreeMap<Integer, Permit>();

	/**
	 * 初始化
	 */
	public Account() {
		super();
	}

	/**
	 * @param user
	 */
	public Account(User user) {
		this();
		this.setUser(user);
	}

	/**
	 * 设置用户账号
	 * @param s
	 */
	public void setUser(User s) {
		this.user = new User(s);
	}

	/**
	 * @return
	 */
	public User getUser() {
		return this.user;
	}

	/**
	 * 保存一个数据库配置
	 * @param schema
	 * @return
	 */
	public boolean addSchema(String schema) {
		Naming naming = new Naming(schema);
		SpaceSet set = spaces.get(naming);
		if (set != null) {
			return false;
		}
		set = new SpaceSet();
		return spaces.put(naming, set) == null;
	}

	/**
	 * 删除数据库配置
	 * 
	 * @param schema
	 * @return
	 */
	public boolean deleteSchema(String schema) {
		Naming naming = new Naming(schema);
		return spaces.remove(naming) != null;
	}

	/**
	 * 增加一个数据库表，前提是数据库必须存在
	 * @param space
	 * @return
	 */
	public boolean addSpace(Space space) {
		Naming naming = new Naming(space.getSchema());
		SpaceSet set = spaces.get(naming);
		if (set == null) {
			return false;
		}
		return set.add(space);
	}

	/**
	 * 删除一个数据库表，前提是数据库必须存在
	 * @param space
	 * @return
	 */
	public boolean deleteSpace(Space space) {
		Naming naming = new Naming(space.getSchema());
		SpaceSet set = spaces.remove(naming);
		if (set == null) {
			return false;
		}
		return set.remove(space);
	}

	/**
	 * 返回数据库名称集合
	 * @return
	 */
	public Set<String> schemaKeys() {
		Set<String> set = new TreeSet<String>();
		for (Naming naming : spaces.keySet()) {
			set.add(naming.toString());
		}
		return set;
	}

	/**
	 * 查找某个数据库下的全部数据库表
	 * @param schema
	 * @return
	 */
	public List<Space> findSpaces(String schema) {
		Naming naming = new Naming(schema);
		ArrayList<Space> a = new ArrayList<Space>();
		SpaceSet set = spaces.get(naming);
		if (set != null) {
			a.addAll(set.list());
		}
		return a;
	}

	/**
	 * 是否允许建立账号
	 * @return
	 */
	public boolean allowCreateUser() {
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				return permit.isAllow(Control.CREATE_USER);
			}
		}
		return false;
	}

	/**
	 * 是否允许删除账号
	 * @return
	 */
	public boolean allowDropUser() {
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				return permit.isAllow(Control.DROP_USER);
			}
		}
		return false;
	}

	/**
	 * 是否允许建立权限(账号级别下)
	 * @return
	 */
	public boolean allowGrant() {
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				return permit.isAllow(Control.GRANT);
			}
		}
		return false;
	}

	/**
	 * 是否允许回收权限(账号级别下)
	 * @return
	 */
	public boolean allowRevoke() {
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				return permit.isAllow(Control.REVOKE);
			}
		}
		return false;
	}

	/**
	 * 是否允许建立数据库(账号级别)
	 * @return
	 */
	public boolean allowCreateSchema() {
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				return permit.isAllow(Control.CREATE_SCHEMA);
			}
		}
		return false;
	}

	/**
	 * 是否允许删除数据库(账号级别)
	 * @return
	 */
	public boolean allowDropSchema() {
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				return permit.isAllow(Control.DROP_SCHEMA);
			}
		}
		return false;
	}

	/**
	 * 是否允许建立数据库表(账号和数据库级别)
	 * @param schema
	 * @return
	 */
	public boolean allowCreateTable(String schema) {
		boolean success = false;
		for (Permit permit : permits.values()) {
			if (permit.isUserPermit()) {
				success = permit.isAllow(Control.CREATE_TABLE);
				if (success) break;
			} else if (permit.isSchemaPrimit()) {
				success = ((SchemaPermit) permit).isAllow(schema, Control.CREATE_TABLE);
				if (success) break;
			}
		}
		return success;
	}

	/**
	 * 是否允许删除数据库表(账号和数据库级别)
	 * @param schema
	 * @return
	 */
	public boolean allowDropTable(String schema) {
		boolean success = false;
		for(Permit permit : permits.values()) {
			if(permit.isUserPermit()) { 
				success = permit.isAllow(Control.DROP_TABLE);
				if(success) break;
			} else if(permit.isSchemaPrimit()) {
				success = ((SchemaPermit) permit).isAllow(schema, Control.DROP_TABLE);
				if(success) break;
			}
		}
		return success;
	}

	/**
	 * 增加一类权限许可
	 * @param permit
	 * @return
	 */
	public boolean add(Permit permit) {
		int priority = permit.getPriority();
		Permit previous = permits.get(priority);
		if (previous == null) {
			return permits.put(priority, permit) == null;
		} else {
			return previous.add(permit);
		}
	}

	/**
	 * 删除一类权限许可(可能是其中一部分或者全部)
	 * @param permit
	 * @return
	 */
	public boolean remove(Permit permit) {
		int priority = permit.getPriority();
		Permit previous = permits.get(priority);
		if (previous == null) {
			return false;
		}
		boolean success = previous.remove(permit);
		if (previous.isEmpty()) {
			permits.remove(priority);
		}
		return success;
	}

	/**
	 * 权限可许集合
	 * all permit
	 * @return
	 */
	public Collection<Permit> list() {
		return permits.values();
	}

	public boolean isEmpty() {
		return permits.isEmpty();
	}

	public int size() {
		return permits.size();
	}

	/**
	 * 用户账号配置转成XML文档
	 * @return
	 */
	public String buildXML() {
		StringBuilder buff = new StringBuilder(512);
		buff.append(XML.cdata_element("username", user.getHexUsername()));
		buff.append(XML.cdata_element("password", user.getHexPassword()));
		buff.append(XML.cdata_element("maxsize", user.getMaxSize()));

		// 全部数据库
		for (Naming naming : spaces.keySet()) {
			SpaceSet set = spaces.get(naming);

			StringBuilder a = new StringBuilder(512);
			for (Space space : set.list()) {
				a.append(XML.cdata_element("table", space.getTable()));
			}
			StringBuilder b = new StringBuilder(512);
			b.append(XML.cdata_element("schema", naming.toString()));
			b.append(XML.element("all", a.toString()));

			buff.append(XML.element("architecture", b.toString()));
		}
		// 全部操作权限
		for(Permit permit : permits.values()) {
			String s = permit.buildXML();
			buff.append(s);
		}
		return XML.element("account", buff.toString());
	}

	/**
	 * XML文档解析为账号配置
	 * @param xml
	 * @param root
	 * @return
	 */
	public boolean parseXML(XMLocal xml, Element root) {
		String s = xml.getXMLValue(root.getElementsByTagName("username"));
		user.setHexUsername(s);
		s = xml.getXMLValue(root.getElementsByTagName("password"));
		user.setHexPassword(s);
		s = xml.getXMLValue(root.getElementsByTagName("maxsize"));
		user.setMaxSize(Long.parseLong(s));

		// 解析当前账号下的全部数据库配置
		NodeList list = root.getElementsByTagName("architecture");
		int len = list.getLength();
		for (int i = 0; i < len; i++) {
			Element element = (Element) list.item(i);
			String schema = xml.getXMLValue(element.getElementsByTagName("schema"));
			String[] tables = xml.getXMLValues(element.getElementsByTagName("table"));
			this.addSchema(schema);
			for (int n = 0; tables != null && n < tables.length; n++) {
				this.addSpace(new Space(schema, tables[n]));
			}
		}

		// 解析当前账号下的全部操作权限配置
		list = root.getElementsByTagName("permit");
		// not found , return true;
		if (list == null) return true;
		len = list.getLength();
		for (int i = 0; i < len; i++) {
			Element element = (Element) list.item(i);
			s = xml.getXMLValue(element.getElementsByTagName("rank"));
			int priority = Integer.parseInt(s);
			Permit permit = null;
			switch(priority) {
			case Permit.TABLE_PERMIT:
				permit = new TablePermit();
				break;
			case Permit.SCHEMA_PERMIT:
				permit = new SchemaPermit();
				break;
			case Permit.USER_PERMIT:
				permit = new UserPermit();
				break;
			default:
				return false;
			}
			boolean success = permit.parseXML(element);
			if (success) {
				permits.put(permit.getPriority(), permit);
			}
		}
		return true;
	}

}
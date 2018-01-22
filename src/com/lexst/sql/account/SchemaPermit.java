/**
 *
 */
package com.lexst.sql.account;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.lexst.util.naming.*;
import com.lexst.xml.XML;
import com.lexst.xml.XMLocal;

import java.util.*;

/**
 * 数据库表操作许可
 *
 */
public class SchemaPermit extends Permit {

	private static final long serialVersionUID = -1040577531373515808L;
	
	/** 数据库名 -> 控制标识号集合 **/
	private Map<Naming, Control> mapCtrl = new TreeMap<Naming, Control>();

	/**
	 * default
	 */
	public SchemaPermit() {
		super(Permit.SCHEMA_PERMIT);
	}

	public void add(String schema, Control ctrl) {
		mapCtrl.put(new Naming(schema), ctrl);
	}

	public boolean remove(String schema) {
		Naming naming = new Naming(schema);
		return mapCtrl.remove(naming) != null;
	}

	public Set<String> keys() {
		Set<String> a = new TreeSet<String>();
		for(Naming naming : mapCtrl.keySet()) {
			a.add(naming.toString());
		}
		return a;
	}

	public Control find(String schema) {
		return mapCtrl.get(new Naming(schema));
	}

	public Collection<Control> list() {
		return mapCtrl.values();
	}

	public int size() {
		return mapCtrl.size();
	}

	public boolean isEmpty() {
		return mapCtrl.isEmpty();
	}

	public boolean isAllow(int id) {
		return false;
	}

	public boolean isAllow(String schema, int id) {
		Control ctrl =	mapCtrl.get(new Naming(schema));
		if(ctrl != null) {
			return ctrl.isAllow(id);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#add(com.lexst.sql.account.Permit)
	 */
	@Override
	public boolean add(Permit other) {
		if( other.getClass() != SchemaPermit.class) {
			return false;
		}
		SchemaPermit permit = (SchemaPermit)other;
		for(Naming schema : permit.mapCtrl.keySet()) {
			Control ctrl = permit.mapCtrl.get(schema);
			Control previous = mapCtrl.get(schema);
			if(previous == null) {
				mapCtrl.put(schema, ctrl);
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
	public boolean remove(Permit other) {
		if (other.getClass() != SchemaPermit.class) {
			return false;
		}
		SchemaPermit permit = (SchemaPermit) other;
		for (Naming schema : permit.mapCtrl.keySet()) {
			Control ctrl = permit.mapCtrl.get(schema);
			Control previous = mapCtrl.get(schema);
			if (previous != null) {
				previous.delete(ctrl);
				if (previous.isEmpty()) mapCtrl.remove(schema);
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

		for (Naming schema : mapCtrl.keySet()) {
			Control ctrl = mapCtrl.get(schema);
			StringBuilder a = new StringBuilder(128);
			a.append(XML.cdata_element("schema", schema.toString()));
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
		if (family != Permit.SCHEMA_PERMIT) {
			return false;
		}
		super.setPriority(family);

		NodeList list = element.getElementsByTagName("set");
		int len = list.getLength();
		for (int i = 0; i < len; i++) {
			Element elem = (Element) list.item(i);
			Naming schema = new Naming(xml.getXMLValue(elem.getElementsByTagName("schema")));

			String[] all = xml.getXMLValues(elem.getElementsByTagName("ctrl"));
			Control ctrl = new Control();
			for (String s : all) {
				int id = Control.translate(s);
				if (id != -1)
					ctrl.add(id);
			}
			mapCtrl.put(schema, ctrl);
		}
		return true;
	}

}
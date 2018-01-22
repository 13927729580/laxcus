/**
 *
 */
package com.lexst.sql.account;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.*;

import com.lexst.sql.schema.*;
import com.lexst.xml.XML;
import com.lexst.xml.XMLocal;

/**
 * 数据库表操作权限
 *
 */
public class TablePermit extends Permit {

	private static final long serialVersionUID = -2408541815331675463L;

	/** 数据库表名称 -> 操作许可标识号集合 **/
	private Map<Space, Control> mapCtrl = new TreeMap<Space, Control>();

	/**
	 *
	 */
	public TablePermit() {
		super(Permit.TABLE_PERMIT);
	}

	public boolean isEmpty() {
		return mapCtrl.isEmpty();
	}

	public int size() {
		return mapCtrl.size();
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#isAllow(int)
	 */
	public boolean isAllow(int id) {
		for (Control ctrl : mapCtrl.values()) {
			if (ctrl.isAllow(id)) return true;
		}
		return false;
	}

	/**
	 * @param space
	 * @param ctrl
	 * @return
	 */
	public boolean add(Space space, Control ctrl) {
		Control previous = mapCtrl.get(space);
		if (previous == null) {
			mapCtrl.put(space, ctrl);
		} else {
			previous.add(ctrl);
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.sql.account.Permit#add(com.lexst.sql.account.Permit)
	 */
	@Override
	public boolean add(Permit object) {
		if( object.getClass() != TablePermit.class) {
			return false;
		}
		TablePermit permit = (TablePermit)object;
		for(Space space : permit.mapCtrl.keySet()) {
			Control ctrl = permit.mapCtrl.get(space);
			Control previous = mapCtrl.get(space);
			if(previous == null) {
				mapCtrl.put(space, ctrl);
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
		if (object.getClass() != TablePermit.class) {
			return false;
		}
		TablePermit permit = (TablePermit) object;
		for (Space space : permit.mapCtrl.keySet()) {
			Control ctrl = permit.mapCtrl.get(space);
			Control previous = mapCtrl.get(space);
			if (previous != null) {
				previous.delete(ctrl);
				if (previous.isEmpty()) mapCtrl.remove(space);
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

		for (Space space : mapCtrl.keySet()) {
			Control ctrl = mapCtrl.get(space);
			StringBuilder a = new StringBuilder(128);
			a.append(XML.cdata_element("schema", space.getSchema()));
			a.append(XML.cdata_element("table", space.getTable()));
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
		int priority = Integer.parseInt(level);
		if (priority != Permit.TABLE_PERMIT) {
			return false;
		}
		super.setPriority(priority);

		NodeList list = element.getElementsByTagName("set");
		int len = list.getLength();
		for (int i = 0; i < len; i++) {
			Element elem = (Element) list.item(i);
			String schema = xml.getXMLValue(elem.getElementsByTagName("schema"));
			String table = xml.getXMLValue(elem.getElementsByTagName("table"));

			String[] all = xml.getXMLValues(elem.getElementsByTagName("ctrl"));
			Control ctrl = new Control();
			for (String s : all) {
				int id = Control.translate(s);
				if (id != -1) ctrl.add(id);
			}
			mapCtrl.put(new Space(schema, table), ctrl);
		}
		return true;
	}

}
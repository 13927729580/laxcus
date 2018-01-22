/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * lexst database configure
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 3/8/2009
 * 
 * @see com.lexst.sql.schema
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.schema;

import java.io.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.util.*;
import com.lexst.util.naming.*;
import com.lexst.xml.*;

/**
 * 数据库名称及参数定义
 *
 */
public class Schema implements Serializable, Cloneable, Comparable<Schema> {

	private static final long serialVersionUID = -1471970483804001910L;

	/** 数据库名称，忽略大小写。名称长度不得超过64字节，此定义见Space类 */
	private Naming name;

	/** 数据库空间的最大允许字节量。默认是0，不定义 */
	private long maxsize;

	/** 数据库中包含的数据表集合  */
	private Map<Space, Table> tables = new TreeMap<Space, Table>();

	/** 数据库中的参数优化配置(定时更新等) */
	private Map<Space, SwitchTime> switches = new HashMap<Space, SwitchTime>();

	/**
	 * default
	 */
	public Schema() {
		super();
		maxsize = 0L;
	}

	/**
	 * @param name
	 */
	public Schema(String name) {
		this();
		this.setName(name);
	}
	
	/**
	 * 复制对象
	 * @param object
	 */
	public Schema(Schema object) {
		this();
		this.setName(object.name);
		this.setMaxSize(object.maxsize);
		this.switches.putAll(object.switches);
		this.tables.putAll(object.tables);
	}

	/**
	 * 设置数据库名称
	 * @param s
	 */
	public void setName(String s) {
		this.name = new Naming(s);
	}
	
	/**
	 * 设置数据库名称
	 * @param s
	 */
	public void setName(Naming s) {
		this.name = new Naming(s);
	}
	
	/**
	 * 返回数据库名称
	 * @return
	 */
	public String getName() {
		return this.name.toString();
	}
	
	/**
	 * 返回数据库名称
	 * @return
	 */
	public Naming getNaming() {
		return this.name;
	}
	
	/**
	 * 设置数据库最大空间尺寸
	 * @param size
	 */
	public void setMaxSize(long size) {
		if (size < 0L) {
			throw new IllegalArgumentException("invalid maxsize:" + size);
		}
		this.maxsize = size;
	}

	/**
	 * 返回数据库最大空间尺寸
	 * @return
	 */
	public long getMaxSize() {
		return this.maxsize;
	}

	/**
	 * 检查数据库表是否存在
	 * @param space
	 * @return
	 */
	public boolean exists(Space space) {
		return tables.get(space) != null;
	}
	
	/**
	 * 返回数据库下的表名集合
	 * @return
	 */
	public Set<Space> spaces() {
		TreeSet<Space> set = new TreeSet<Space>();
		if (!tables.isEmpty()) {
			set.addAll(tables.keySet());
		}
		return set;
	}

	/**
	 * 根据表名查找一个表配置
	 * 
	 * @param space
	 * @return
	 */
	public Table findTable(Space space) {
		return tables.get(space);
	}

	/**
	 * 根据表名查找一个重构触发时间
	 * 
	 * @param space
	 * @return
	 */
	public SwitchTime findSwitchTime(Space space) {
		return switches.get(space);
	}

	/**
	 * find chunk size
	 * @param space
	 * @return
	 */
	public int findChunkSize(Space space) {
		Table table = tables.get(space);
		if(table != null) {
			return table.getChunkSize();
		}
		return -1;
	}
	
	/**
	 * set chunk size
	 * @param space
	 * @param size
	 * @return
	 */
	public boolean setChunkSize(Space space, int size) {
		Table table = tables.get(space);
		if (table == null) return false;
		table.setChunkSize(size);
		return true;
	}
	
	/**
	 * 定义表的数据重构时间
	 * @param space
	 * @param columnId
	 * @param type
	 * @param interval
	 * @return
	 */
	public boolean setRebuildTime(Space space, short columnId, int type, long interval) {
		if (!tables.containsKey(space)) {
			return false;
		}

		SwitchTime switchTime = new SwitchTime();
		switchTime.setSpace(space);
		switchTime.setColumnId(columnId);
		switchTime.setType(type);
		switchTime.setInterval(interval);

		switches.put(space, switchTime);
		return true;
	}

//	/**
//	 * get expired space 
//	 * @return
//	 */
//	public List<Space> getExpiredSpace() {
//		List<Space> array = new ArrayList<Space>();
//		for(Space space : mapSwitch.keySet()) {
//			SwitchTime trig = mapSwitch.get(space);
//			if(trig.isExpired()) {
//				array.add(space);
//				trig.touch(); // make next time
//			}
//		}
//		return array;
//	}

	/**
	 * @param space
	 * @param table
	 * @return
	 */
	public boolean add(Space space, Table table) {
		if(tables.containsKey(space)) {
			return false;
		}
		return tables.put(space, table) == null;
	}

	/**
	 * delete a table schema
	 * @param space
	 * @return
	 */
	public boolean remove(Space space) {
		boolean success = (tables.remove(space) != null);
		this.switches.remove(space);
		return success;
	}

	public Collection<Table> listTable() {
		List<Table> array = new ArrayList<Table>();
		if(!tables.isEmpty()) {
			array.addAll(tables.values());
		}
		return array;
	}

	/**
	 * 生成XML格式的数据流
	 * 
	 * @return
	 */
	public String buildXML() {
		StringBuilder buff = new StringBuilder();
		buff.append(XML.element("name", this.getName()));
		buff.append(XML.element("maxsize", maxsize));

		for (Space space : tables.keySet()) {
			StringBuilder a = new StringBuilder();
			Table table = tables.get(space);
			String tablename = table.getSpace().getTable();

			a.append(XML.element("name", tablename));
			// time trigger
			SwitchTime switchTime = switches.get(space);
			if (switchTime != null) {
				StringBuilder bu = new StringBuilder();
				bu.append(XML.element("columnId", switchTime.getColumnId()));
				bu.append(XML.element("type", switchTime.getType()));
				bu.append(XML.element("interval", switchTime.getInterval()));
				a.append(XML.element("trigger", bu.toString()));
			}

			byte[] b = Base64.encode(table.build());
			String text = new String(b);
			MD5Encoder md5 = new MD5Encoder();
			String hex = md5.encode(text);
			String s = String.format("<schema encode=\"base64\" tag=\"%s\"><![CDATA[%s]]></schema>", hex, text);
			a.append(s);

			String body = XML.element("table", a.toString());
			buff.append(body);
		}
		return XML.element("database", buff.toString());
	}

	/**
	 * 解析XML数据流
	 * 
	 * @param xml
	 * @param element
	 * @return
	 */
	public boolean parseXML(XMLocal xml, Element element) {
		this.setName(xml.getXMLValue(element.getElementsByTagName("name")));
		try {
			String s = xml.getXMLValue(element.getElementsByTagName("maxsize"));
			maxsize = Long.parseLong(s);
		} catch (NumberFormatException exp) {
			return false;
		}

		NodeList nodes = element.getElementsByTagName("table");
		// not found , return true;
		if (nodes == null) return true;

		int len = nodes.getLength();
		for (int j = 0; j < len; j++) {
			Element sub = (Element) nodes.item(j);
			String tabname = xml.getXMLValue(sub.getElementsByTagName("name"));
			Element head = (Element) sub.getElementsByTagName("schema").item(0);
			String tag = head.getAttribute("tag");
			String encode = head.getAttribute("encode");
			String text = head.getTextContent();

			// check tag
			MD5Encoder md5 = new MD5Encoder();
			String hex = md5.encode(text);
			if (!hex.equals(tag)) {
				return false;
			}

			byte[] b = null;
			if ("base64".equalsIgnoreCase(encode)) {
				b = Base64.decode(text.getBytes());
			} else {
				return false;
			}

			Table table = new Table();
			int end = table.resolve(b, 0, b.length);
			if (end != b.length) {
				return false;
			}

			// save table
			Space space = new Space(this.getName(), tabname);
			if (tables.put(space, table) != null) {
				return false;
			}
			
			// save space trigger
			NodeList trig = sub.getElementsByTagName("trigger");
			if (trig != null && trig.getLength() == 1) {
				Element el = (Element) trig.item(0);
				String columnId = xml.getXMLValue(el.getElementsByTagName("columnId"));
				String type = xml.getXMLValue(el.getElementsByTagName("type"));
				String time = xml.getXMLValue(el.getElementsByTagName("interval"));
				
				SwitchTime switchTime = new SwitchTime();
				switchTime.setSpace(space);
				switchTime.setColumnId(java.lang.Short.parseShort(columnId));
				switchTime.setType(java.lang.Integer.parseInt(type));
				switchTime.setInterval(java.lang.Long.parseLong(time));
				
				switches.put(space, switchTime);
				
//				SwitchTime tt = new SwitchTime(space, Integer.parseInt(type), Long.parseLong(time));
//				mapSwitch.put(space, tt);
			}
		}
		return true;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof Schema)) {
			return false;
		} else if (object == this) {
			return true;
		}
		return this.compareTo((Schema) object) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (name == null) {
			return 0;
		}
		return name.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Schema(this);
	}

	/*
	 * 比较名称是否一致(忽略大小写)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Schema schema) {
		if (name == null) {
			return -1;
		} else if (schema == null || schema.name == null) {
			return 1;
		} else {
			return this.name.compareTo(schema.name);
		}
	}

}
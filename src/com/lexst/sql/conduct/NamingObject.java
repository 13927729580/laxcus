/**
 * @email admin@lexst.com
 *
 */
package com.lexst.sql.conduct;

import java.io.*;
import java.util.*;

import com.lexst.sql.conduct.value.*;
import com.lexst.sql.index.section.*;
import com.lexst.util.naming.*;

/**
 * 所有命名任务的基类
 * 
 */
public abstract class NamingObject implements Serializable, Cloneable {

	private static final long serialVersionUID = -4962265931903145480L;

	/** 命名对象 **/
	private Naming naming;

	/** 参数集(允许多个相同参数名的值存在) **/
	private ArrayList<CValue> array = new ArrayList<CValue>(5);

	/**
	 * default
	 */
	public NamingObject() {
		super();
	}

	/**
	 * @param naming
	 */
	public NamingObject(Naming naming) {
		this();
		this.setNaming(naming);
	}

	/**
	 * 完全复制对象
	 * 
	 * @param object
	 */
	public NamingObject(NamingObject object) {
		this();
		this.setNaming(object.naming);
		this.array.addAll(object.array);
	}

	/**
	 * 设置任务命名。命名可以忽略大小写，必须在TOP集群唯一。
	 * 
	 * @param s
	 */
	public void setNaming(Naming s) {
		this.naming = new Naming(s);
	}

	/**
	 * 设置命名
	 * 
	 * @param naming
	 */
	public void setNaming(String s) {
		this.naming = new Naming(s);
	}

	/**
	 * 返回命名
	 * 
	 * @return
	 */
	public Naming getNaming() {
		return this.naming;
	}

	/**
	 * 返回命名的字符串
	 * 
	 * @return
	 */
	public String getNamingString() {
		if (naming == null) {
			return null;
		}
		return naming.toString();
	}

	/**
	 * 保存一组参数
	 * 
	 * @param values
	 */
	public boolean addValues(Collection<CValue> values) {
		return this.array.addAll(values);
	}

	/**
	 * 保存一个参数
	 * 
	 * @param value
	 * @return
	 */
	public boolean addValue(CValue value) {
		return this.array.add(value);
	}

	/**
	 * 返回参数集合
	 * 
	 * @return
	 */
	public List<CValue> list() {
		return this.array;
	}

	/**
	 * 返回下标位置的参数
	 * 
	 * @param index
	 * @return
	 */
	public CValue getValue(int index) {
		if (index < 0 || index >= array.size()) {
			throw new IndexOutOfBoundsException("array indexout!");
		}
		return array.get(index);
	}

	/**
	 * 空集合判断
	 * 
	 * @return
	 */
	public boolean isEmpty() {
		return array.isEmpty();
	}

	/**
	 * 集合成员数目
	 * 
	 * @return
	 */
	public int size() {
		return array.size();
	}

	/**
	 * 内存空间占用收缩到成员数
	 */
	public void trimValues() {
		array.trimToSize();
	}

	/**
	 * 根据名称和在数据的下标位置，找到对应的参数
	 * 
	 * @param name
	 * @param index
	 * @return
	 */
	public CValue find(String name, int index) {
		if (index < 0 || index >= array.size()) {
			throw new IndexOutOfBoundsException("array indexout!");
		}
		int seek = 0;
		for (CValue value : array) {
			if (value.getName().equalsIgnoreCase(name)) {
				if (seek == index) {
					return value;
				}
				seek++;
			}
		}
		return null;
	}

	/**
	 * 根据名称，找到第一个匹配参数
	 * 
	 * @param name
	 * @return
	 */
	public CValue find(String name) {
		return find(name, 0);
	}

	/**
	 * 增加一组分片区域
	 * 
	 * @param name
	 * @param sector
	 * @return
	 */
	public boolean addSector(String name, ColumnSector sector) {
		byte[] b = sector.build();
		CRaw raw = new CRaw(name, b);
		return this.addValue(raw);
	}

	/**
	 * 根据名字和存储排序位置，找对应的分片区域
	 * 
	 * @param name
	 * @param index
	 * @return
	 */
	public ColumnSector findSector(String name, int index) {
		CValue value = this.find(name, index);
		// 没找到或者数据类型不匹配
		if (value == null || !value.isRaw()) {
			return null;
		}

		byte[] b = ((CRaw) value).getValue();
		return SectorParser.split(b, 0, b.length);
	}

	/**
	 * 查找数组中第一个数据分片
	 * 
	 * @param name
	 * @return
	 */
	public ColumnSector findSector(String name) {
		return this.findSector(name, 0);
	}

	/*
	 * 克隆命名对象
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return duplicate();
	}

	/**
	 * 复制命名对象，具体由子类实现
	 * 
	 * @return
	 */
	public abstract Object duplicate();
}
/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2009 lexst.com, All rights reserved
 * 
 * sql table information
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 6/3/2009
 * @see com.lexst.sql.schema
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.sql.schema;

import java.io.*;
import java.net.*;
import java.util.*;

import com.lexst.sql.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.util.*;
import com.lexst.util.host.*;

/**
 * 数据库表配置，包括列属性集合和其它参数定义
 *
 */
public class Table implements Serializable, Cloneable, Comparable<Table> {

	private static final long serialVersionUID = 3779757357483300218L;

	/** 表的版本号(通过版本号做不同的数据流生成和解析) **/
	private static final int VERSION = 0x1;

	/** 表在主机上模式(共享或者独占) */
	public static final int SHARE = 1;

	public static final int EXCLUSIVE = 2;

	/** 表的存储模式(物理内模式)， 默认是行存储(NSM)。定义见Type类 */
	private byte storage;

	/** 表名 **/
	private Space space;

	/** 列属性集合 **/
	private ArrayList<ColumnAttribute> array = new ArrayList<ColumnAttribute>();

	/** 分派到的HOME节点配置 */
	private Cluster cluster = new Cluster();

	/** 允许的DATA主节点数目(一个表可以在多个DATA主节点上存在，默认是1) **/
	private int primes;

	/** CHUNK备份数目 (默认是 1) **/
	private int copy;

	/** 表在DATA主机上的存在模式 (share or exclusive, 默认是 share) */
	private int mode;

	/** 数据文件尺寸 (默认是 64M) **/
	private int chunksize;

	/** 是否在DATA主节点上启用缓存(默认是TRUE) **/
	private boolean caching;

	/**
	 * default
	 */
	public Table() {
		super();
		this.setStorage(Type.NSM);
		this.setPrimes(1);
		this.setCopy(1);
		this.setMode(Table.SHARE);
		this.setChunkSize(0x4000000); //64M
		this.setCaching(true);
	}

	/**
	 * @param space
	 */
	public Table(Space space) {
		this();
		this.setSpace(space);
	}

	/**
	 * @param capacity
	 */
	public Table(int capacity) {
		this();
		array.ensureCapacity(capacity);
	}

	/**
	 * @param space
	 * @param capacity
	 */
	public Table(Space space, int capacity) {
		this(capacity);
		this.setSpace(space);
	}
	
	/**
	 * 复制表配置
	 * 
	 * @param table
	 */
	public Table(Table table) {
		this();
		this.setStorage(table.getStorage());
		
		this.setSpace(table.space);
		this.addAll(table.array);
		
		this.cluster.set(table.cluster);

		this.setPrimes(table.getPrimes());
		this.setCopy(table.getCopy());
		this.setMode(table.getMode());
		this.setChunkSize(table.getChunkSize());
		this.setCaching(table.isCaching());
	}
	
	/**
	 * 设置存储模型(行存储或者列存储)
	 * 
	 * @param b
	 */
	public void setStorage(byte b) {
		if (b != Type.NSM && b != Type.DSM) {
			throw new IllegalArgumentException("invalid storage model");
		}
		this.storage = b;
	}
	
	/**
	 * 返回存储模型ID定义
	 * @return
	 */
	public byte getStorage() {
		return this.storage;
	}
	
	/**
	 * 判断是行存储模型
	 * @return
	 */
	public boolean isNSM() {
		return this.storage == Type.NSM;
	}

	/**
	 * 判断是列存储模型
	 * @return
	 */
	public boolean isDSM() {
		return this.storage == Type.DSM;
	}

	/**
	 * 设置数据表名称
	 * @param s
	 */
	public void setSpace(Space s) {
		space = new Space(s);
	}

	/**
	 * 返回数据表名称
	 * @return
	 */
	public Space getSpace() {
		return space;
	}
	
	/**
	 * get home site set
	 * @return
	 */
	public Cluster getCluster() {
		return this.cluster;
	}
	
	/**
	 * 设置表在DATA主节点上的数量
	 * 
	 * @param num
	 */
	public void setPrimes(int num) {
		if (num < 1) {
			throw new IllegalArgumentException("invalid prime host num:" + num);
		}
		this.primes = num;
	}

	public int getPrimes() {
		return this.primes;
	}
	
	/**
	 * chunk copy number
	 * @param num
	 */
	public void setCopy(int num) {
		if (num < 1) {
			throw new IllegalArgumentException("invalid copy num:" + num);
		}
		this.copy = num;
	}

	public int getCopy() {
		return this.copy;
	}
	
	/**
	 * 表的数据在主机上存在模式(共享或者独占)
	 * @param id
	 */
	public void setMode(int id) {
		if (id != Table.SHARE && id != Table.EXCLUSIVE) {
			throw new IllegalArgumentException("invalid host mode:" + id);
		}
		this.mode = id;
	}

	public int getMode() {
		return this.mode;
	}
	
	/**
	 * 判断是否共享模式
	 * 
	 * @return
	 */
	public boolean isShare() {
		return this.mode == Table.SHARE;
	}

	/**
	 * 判断是否独亨模式
	 * 
	 * @return
	 */
	public boolean isExclusive() {
		return this.mode == Table.EXCLUSIVE;
	}
	
	/**
	 * chunk size
	 * @param size
	 */
	public void setChunkSize(int size) {
		if (size < 1024 * 1024) {
			throw new IllegalArgumentException("invalid chunk size:" + size);
		}
		this.chunksize = size;
	}

	public int getChunkSize() {
		return this.chunksize;
	}
	
	/**
	 * DATA主节点的缓存模式
	 * 
	 * @param b
	 */
	public void setCaching(boolean b) {
		this.caching = b;
	}
	
	public boolean isCaching() {
		return this.caching;
	}

	/**
	 * 保存列属性，按照列ID顺序存储
	 * @param attribute
	 * @return
	 */
	public boolean add(ColumnAttribute attribute) {
		short columnId = attribute.getColumnId();
		if (columnId > array.size()) {
			array.add(attribute);
		} else {
			array.add(columnId - 1, attribute);
		}
		return true;
	}
	
	/**
	 * 保存全部列属性
	 * @param attributes
	 * @return
	 */
	public boolean addAll(Collection<ColumnAttribute> attributes) {
		return array.addAll(attributes);
	}
	
	/**
	 * 根据属性名，删除一列属性
	 * 
	 * @param name
	 * @return
	 */
	public boolean remove(String name) {
		for (int index = 0; index < array.size(); index++) {
			ColumnAttribute attribute = array.get(index);
			if (attribute.getName().equalsIgnoreCase(name)) {
				return array.remove(index) != null;
			}
		}
		return false;
	}
	
	/**
	 * 根据列ID，删除一列属性
	 * 
	 * @param columnId
	 * @return
	 */
	public boolean remove(short columnId) {
		if (columnId <= array.size()) {
			ColumnAttribute attribute = array.get(columnId - 1);
			if (attribute.getColumnId() == columnId) {
				return array.remove(columnId - 1) != null;
			}
		}
		for (int index = 0; index < array.size(); index++) {
			ColumnAttribute attribute = array.get(index);
			if (attribute.getColumnId() == columnId) {
				return array.remove(index) != null;
			}
		}
		return false;
	}
	
	/**
	 * name set
	 * @return
	 */
	public Set<String> nameSet() {
		TreeSet<String> set = new TreeSet<String>();
		for(ColumnAttribute attribute : array) {
			set.add(attribute.getName());
		}
		return set;
	}
	
	/**
	 * column id set
	 * @return
	 */
	public Set<Short> idSet() {
		TreeSet<Short> set = new TreeSet<Short>();
		for(ColumnAttribute attribute : array) {
			set.add(attribute.getColumnId());
		}
		return set;
	}
	
	/**
	 * value set
	 * @return
	 */
	public Collection<ColumnAttribute> values() {
		return this.array;
	}

	/**
	 * 查找表的主键属性(是prime key，非primary key)
	 * 
	 * @return
	 */
	public ColumnAttribute pid() {
		for (ColumnAttribute attribute : array) {
			if (attribute.isPrimeKey()) {
				return attribute;
			}
		}
		return null;
	}
	
	/**
	 * find a attribute by column name
	 * @param name
	 * @return
	 */
	public ColumnAttribute find(String name) {
		for (ColumnAttribute attribute : array) {
			if (attribute.getName().equalsIgnoreCase(name)) {
				return attribute;
			}
		}
		return null;
	}

	/**
	 * 根据列ID查找对应的列属性
	 * 
	 * @param columnId
	 * @return
	 */
	public ColumnAttribute find(short columnId) {
		if (columnId <= array.size()) {
			ColumnAttribute attribute = array.get(columnId - 1);
			if (attribute.getColumnId() == columnId) {
				return attribute;
			}
		}
		for (int index = 0; index < array.size(); index++) {
			ColumnAttribute attribute = array.get(index);
			if (attribute.getColumnId() == columnId) {
				return attribute;
			}
		}
		return null;
	}
	
	/**
	 * 返回指定下标的列属性
	 * 
	 * @param index
	 * @return
	 */
	public ColumnAttribute get(int index) {
		if (index < 0 || index >= array.size()) {
			return null;
		}
		return array.get(index);
	}
	
	/**
	 * 根据列ID数组的排列顺序，生成一个Sheet
	 * 
	 * @param columnIds
	 * @return
	 */
	public Sheet getSheet(short[] columnIds) {
		int index = 0;
		Sheet sheet = new Sheet();
		for(short columnId : columnIds) {
			ColumnAttribute attribute = find(columnId);
			if(attribute == null) {
				throw new ColumnAttributeException("cannot find %d", columnId);
			}
			sheet.add(index++, (ColumnAttribute) attribute.clone());
		}
		return sheet;
	}
	
	/**
	 * 返回有序表
	 * @return
	 */
	public Sheet getSheet() {
		short[] columnIds = new short[array.size()];
		for (int i = 0; i < columnIds.length; i++) {
			columnIds[i] = array.get(i).getColumnId();
		}
		java.util.Arrays.sort(columnIds);
		return getSheet(columnIds);
	}

	/**
	 * 将列属性数组空间调整为实际大小(删除多余空间)
	 */
	public void reserve() {
		array.trimToSize();
	}
	
	/**
	 * 清除全部列属性
	 */
	public void clear() {
		array.clear();
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

	public int size() {
		return array.size();
	}

	/*
	 * 比较是否一致
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != Table.class) {
			return false;
		} else if (obj == this) {
			return true;
		}
		Table table = (Table) obj;
		return space != null && space.equals(table.space);
	}

	/*
	 * 散列码
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return space.hashCode();
	}
	
	/*
	 * 复制Table
	 * 
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new Table(this);
	}

	/* 
	 * 比较表名是否相同
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Table table) {
		return space.compareTo(table.space);
	}

	/**
	 * 生成数据流并且返回
	 * 
	 * @return
	 */
	public byte[] build() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024 * 5);
		// 版本号
		byte[] b = Numeric.toBytes(Table.VERSION);
		buff.write(b, 0, b.length);
		// 物理存储模型
		buff.write(this.storage);

		// 数据库名和表名，不允许超过64字节
		byte[] schemaName = space.getSchema().getBytes();
		byte[] tableName = space.getTable().getBytes();
		buff.write((byte)schemaName.length);
		buff.write((byte)tableName.length);
		buff.write(schemaName, 0, schemaName.length);
		buff.write(tableName, 0, tableName.length);
		// tag
		byte[] tags = buff.toByteArray();

		// 全部列属性
		buff.reset();
		for (ColumnAttribute attribute : array) {
			b = attribute.build();
			buff.write(b, 0, b.length);
		}
		byte[] data = buff.toByteArray();
		buff.reset();
		// 列属性成员数
		short elements = (short)array.size();
		b = Numeric.toBytes(elements);
		buff.write(b, 0, b.length);
		// 全部列属性字节数
		b = Numeric.toBytes(data.length);
		buff.write(b, 0, b.length);
		// write data
		buff.write(data, 0, data.length);
		
		// "数据块复制数量"标记
		data = Numeric.toBytes(copy);
		int size = (data == null ? 0 : data.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(data, 0, data.length);
		}
		// 数据存在模式(共享/独享)
		data = Numeric.toBytes(mode);
		size = (data == null ? 0 : data.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(data, 0, data.length);
		}
		// 数据块尺寸
		data = Numeric.toBytes(chunksize);
		size = (data == null ? 0 : data.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(data, 0, data.length);
		}
		// "prime host"成员数
		data = Numeric.toBytes(primes);
		size = (data == null ? 0 : data.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(data, 0, data.length);
		}
		//  "not catch" 标记
		data = Numeric.toBytes(caching ? 1 : 0);
		size = (data == null ? 0 : data.length);
		b = Numeric.toBytes(size);
		buff.write(b, 0, b.length);
		if (size > 0) {
			buff.write(data, 0, data.length);
		}

		// 生成集群参数
		List<Address> addresses = cluster.list();
		b = Numeric.toBytes(addresses.size());
		buff.write(b, 0, b.length);
		if (addresses.size() > 0) {
			for (Address address : addresses) {
				byte[] addr = address.bits();
				b = Numeric.toBytes(addr.length);
				buff.write(b, 0, b.length);
				buff.write(addr, 0, addr.length);
			}
		}

		// flush all
		data = buff.toByteArray();
		// write all
		buff.reset();
		int count = 4 + tags.length + data.length;
		b = Numeric.toBytes(count);
		buff.write(b, 0, b.length);
		buff.write(tags, 0, tags.length);
		buff.write(data, 0, data.length);

		return buff.toByteArray();
	}

	/**
	 * 解析表的数据流
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public int resolve(byte[] b, int off, int len) {
		int seek = off;
		int end = off + len;
		
		// check size
		if(seek + 4 > end) {
			throw new IllegalArgumentException("table sizeout!");
		}
		int count = Numeric.toInteger(b, seek, 4);
		seek += 4;
		// 版本号
		int version = Numeric.toInteger(b, seek, 4);
		if(version != Table.VERSION) {
			throw new IllegalArgumentException("version not match!");
		}
		seek += 4;
		// 物理存储模型 (nsm or dsm)
		this.storage = b[seek++];

		// 数据表空间名称
		int schemaSize = b[seek++] & 0xFF;
		int tableSize = b[seek++] & 0xFF;
		if (!Space.isSchemaSize(schemaSize) || !Space.isTableSize(tableSize)) {
			throw new IllegalArgumentException("invalid space size!");
		}
		
		String schema = new String(b, seek, schemaSize);
		seek += schemaSize;
		String table = new String(b, seek, tableSize);
		seek += tableSize;
		space = new Space(schema, table);

		// 列属性成员数和长度
		short elements = Numeric.toShort(b, seek, 2);
		seek += 2;
		int allsize = Numeric.toInteger(b, seek, 4);
		seek += 4;

		int scan = seek;

		for (short i = 0; i < elements; i++) {
			// 解析属性，保存属性
			byte type = b[seek];
			ColumnAttribute attribute = ColumnAttributeCreator.create(type);
			if(attribute == null) {
				throw new ColumnAttributeException("unknown column attribute:%d", type & 0xFF);
			}
			int size = attribute.resolve(b, seek, end - seek);
			if (size < 1) {
				throw new IllegalArgumentException("column attribute resolve error!");
			}
			seek += size;
			this.add(attribute);
		}
		if (seek - scan != allsize) {
			throw new IllegalArgumentException("column attribute size error!");
		}
		
		// resolve copy
		int size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
			copy = Numeric.toInteger(b, seek, 4);
			seek += 4;
		}
		// resolve mode
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
			mode = Numeric.toInteger(b, seek, 4);
			seek += 4;
		}
		// resolve chunk size
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(size > 0) {
			chunksize = Numeric.toInteger(b, seek, 4);
			seek += 4;
		}
		// resolve prime host num
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(size > 0) {
			primes = Numeric.toInteger(b, seek, 4);
			seek += 4;
		}
		// resolve not cache
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if (size > 0) {
			int value = Numeric.toInteger(b, seek, 4);
			seek += 4;
			caching = (value == 1);
		}

		// 解析HOME集群	
		size = Numeric.toInteger(b, seek, 4);
		seek += 4;
		if(size > 0) {
			for(int i = 0; i < size; i++) {
				int addrlen = Numeric.toInteger(b, seek, 4);
				seek += 4;
				byte[] addr = Arrays.copyOfRange(b, seek, seek + addrlen);
				seek += addrlen;
				try {
					cluster.add(new Address(addr));
				} catch (UnknownHostException e) {
					
				}
			}
			// 重定义HOME集群数量
			cluster.setSites(size);
		}
		
		// check all size
		if (seek - off != count) {
			throw new IllegalArgumentException("table resolve error!");
		}
		
		// 减去数组剩余空间
		this.reserve();

		return seek - off;
	}

}
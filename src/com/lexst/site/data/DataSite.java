/**
 *
 */
package com.lexst.site.data;

import java.util.*;

import com.lexst.site.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.schema.*;
import com.lexst.util.naming.*;

/**
 * 数据存储节点配置
 *
 */
public class DataSite extends RankSite {

	private static final long serialVersionUID = -4596350053905504921L;

	/** 磁盘已经使用尺寸 */
	private long usedSize;

	/** 未使用尺寸 */
	private long freeSize;

	/** 数据库索引信息 */
	private IndexSchema schema = new IndexSchema();
	
	/** 节点上绑定的diffuse命名任务集合 */
	private ArrayList<Naming> array = new ArrayList<Naming>();

	/**
	 * default constructor
	 */
	public DataSite() {
		super(Site.DATA_SITE);
		usedSize = freeSize = 0L;
	}

	/**
	 * @param rank
	 */
	public DataSite(byte rank) {
		this();
		this.setRank(rank);
	}

	/**
	 * 增加一个diffuse命名
	 * 
	 * @param naming
	 * @return
	 */
	public boolean addNaming(Naming naming) {
		if (naming == null || array.contains(naming)) {
			return false;
		}
		return array.add((Naming) naming.clone());
	}
	
	/**
	 * 删除一个diffuse命名
	 * @param naming
	 * @return
	 */
	public boolean removeNaming(String naming) {
		return array.remove(new Naming(naming));
	}
	
	/**
	 * 增加一组diffuse命名
	 * @param list
	 * @return
	 */
	public int addNamings(Collection<Naming> list) {
		int size = array.size();
		for (Naming naming : list) {
			addNaming(naming);
		}
		return array.size() - size;
	}

	/**
	 * 更新全部命名参数
	 * 
	 * @param list
	 * @return
	 */
	public int updateNamings(Collection<Naming> list) {
		array.clear();
		for (Naming naming : list) {
			addNaming(naming);
		}
		return array.size();
	}
	
	public void clearAllNaming() {
		array.clear();
	}
	
	public List<Naming> listNaming() {
		return array;
	}


	/**
	 * 设置DATA节点上已经使用的磁盘空间尺寸
	 * @param i
	 */
	public void setUsable(long i) {
		this.usedSize = i;
	}

	/**
	 * 返回DATA节点已经使用的磁盘空间尺寸
	 * @return
	 */
	public long getUsable() {
		return this.usedSize;
	}
	
	/**
	 * 设置DATA节点没有使用的磁盘空间尺寸
	 * @param i
	 */
	public void setFree(long i) {
		this.freeSize = i;
	}

	/**
	 * 返回DATA节点没有使用的磁盘空间尺寸
	 * @return
	 */
	public long getFree() {
		return this.freeSize;
	}
	

	public void setIndexSchema(IndexSchema db) {
		this.schema = db;
	}

	public IndexSchema getIndexSchema() {
		return this.schema;
	}

	/**
	 * @param space
	 * @return
	 */
	public boolean contains(Space space) {
		return schema.contains(space);
	}

	/**
	 * clear database index
	 */
	public void clear() {
		schema.clear();
	}
}
/**
 *
 */
package com.lexst.site.work;

import java.util.*;

import com.lexst.site.*;
import com.lexst.util.naming.*;

/**
 * WORK节点参数配置 
 */
public class WorkSite extends Site {
	private static final long serialVersionUID = -3196055910538205137L;

	/** aggregate命名集合 */
	private ArrayList<Naming> array = new ArrayList<Naming>();

	/**
	 * default
	 */
	public WorkSite() {
		super(Site.WORK_SITE);
	}

	/**
	 * 保存一个任务命名
	 * @param naming
	 * @return
	 */
	public boolean add(Naming naming) {
		if (naming == null || array.contains(naming)) {
			return false;
		}
		return array.add((Naming) naming);
	}

	/**
	 * 保存一组任务命名
	 * @param set
	 * @return
	 */
	public int addAll(Collection<Naming> set) {
		int count = 0;
		for (Naming naming : set) {
			if (add(naming)) count++;
		}
		return count;
	}
	
	/**
	 * 更新全部命名对象
	 * @param set
	 * @return
	 */
	public int update(Collection<Naming> set) {
		array.clear();
		for (Naming naming : set) {
			array.add((Naming) naming.clone());
		}
		return array.size();
	}
	
	public void clear() {
		array.clear();
	}

	public boolean remove(Naming naming) {
		return array.remove(naming);
	}

	public List<Naming> list() {
		return array;
	}

	/**
	 * 将命名数组调整为实际大小
	 */
	public void trim() {
		array.trimToSize();
	}
}
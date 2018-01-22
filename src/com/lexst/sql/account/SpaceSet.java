/**
 *
 */
package com.lexst.sql.account;

import java.util.*;

import com.lexst.sql.schema.*;

/**
 * 数据库表名集合
 *
 */
final class SpaceSet {

	private Set<Space> array = new TreeSet<Space>();

	/**
	 *
	 */
	public SpaceSet() {
		super();
	}

	public boolean add(Space space) {
		return array.add(space);
	}

	public boolean remove(Space space) {
		return array.remove(space);
	}

	public Collection<Space> list() {
		return array;
	}

	public boolean isEmpty() {
		return array.isEmpty();
	}

	public int size() {
		return array.size();
	}
}
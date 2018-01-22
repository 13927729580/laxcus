/**
 *
 */
package com.lexst.debug;

import java.util.*;

/**
 * @author siven
 *
 */
public class Sort {

	private int capacity = 10000;
	private ArrayList<Integer> array;
	private Set<Integer> set ;

	java.util.Random rnd = new java.util.Random(System.currentTimeMillis());

	/**
	 *
	 */
	public Sort(int size) {
		this.capacity = size;
		if (this.capacity < 10000) {
			this.capacity = 100000;
		}
		array = new ArrayList<Integer>(this.capacity);
		set = new java.util.TreeSet<Integer>();
	}

	public void random() {
		long begin = System.currentTimeMillis();
		for (int i = 0; i < this.capacity; i++) {
			array.add(rnd.nextInt());
		}
		java.util.Collections.sort(array);
		long used = System.currentTimeMillis() - begin;
		System.out.printf("array sort used time:%d\n", used);

		array.clear();
	}

	public void insert() {
		long begin = System.currentTimeMillis();
		for (int i = 0; i < this.capacity; i++) {
			set.add(rnd.nextInt());
		}
		long used = System.currentTimeMillis() - begin;
		System.out.printf("treeset insert used time:%d\n", used);

		set.clear();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Sort s = new Sort(100000);
		for(int i = 0; i< 5; i++) {
			s.random();
		}
		for(int i = 0; i< 5; i++) {
			s.insert();
		}

	}

}

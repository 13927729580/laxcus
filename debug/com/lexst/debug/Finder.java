/**
 * 
 */
package com.lexst.debug;

import java.io.*;

/**
 * @author siven
 *
 */
public class Finder {

//	private int count = 0;
	private int search_count = 0;
	private int found_size = 0;
	
	/**
	 * 
	 */
	public Finder() {
		super();
	}
	
	
	
	private void search(File file, String words) {
		if(!file.exists()) return;
		
		int len = (int)file.length();
		byte[] b = new byte[len];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(b);
			in.close();
		} catch (IOException exp) {
			
		}
		
		search_count++;
//		System.out.printf("scan %s\n", file.getAbsolutePath());
		
		String s = new String(b).toLowerCase();
		int index = s.indexOf(words.toLowerCase());
		if(index > -1) {
			this.found_size++;
			System.out.println(file.getAbsolutePath());
		}
	}
	
	public void find(File path, String words) {
		if (path.exists() && path.isDirectory()) {
			File[] subs = path.listFiles();
			for (int i = 0; subs != null && i < subs.length; i++) {
				if (!subs[i].exists()) continue;
				if (subs[i].isDirectory()) {
					this.find(subs[i], words);
				} else if (subs[i].isFile()) {
					search(subs[i], words);
				}
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String path = "F:/lexst";
		path = "D:/nexst/lexst090816/lexst";
		File file = new File(path);
		String words = "ClassLoader";
		words = "JList";
		words = "MimeCellRederer";
		
		Finder finder = new Finder();
		finder.find(file, words);
		System.out.printf("finished! scan size:%d, found size:%d\n", finder.search_count, finder.found_size);
	}

}

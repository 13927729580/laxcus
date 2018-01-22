/**
 * 
 */
package com.lexst.debug;

import java.io.*;
import java.util.*;

/**
 * @author siven
 *
 */
public class SpellLoader {
	
	private String rootfile = "f:\\document\\splitword\\spell.txt";
	
	/**
	 * 
	 */
	public SpellLoader() {
		// TODO Auto-generated constructor stub
	}
	
	private boolean isAllow(String s) {
		if (s.length() < 2) return false;
		for (int i = 0; i < s.length(); i++) {
			char w = s.charAt(i);
			if (0 <= w && w <= 0xff) return false;
		}
		return true;
	}
	
	public void importText(String filename, String encode) throws IOException {
		File file = new File(filename);
		if (!file.exists()) return;
		byte[] b = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(filename);
		in.read(b, 0, b.length);
		in.close();
		
		String text = new String(b, 0, b.length, encode);
		System.out.println(text);

		StringReader in2 = new StringReader(text);
		BufferedReader reader = new BufferedReader(in2);
		ArrayList<String> array = new ArrayList<String>(1024000);
		while (true) {
			String s = reader.readLine();
			if (s == null) break;
			s = s.trim();
			if (isAllow(s)) array.add(s);
		}
		reader.close();
		in.close();
		// 写入磁盘文件
		writeRoot(array);
	}
	
	public void importFirst(String filename, String encode) throws IOException {
		char w = '	';
		File file = new File(filename);
		if (!file.exists()) return;
		byte[] b = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(filename);
		in.read(b, 0, b.length);
		in.close();
		
		String text = new String(b, 0, b.length, encode); // "GBK");
		System.out.println(text);

		StringReader in2 = new StringReader(text);
		BufferedReader reader = new BufferedReader(in2);
		ArrayList<String> array = new ArrayList<String>(1024000);
		while (true) {
			String s = reader.readLine();
			if (s == null) break;
//			System.out.println(s);
			s = s.trim();
			int index = s.indexOf((char)0x20);
			if(index == -1) {
				index = s.indexOf(w);
				if (index == -1) continue;
			}
			String first = s.substring(0, index);
			if (isAllow(first)) array.add(first);
		}
		reader.close();
		in.close();
		// 写入磁盘文件
		writeRoot(array);
	}
	
	private long count = 0;
	
	private void writeRoot(Collection<String> list) throws IOException {
		StringBuilder buff = new StringBuilder();
		for (String word : list) {
			buff.append(String.format("%s\n", word));
		}
		count += list.size();
		
		File file = new File(rootfile);
		boolean append = (file.exists() ? true : false);
		FileOutputStream out = new FileOutputStream(file, append );
		
		byte[] b = buff.toString().getBytes("UTF-8");
		out.write(b, 0, b.length);
		out.close();
	}
	
	public void trim() {
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String first = "first";
		String text = "text";
		Map<String, SpellEncode> map = new HashMap<String, SpellEncode>();

		map.put("F:\\document\\splitword\\words\\dict.txt", new SpellEncode(first, "UTF-8")); //UTF8
		map.put("F:\\document\\splitword\\words\\cutdic.txt", new SpellEncode(first, "GBK")); //GBK
		map.put("F:\\document\\splitword\\words\\SogouLabDic.dic", new SpellEncode(first,"GBK")); //GBK
		map.put("F:\\document\\splitword\\words\\httpcws_dict.txt", new SpellEncode(first, "GBK")); //GBK
		
		map.put("F:\\document\\splitword\\words\\festival.dic", new SpellEncode(text, "UTF-8"));
		map.put("F:\\document\\splitword\\words\\china.dic", new SpellEncode(text, "UTF-8"));
		map.put("F:\\document\\splitword\\words\\simplexu8.txt", new SpellEncode(text, "UTF-8"));
		map.put("F:\\document\\splitword\\words\\sDict.txt", new SpellEncode(text, "UTF-8"));
		map.put("F:\\document\\splitword\\words\\t-base.dic", new SpellEncode(text, "UTF-8"));
		
		map.put("F:\\document\\splitword\\words\\cndict.txt", new SpellEncode(text, "GBK"));

		long time = System.currentTimeMillis();
		SpellLoader spell = new SpellLoader();
		try {
			for(String key : map.keySet()) {
				SpellEncode se = map.get(key);
				if(se.type.equals(first)) {
					spell.importFirst(key, se.encode);
				} else if(se.type.equals(text)) {
					spell.importText(key, se.encode);		
				}
			}
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("all finished, usedtime:%d\n", usedtime);
		System.out.printf("count word item:%d\n", spell.count);
	}

}

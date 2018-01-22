package com.lexst.debug;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

import com.dwms.spell.Spell;
import com.dwms.spell.SpellSet;

public class SpellTrimer {
	
	private String rootfile = "f:\\document\\splitword\\spells.txt";

	public SpellTrimer() {
		// TODO Auto-generated constructor stub
	}
	
	private void trim(String filename, String to) throws IOException {
		
		Map<Integer, SpellSet> map = new TreeMap<Integer, SpellSet>();
		
		File file = new File(filename);
		if (!file.exists()) return;
		byte[] b = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(filename);
		in.read(b, 0, b.length);
		in.close();
		
		String text = new String(b, 0, b.length, "UTF-8");
//		System.out.println(text);

		StringReader in2 = new StringReader(text);
		BufferedReader reader = new BufferedReader(in2);
		int count = 0;
		while (true) {
			String s = reader.readLine();
			if (s == null) break;
			s = s.trim();
			count++;
			int hash = s.hashCode();
			SpellSet set = map.get(hash);
			if(set == null) {
				set = new SpellSet();
				map.put(hash, set);
			}
			set.add(new Spell(s));
		}
		reader.close();
		in.close();
		
		int real = 0;
		for(SpellSet set : map.values()) {
			real += set.size();
		}
		
		StringBuilder buff = new StringBuilder(real * 10);
		for(SpellSet set : map.values()) {
			for(Spell spell : set.list()) {
				String s = String.format("%s\n", spell.word());
				buff.append(s);
			}
		}

		text = buff.toString();
		b = text.getBytes("UTF-8");
		FileOutputStream out = new FileOutputStream(to);
		out.write(b, 0, b.length);
		out.close();
		
		System.out.printf("disk count:%d, real count:%d\n", count, real);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String to = "f:\\document\\splitword\\trims.txt";
		SpellTrimer l = new SpellTrimer();
		long time = System.currentTimeMillis();
		try {
			l.trim(l.rootfile, to);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("usedtime:%d\n", usedtime);
	}

}

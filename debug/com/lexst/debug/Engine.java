/**
 * 
 */
package com.lexst.debug;

import java.io.*;
import java.util.*;

import com.lexst.data.*;
import com.lexst.remote.client.call.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.util.*;
import com.lexst.util.host.*;

/**
 * @author siven
 *
 */
public class Engine {

	class WordItem {
		
		public String getWord() { return ""; }
		public int getDocumentId() { return 1;}
		public short getWeight() { return Short.MAX_VALUE;}
	}
	
	/**
	 * 
	 */
	public Engine() {
		// TODO Auto-generated constructor stub
	}

	
	public void uploadWord(List<WordItem> array, Table table1, String ip, int port) throws IOException {
//		Inject inject = new Inject(table);
//		UTF16 utf16 = new UTF16();
//		
//		Field wordField = table.find("word");
//		Field docIdField = table.find("documentId");
//		Field weightField = table.find("weight");
//		
//		for(WordItem item : array) {
//			Row row = new Row();
//			byte[] b = utf16.encode(item.getWord());
//			row.add(new NChar(wordField.getColumnId(), b));
//			row.add(new Int(docIdField.getColumnId(), item.getDocumentId()));
//			row.add(new Small(weightField.getColumnId(), item.getWeight()));			
//			inject.add(row);
//		}
//		
//		CallClient client = new CallClient(true); 
//		SocketHost remote = new SocketHost(SocketHost.TCP, ip ,port);
//		client.connect(remote);
//		int items = client.inject(inject, false);
//		System.out.printf("insert count: %d\n", items);
//		client.close();
		
		byte[] db = "engine".getBytes();
		byte[] table = "words".getBytes();
//		long[] values = Install.marshal(db, table, (short)0);
//		long i = values[0];

		int result = Install.marshal(db, table, (short)0);

		if(result < 0) ; // this is error
		else if(result == 0) ; // this is empty
		else {
			while(true) {
				byte[] data = Install.educe(db, table, 1024*1024);
				if(data == null) break;
				// analyse data
				compress(null, data);
			}
		}
		
//		while(true) {
//			byte[] data = Install.educe(db, table, 10485760);
//			if(data == null) break;
//			compress(null, data);
//		}
	}
	
	int HASH_SIZE = 1000;
	
	class Item implements Comparable<Item> {
		int doucmentId;
		short weight;

		public Item(int documentId, short weight) {
			this.doucmentId = documentId;
			this.weight = weight;
		}

		@Override
		public int compareTo(Item arg) {
			if(weight < arg.weight) return 1;
			else if(weight > arg.weight) return -1;
			return 0;
		}
	}
	
	class ItemSet {
		ArrayList<Item> array = new ArrayList<Item>();
		
		public void add(int documentId, short weight) {
			array.add(new Item(documentId, weight));
		}
		
		public void sort() {
			java.util.Collections.sort(array);
		}
		
		public byte[] build() {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			for(Item block : array) {
				byte[] b = Numeric.toBytes(block.doucmentId);
				out.write(b, 0, b.length);
				b = Numeric.toBytes(block.weight);
				out.write(b, 0, b.length);
			}
			return out.toByteArray();
		}
	}
	
	class WordSet {
		Map<Integer, ItemSet> mapSet = new TreeMap<Integer, ItemSet>();
		
		public void add(int documentId, short weight) {
			int hash = documentId % HASH_SIZE;
			ItemSet set = mapSet.get(hash);
			if(set == null) {
				set = new ItemSet();
				mapSet.put(hash, set);
			}
			set.add(documentId, weight);
		}
		
		public Set<Integer> keySet() {
			return mapSet.keySet();
		}
		
		public ItemSet get(int hash) {
			return mapSet.get(hash);
		}		
	}

	private void compress(Table zipwords_table, byte[] data) {
		Map<String, WordSet> map = new TreeMap<String, WordSet>();

		UTF16 utf16 = new UTF16();
		for (int off = 0; off < data.length;) {
			Row row = new Row();
			int len = row.resolve(zipwords_table, data, off, data.length-off);
			off += len;

			byte[] b = ((com.lexst.sql.column.SChar) row.find((short) 1)).getValue();
			String word = new String(utf16.decode(b, 0, b.length));
			int documentId = ((com.lexst.sql.column.Integer) row.find((short) 2)).getValue();
			short weight = ((com.lexst.sql.column.Short) row.find((short) 3)).getValue();

			WordSet set = map.get(word);
			if (set == null) {
				set = new WordSet();
				map.put(word, set);
			}
			set.add(documentId, weight);
		}

		Inject inject = new Inject(zipwords_table);
		for (String word : map.keySet()) {
			WordSet set = map.get(word);
			
			byte[] b = utf16.encode(word);
			com.lexst.sql.column.SChar nchar = new com.lexst.sql.column.SChar((short) 1, b);

			for(int hash : set.keySet()) {
				ItemSet is = set.get(hash);
				is.sort(); // 降序排序
				
				Row row = new Row();
				row.add(nchar);

				row.add(new com.lexst.sql.column.Integer((short) 2, hash));

				b = is.build();
				row.add(new com.lexst.sql.column.Raw((short) 3, b));

				inject.add(row);
			}
		}

		byte[] b = inject.build();
		Install.insert(b);
	}
	
	public void abb() {
		ItemSet set = new ItemSet();
		short end = 20;
		int documentId = 1288;
		for(short begin = 5; begin < end; begin+=2) {
			set.add(documentId, begin);
		}
		set.sort();
		for(Item item : set.array) {
			System.out.printf("%d - %d\n", item.weight, item.doucmentId);
		}
	}
	
	public static void main(String[] args) {
		Engine e = new Engine();
		e.abb();
	}
	
}


//// int hash = nchar.hashCode() % HASH_SIZE;
//// module是基础值，必须是一个大于的正整数，这里定义为1000，必须与diffuse中的分割值一致
//Int hash = new Int((short) 2, nchar.hashCode() % HASH_SIZE);
//row.add(hash);
//
//b = set.values();

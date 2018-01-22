/**
 *
 */
package com.lexst.debug;

import java.awt.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import javax.swing.*;

import org.w3c.dom.*;

import com.lexst.algorithm.*;
import com.lexst.algorithm.collect.*;
import com.lexst.algorithm.util.*;
import com.lexst.fixp.*;
import com.lexst.fixp.client.*;
import com.lexst.remote.*;
import com.lexst.remote.client.call.*;
import com.lexst.remote.client.data.*;
import com.lexst.remote.client.work.*;
import com.lexst.site.call.*;
import com.lexst.sql.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.charset.codepoint.*;
import com.lexst.sql.chunk.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.index.*;
import com.lexst.sql.index.balance.*;
import com.lexst.sql.index.chart.*;
import com.lexst.sql.index.range.*;
import com.lexst.sql.index.section.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.sort.*;
import com.lexst.sql.util.*;
import com.lexst.util.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.xml.*;
import com.lexst.util.naming.*;
import com.lexst.util.range.*;

/**
 * @author siven
 *
 */
public class Debug {

	/**
	 *
	 */
	public Debug() {
		super();
	}
	
	/**
	 * 二分查找, 数组已经排序
	 * @param b
	 * @param off
	 * @param len
	 * @param itemoff (行开始下标)
	 * @return
	 */
	public int searchChop(byte[] b, int off, int len, int itemoff) {
		int left = 0;
		int right = (len / 8) - 1;
		
		while (left <= right) {
			int mid = (left + right) / 2;
			int seek = mid * 8; // off + (mid * 8);
			
			int offset = Numeric.toInteger(b, seek, 4);
			// int index2off = Numeric.toInteger(b, seek + 4, 4);

			System.out.printf("left:%d, right:%d, middle:%d, seek:%d, item-off:%d\n", left, right, mid, seek, offset);

			if (itemoff > offset) {
				left = mid + 1;
			} else if(itemoff < offset) {
				right = mid - 1;
			} else {
				int index2off = Numeric.toInteger(b, seek + 4, 4);
				return index2off;
//				return mid;
			}
		}

		return -1; // not found
	}
	
	public void binaryChop() {
		int[] values = new int[10];
		java.util.Random rnd =  new java.util.Random(System.currentTimeMillis());
		for(int i = 0; i < values.length; i++) {
			values[i] = i;
			values[i] = rnd.nextInt();
		}
		
		java.io.ByteArrayOutputStream buff = new java.io.ByteArrayOutputStream();
		for(int i = 0; i < values.length; i++) {
			byte[] b = Numeric.toBytes(values[i]);
			buff.write(b, 0, b.length);
			
			b = Numeric.toBytes(i + 200);
			buff.write(b, 0, b.length);
		}
		
		byte[] b = buff.toByteArray();
		System.out.printf("stream length:%d\n", b.length);
		int offset = searchChop(b, 0, b.length, 1);
		if(offset > 0 ) {
			System.err.println("OKA!!!");
			java.awt.Toolkit.getDefaultToolkit().beep();
		}
		System.out.printf("binary chop offset is:%d\n", offset);
	}
	
	public void test101() {
		com.lexst.home.Launcher launcher = com.lexst.home.Launcher.getInstance();
		Space[] s = launcher.balance(10);
		
		System.out.printf("balance size is:%d\n", (s == null ? -1 : s.length));
		
		for (int i = 0; s != null && i < s.length; i++) {
			System.out.printf("space is:%s\n", s[i]);
		}
	}
	
	public void test102() {
		File file = new File("d:/result.bin");
		int len = (int)file.length();
		
		byte[] b = new byte[len];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(b);
			in.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		
		short id = 1;
		Table table = new Table();
		table.add(new com.lexst.sql.column.attribute.RawAttribute(id++, "raw", "abc".getBytes()));
		table.add(new com.lexst.sql.column.attribute.ShortAttribute(id++, "short", Short.MAX_VALUE));
		table.add(new com.lexst.sql.column.attribute.TimestampAttribute(id++, "stamp", Long.MAX_VALUE));
		
		ArrayList<Row> a = new ArrayList<Row>();
		for(int off = 0; off < b.length;) {
			Row row = new Row();
			int size = row.resolve(table, b, off, b.length-off);
			if(size < 1) {
				System.out.println("error!"); break;
			}
			off += size;
			a.add(row);
		}
		
		System.out.printf("array size:%d\n", a.size());
		
		Row row = a.get(0);
		for(com.lexst.sql.column.Column col : row.list()) {
			System.out.printf("id:%d, %s\n", col.getId(), Type.showDataType( col.getType()) );
		}
		
		System.out.println("finished!");
	}
	
	public void test100() {
		java.util.List<Row> a = new ArrayList<Row>();
		
		for(int i = 1; i <= 10; i++) {
			short id = 1;
			Row row = new Row();
			
			byte[] bytes = String.format("RAW:%d-%d", i, id).getBytes();
			row.add(new com.lexst.sql.column.Raw(id++, bytes));
			bytes = String.format("CHAR:%d-%d", i, id).getBytes();
			row.add(new com.lexst.sql.column.Char(id++, bytes));
			bytes = String.format("NCHAR:%d-%d", i, id).getBytes();
			row.add(new com.lexst.sql.column.SChar(id++, bytes));
			bytes = String.format("WCHAR:%d-%d", i, id).getBytes();
			row.add(new com.lexst.sql.column.WChar(id++, bytes));

			row.add(new com.lexst.sql.column.Short(id++, Short.MAX_VALUE));
			row.add(new com.lexst.sql.column.Integer(id++, Integer.MAX_VALUE));
			row.add(new com.lexst.sql.column.Long(id++, Long.MAX_VALUE));
			row.add(new com.lexst.sql.column.Float(id++));
			row.add(new com.lexst.sql.column.Double(id++));

			row.add(new com.lexst.sql.column.Time(id++, com.lexst.util.datetime.SimpleTime.format()));
			row.add(new com.lexst.sql.column.Date(id++, com.lexst.util.datetime.SimpleDate.format()));
			row.add(new com.lexst.sql.column.Timestamp(id++, com.lexst.util.datetime.SimpleTimestamp.format()));
			
			a.add(row);
		}
		
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
//		int filend = 0;
//		for(Row row :a) {
//			byte[] b = row.build(filend);
//			filend += b.length;
//			buff.write(b, 0, b.length);
//		}
		
		for(Row row :a) {			
			row.build(buff);
		}
		
		byte[] b = buff.toByteArray();
		try {
			FileOutputStream writer = new FileOutputStream("d:/raw.bin");
			writer.write(b, 0, b.length);
			writer.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

	public void test() {
		String key = "java.library.path";
		String s = System.getProperty(key);
		System.out.println(s);

		java.util.Random rnd = new java.util.Random(System.currentTimeMillis());
		byte[] b = new byte[128];
		for(int i = 0; i<b.length; i++) {
			b[i] = (byte)'a';
		}
		for (int i = 0; i < 10; i++) {
			rnd.nextBytes(b);
			long value = com.lexst.util.Sign.sign(b, 0, b.length);
			System.out.printf("%d - %X\n", value, value);
		}
	}

	public void test2() {
		java.util.TreeSet<Long> set = new java.util.TreeSet<Long>();
		java.util.Random rnd = new java.util.Random(System.currentTimeMillis());
		byte[] b = new byte[128];
		rnd.nextBytes(b);

		long begin = System.currentTimeMillis();
		int count = 0;
		int num = 6000000;
		for (int i = 0; i < num; i++) {
			rnd.nextBytes(b);
			long value = com.lexst.util.Sign.sign(b, 0, b.length);
			if (set.contains(value)) {
				count++;
				continue;
			}
			set.add(value);
		}

		long usedtime = System.currentTimeMillis() - begin;
		System.out.printf("usedtime:%d\n", usedtime);
		System.out.printf("result:%d - %d\n", num , count);
	}

	public void test3() {
		String key = "java.library.path";
		String s = System.getProperty(key);
		System.out.println(s);

		for (int size = 8; size <= 1024; size += 64) {
			byte[] b = new byte[size];
			for (int i = 0; i < b.length; i++) {
				b[i] = (byte) 'a';
			}
			for (int i = 0; i < 2; i++) {
				long value = com.lexst.util.Sign.sign(b, 0, b.length);
				System.out.printf("%d - %X\n", value, value);
			}
		}
	}

	public void test5() {
		byte[] word = new byte[38];
		for(int  i = 0; i <word.length; i++) word[i] = (byte)'a';

		java.util.zip.CRC32 hand = new java.util.zip.CRC32();
		hand.update(word, 0, word.length);
		hand.update(word, 0, word.length);
		long checksum = hand.getValue();
		System.out.printf("check sum %d - %x\n", checksum, checksum);
	}

	public void test6() {
		int size = 16 * 1024 * 1024;
		byte[] b = new byte[size];
		for(int i = 0; i < b.length; i++) {
			b[i] = 1;
		}

		long time = System.currentTimeMillis();
		int count = 0;
		for(int off = 0; off < size; off += 64) {
			int num = com.lexst.util.Numeric.toInteger(b, off, 4);
			count++;
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("count %d, used time:%d\n", count, usedtime);
	}

	public void test7() {
		int seed_size = 10240;
		byte[] seed = new byte[seed_size];
//		java.util.Random rnd = new java.util.Random(System.currentTimeMillis());
//		rnd.nextBytes(seed);

		for(int i = 0; i < seed.length; i++) seed[i] = 1;

		int data_size = 16 * 1024 * 1024;
		byte[] data = new byte[data_size];
		for(int i =0; i < data.length; i++) data[i] = 2;

		long time = System.currentTimeMillis();
		for(int i = 0, n = 0; i<data.length; i++, n++) {
			if(n == seed.length) {
				n = 0;
			}
			data[i] ^= seed[n];
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("XOR TIME:%d\n", usedtime);
	}

	public void test8() {
		int size = 10 * 1024 * 1024;
		char[] s = new char[size];
		for(int i =0; i <s.length; i++) {
			s[i] = 'A';
		}
		String buf = new String(s);

		long time = System.currentTimeMillis();
		int hash = buf.hashCode();
		long end = System.currentTimeMillis();

		System.out.printf("hash used time %d -  hash: %d\n", end - time, hash);
	}

	// 只可以它本身和1整除
	private boolean isLeft(int value) {
		int middle = value / 2;
		for (int pot = 2; pot <= middle; pot++) {
			if (value % pot == 0) return false;
		}
		return true;
	}

	public void test9() {
		int min = 0xdfcfef1, max = Integer.MAX_VALUE;
//		int min = 0xdeadbeef / 10 * 5, max = Integer.MAX_VALUE;
		int count = 0;
		for (int value = min; value < max; value++) {
			if (value % 2 == 0)
				continue;
			if (isLeft(value)) {
				System.out.printf("%d - %x\n", value, value);
				count++;
				if (count >= 20000) break;
			}
		}
		System.out.printf("count is %d\n", count);
	}

	public void test10() {
		long mod, value = 0xdeadbeefL; // 0x3735928559l;
		long middle = value -1; // / 2 + 1;
		for (mod = 2; mod < middle; mod++) {
			if (value % mod == 0) {
				System.out.printf("mod: %d - %x\n", mod, mod);
			}
		}
		System.out.println("finished!");
	}

	public void test11() {
		int size = 1024;
//		java.io.File[] files = new java.io.File[size];
		String[] files = new String[size];

		for (int i = 0; i < size; i++) {
			files[i] = String.format("f:/duplex/file%d.txt", i + 1);
		}

		System.out.println("into file");
		long time = System.currentTimeMillis();
		byte[] b = new byte[256 * 1024];

		for(String s : files) {
			try {
				java.io.FileOutputStream out = new java.io.FileOutputStream(s);
				out.write(b, 0, b.length);
				out.close();
			} catch(IOException exp) {
				exp.printStackTrace();
			}
		}
		System.out.printf("create file, used time %d\n", System.currentTimeMillis()-time);

		System.out.println("open file");
		time = System.currentTimeMillis();
		java.io.FileInputStream[] in = new java.io.FileInputStream[size];
		for(int i = 0; i < size; i++) {
			try {
				in[i] = new java.io.FileInputStream(files[i]);
			} catch(IOException exp) {
				exp.printStackTrace();
			}
		}
		System.out.printf("create file, used %d\n", System.currentTimeMillis() - time);

		// read data
		System.out.println("read file");
		time = System.currentTimeMillis();
		for(int i = 0; i < size; i++) {
			try {
				in[i].read(b);
			} catch(IOException exp) {
				exp.printStackTrace();
			}
		}
		System.out.printf("read used time %d\n", System.currentTimeMillis()-time );

		// close file
		System.out.println("close file!");
		for (int i = 0; i < size; i++) {
			try {
				in[i].close();
			} catch (IOException exp) {
				exp.printStackTrace();
			}
		}
	}

	public void test12() {
		long value = Long.MIN_VALUE;
		System.out.printf("%d - %x\n", value, value);

		long time = System.currentTimeMillis();
		while (value < 0) { //Long.MAX_VALUE) {
			value++;
			if (value == 0) value += 1;
		}
		long end = System.currentTimeMillis();
		System.out.printf("usedtime: %d \n", end - time);
	}

	public void test13() {
		int value = Integer.MIN_VALUE;
		System.out.printf("%d - %x\n", value, value);
		long time = System.currentTimeMillis();
		while (value < 0) {
			value++;
		}
		long end = System.currentTimeMillis();
		System.out.printf("usedtime: %d \n", end - time);

		value = Integer.MIN_VALUE;
		time = System.currentTimeMillis();
		while (value < Integer.MAX_VALUE) {
			value++;
		}
		end = System.currentTimeMillis();
		System.out.printf("usedtime: %d\n", end - time);
	}
	
	public void test14() {
		CallClient client = new CallClient(true);
		
		System.out.printf("%s\n", client.getClass().getName());
	}
	
	public void test15() {
		
		java.lang.NullPointerException exp = new java.lang.NullPointerException("THIS IS NULL POINTER TEST");
		Reply reply = new Reply(null, exp);
		String s = reply.getThrowText();
		
//		System.out.printf("s size is %d\n", s.length());
		System.out.println(s);
//		Throwable e = reply.getThrowable();
//		e.printStackTrace();
		
		VisitException vis = new VisitException(s);
		vis.printStackTrace();
		
		System.out.println("------------");
		
		VisitException v2 = new VisitException( reply.getThrowable() );
		v2.printStackTrace();
	}
	
	public void test16() {
		String cls = System.getProperty("java.library.path");
		System.out.printf("%s\n", cls);
		
		byte[] b = "pentium".getBytes();
		long value = com.lexst.util.Sign.sign(b, 0, b.length);
		System.out.printf("sign %d - %x\n", value, value);
	}

	public void test17() {
		long chunkid = Long.MAX_VALUE;
		byte rank = 1;
		byte status = 1;
		short cid = 1;
		
		IndexRange sign = new IntegerIndexRange(chunkid, cid, 1000, 2000);
		ChunkAttribute cs = new ChunkAttribute(chunkid, rank, status);
		cs.add(sign);

		IndexRange sign2 = new IntegerIndexRange(chunkid - 1, cid, 1200, 3000);
		ChunkAttribute cs2 = new ChunkAttribute(chunkid-1, rank, status);
		cs2.add(sign2);

//		columnId = 2;
//		begin = 1200; end = 3000;
//		index = new IntSignRange(chunkId, columnId, begin, end);
//		sheet.add(index);
		
		Space space = new Space("db", "table");
		IndexSchema db = new IndexSchema();
		db.add(space, cs);
		db.add(space, cs2);
		
		IndexTable indexTable = db.find(space);
		
		// check ...
		SocketHost host = null;
		try {
			host = new SocketHost(SocketHost.TCP, "128.23.12.35", 9000);
		} catch (UnknownHostException exp) {
			
		}
		Map<Space, IndexModule> mapIndex = new HashMap<Space, IndexModule>();
		
		IndexModule module = mapIndex.get(space);
		if(module == null) {
			module = new IndexModule(space);
			mapIndex.put(space, module);
		}
		for(long chunkId : indexTable.keys()) {
			ChunkAttribute sheet = indexTable.find(chunkId);
			for (short columnId : sheet.keys()) {
				IndexRange index = sheet.find(columnId);
//				module.add(host, index);
			}
			
//			// save data site
//			set = mapHost.get(chunkId);
//			if (set == null) {
//				set = new HostSet();
//				mapHost.put(chunkId, set);
//			}
//			set.add(host);
		}
		
		ChunkSet array = new ChunkSet();
		com.lexst.sql.column.Integer column = new com.lexst.sql.column.Integer(cid, 1500);
		IntegerIndex is = new IntegerIndex(1500, column);
		Condition condi = new Condition("abc", Condition.LESS, is);
		int count = module.find(condi, array);
		System.out.printf("count is %d\n", count);
		
		for(long chunkId : array.list()) {
			System.out.printf("%x - %d\n", chunkId, chunkId);
		}
	}
	
	private int buildLikeId(int colId) {
		return colId | 1024;
	}
	private boolean isLikeId(int colId) {
		return (colId & 1024) == 1024;
	}
	
	public void test18() {
//		int colId = 1025;
//		int value = colId & 1023;
//		System.out.printf("%d - %x\n", value, value);
		
//		int colId = 6;
//		int value = buildLikeId(colId); // colId | 1024;
//		System.out.printf("%d - %x\n", value, value);
//		
//		System.out.printf("like identity %b\n", isLikeId(value));
//		
//		value = value & 1024;
//		System.out.printf("%d - %x\n", value, value);
//		
//		value = colId & 1023;
//		System.out.printf("%d - %x\n", value, value);

//		long value = 0x514b8522c49e41b0L;
//		long min = 0x64d7ec542041L;
//		long gap = 0x588f5a2b8222L;
//		
//		long index1off = (value - min) / gap;
//		System.out.printf("index1 offset %d - %x\n", index1off, index1off);
//		
//		gap+=1;
//		index1off = (value - min) / gap;
//		System.out.printf("index1 offset %d - %x\n", index1off, index1off);
		
		
		long max = 0x7fffbf1230ba097cl;
		long min = 0x7b8313f8169el;
		long index1count = 91590L;
		
		long gap = (max - min) / index1count;
		gap++;
		System.out.printf("gap:%x - %d\n", gap, gap);
		
	}
	
	public void test19() {
//		long milli = System.currentTimeMillis();
//		long nano = System.nanoTime();
//		System.out.printf("%d - %d\n", milli, nano);
		
		ArrayList<Integer> array = new ArrayList<Integer>(100);
		for(int i = 0; i < 50; i++) {
			array.add(i);
		}
		System.out.printf("array size:%d\n", array.size());
		
		array.ensureCapacity(10);
		
		System.out.printf("array size:%d\n", array.size());
	}
	
	public void test20() {
		String regex = String.format("([\\p{Print}]*)\\%c([a-fA-F0-9]{16})(?i)\\.lxdb", File.separatorChar);
		Pattern pattern = Pattern.compile(regex);
		
		String filename = "c:\\windows\\8000000070000000.lxdb";
		Matcher matcher = pattern.matcher(filename);
		if(!matcher.matches()) return;
		
		String prefix = matcher.group(1);
		String value = matcher.group(2);
//		value = "8000000080000000";
		long high = Long.parseLong(value.substring(0, 8), 16);
		long low = Long.parseLong(value.substring(8, 16), 16);
		long id = (high << 32) | low;
//		long id = Long.parseLong(value, 16);
//		Long id = Long.valueOf(value, 16);
		System.out.printf("%s\n", regex);
		System.out.printf("%s - %s | %d - %x\n", prefix, value, id, id);
//		long id = Long.parseLong(value, 16);
	}
	
	public void test21() {
//		long[] chunkId =  { 0x8000000000000000L, 0x8000000000000001L };
		
		long[] chunkIds = new long[100];
		long value = 0;// Long.MIN_VALUE+100;
		for (int i = 0; i < chunkIds.length; i++) {
			chunkIds[i] = value++;
		}
		int size = 3;
		for (int i = 0; i < chunkIds.length; i++) {
			int remainder = (int) (chunkIds[i] % size);
			if(remainder < 0) remainder = Math.abs(remainder);
			System.out.printf("%x - %d\n", chunkIds[i], remainder);
		}
	}
	
	public void test22() {
		short cid = 18;
		byte[] b = "Pentiu".getBytes();
		SCharAttribute field = new SCharAttribute(cid, "word", b);
		field.setSentient(false);

		Map<com.lexst.sql.column.SChar, Integer> mapWord = new HashMap<com.lexst.sql.column.SChar, Integer>(1024);
		int size = 256;
		long time = System.currentTimeMillis();
		
		for(int i = 0; i < size; i++) {
			b = String.format("Value:%d", i).getBytes();
			com.lexst.sql.column.SChar word = new com.lexst.sql.column.SChar(cid, b);
//			word.setProperty(field);
			mapWord.put(word, i);
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("nchar value is:%d, usedtime:%d\n", mapWord.size(), usedtime);
		
		Map<Integer, Integer> mapInt = new HashMap<Integer, Integer>();
		time = System.currentTimeMillis();
		for(int i = 0; i < size; i++) {
			mapInt.put(i, i);
		}
		usedtime = System.currentTimeMillis() - time;
		System.out.printf("int value is:%d, usedtime:%d\n", mapInt.size(), usedtime);
		
		Map<String, Integer> mapStr = new HashMap<String, Integer>();
		time = System.currentTimeMillis();
		for(int i = 0; i < size; i++) {
			String s = String.format("value:%d", i);
			mapStr.put(s, i);
		}
		usedtime = System.currentTimeMillis() - time;
		System.out.printf("string value is:%d, usedtime:%d\n", mapStr.size(), usedtime); 
	}
	
	public void test23() {
		try {
		SiteHost[] primes = new SiteHost[] { new SiteHost("10.1.2.33", 101, 102)};
		SiteHost[] slaves = new SiteHost[] { new SiteHost("122.333.44", 201, 202) };
		
		SiteHost[] hosts = new SiteHost[primes.length + slaves.length];
		System.arraycopy(primes, 0, hosts, 0, primes.length);
		System.arraycopy(slaves, 0, hosts, primes.length, slaves.length);

		for(SiteHost host : hosts) {
			System.out.printf("%s\n", host);
		}
		} catch (IOException e) {
			
		}
	}
	
	public void test24() {
		Console console = System.console();
		if(console == null) {
			System.out.println("console is null");
			return ;
		}
		String s = console.readLine();
		System.out.println(s);
	}
	
	public void test25() {
		String regex = "^\\s*(?:(?i)Net|com)\\s+(\\w*)\\s*$";
		String abc = "net unixsystem";
		java.util.regex.Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(abc);
		if(!matcher.matches()) {
			System.out.println("not match!");
		} else {
			String s = matcher.group(1);
			System.out.printf("group:%s\n", s);
		}
	}
	
	public void test26() {
		String regex = "^\\s*\\&\\#(\\d+)\\;(.+)\\s*$";
		String text = "&#54644;&#50868;&#45824;";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(text);
		if (!matcher.matches()) return;

		String s1 = matcher.group(1);
		String s2 = matcher.group(2);
		System.out.printf("%s - %s\n", s1, s2);
		
		int ch = Integer.parseInt(s1);
		char c = (char) ch;
		System.out.printf("%c\n", c);
	}
	
	public void test27() {
		String s = "%e7%89%9b%e9%83%8e%e7%bb%87%e5%a5%b3%0d%0a%e5%b0%91%e5%b9%b4%e5%8c%85%e9%9d%92%e5%a4%a9i%0d%0a%e5%b0%91%e5%b9%b4%e5%8c%85%e9%9d%92%e5%a4%a9i%0d%0a%e5%ae%b6%e5%ba%ad%e6%95%99%e5%b8%88%0d%0a%e5%ae%b6%e5%ba%ad%e6%95%99%e5%b8%88%0d%0a%e8%b5%b0%e8%bf%87%e8%b7%af%e8%bf%87%e8%8e%ab%e9%94%99%e8%bf%87%0d%0a%e7%8b%99%e5%87%bb%e6%89%8b%0d%0a%e7%8b%99%e5%87%bb%e6%89%8b%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e5%90%8e%e6%9d%a5%0d%0a%e7%8b%99%e5%87%bb%e6%89%8b%0d%0a%e7%8b%99%e5%87%bb%e6%89%8b%0d%0aevans%0d%0a%e5%a5%a0%e5%9f%ba%0d%0a%e5%b0%86%e5%86%9b%0d%0a%e5%a6%bb%e5%ad%90%e7%9a%84%e8%af%b1%e6%83%91%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e6%84%8f%e5%a4%96%0d%0a%e7%bb%88%e6%9e%81%e4%b8%80%e5%ae%b6%0d%0a%e7%bb%88%e6%9e%81%e4%b8%80%e5%ae%b6%0d%0a%e7%88%b8%e7%88%b8%0d%0a%e7%88%b8%e7%88%b8%0d%0a%e7%aa%81%e5%8f%91%e4%ba%8b%e4%bb%b6%0d%0a%e6%9b%bf%e8%ba%ab%0d%0a%e5%ae%89%e5%a8%9c%0d%0a%e5%ae%89%e5%a8%9c%0d%0a%e5%8e%86%e5%8f%b2%e7%9a%84%e8%bf%9b%e7%a8%8b%0d%0a2ne1%0d%0a%e6%9d%a8%e8%b4%b5%e5%a6%83%e7%a7%98%e5%8f%b2%0d%0a%e6%9d%a8%e8%b4%b5%e5%a6%83%e7%a7%98%e5%8f%b2%0d%0a%e5%8f%b0%e6%b9%be%e7%a5%9e%e5%a5%87%e5%b0%8f%e7%8b%97%0d%0a%e6%9e%81%e5%93%81%e9%a3%9e%e8%bd%a6%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e7%bb%9d%e5%af%86%e6%8a%bc%e8%bf%90%0d%0a%e6%9d%a8%e8%b4%b5%e5%a6%83%e7%a7%98%e5%8f%b2%0d%0a%e5%be%80%e4%ba%8b%0d%0a%e6%af%8d%e4%ba%b2%0d%0a%e5%be%80%e4%ba%8b%0d%0a%e8%bf%b7%e9%9b%be%e9%87%8d%e9%87%8d%0d%0a%e4%b8%a4%e4%b8%aa%e5%a6%bb%e5%ad%9006%0d%0a%e4%b8%a4%e4%b8%aa%e5%a6%bb%e5%ad%9006%0d%0a%e5%b7%a6%e5%8f%b3%0d%0a%e5%a8%98%e5%ae%b6%e7%9a%84%e6%95%85%e4%ba%8b%0d%0a%e4%ba%b2%e6%83%85%0d%0a%e9%ad%94%e6%9c%af%0d%0a%e9%ad%94%e6%9c%af%0d%0a%e5%ae%b6%e5%ba%ad%e6%95%99%e5%b8%88%0d%0a%e5%ae%b6%e5%ba%ad%e6%95%99%e5%b8%88%0d%0a%e6%90%9e%e7%ac%91%0d%0a%e9%9b%be%e9%87%8c%e7%9c%8b%e8%8a%b1%0d%0a%e5%a6%bb%e5%ad%90%e7%9a%84%e8%af%b1%e6%83%91%0d%0a%e5%a6%bb%e5%ad%90%e7%9a%84%e8%af%b1%e6%83%91%0d%0a%e5%8e%86%e5%8f%b2%e7%9a%84%e8%bf%9b%e7%a8%8b%0d%0a%e7%81%ab%e5%bd%b1%e5%bf%8d%e8%80%85%0d%0a%e7%81%ab%e5%bd%b1%e5%bf%8d%e8%80%85%0d%0a%e8%b4%a5%e7%8a%ac%e5%a5%b3%e7%8e%8b%0d%0a%e8%b4%a5%e7%8a%ac%e5%a5%b3%e7%8e%8b%0d%0a%e6%9d%a8%e8%b4%b5%e5%a6%83%e7%a7%98%e5%8f%b2%0d%0a%e6%9d%a8%e8%b4%b5%e5%a6%83%e7%a7%98%e5%8f%b2%0d%0a%e6%90%9e%e7%ac%91%e8%a7%86%e9%a2%91%0d%0a%e8%8f%b2%e6%9d%8e%e4%ba%9a%e9%b9%8f%0d%0a%e8%8f%b2%e6%9d%8e%e4%ba%9a%e9%b9%8f%0d%0a%e8%87%aa%e7%84%b6%e5%85%bb%e7%8c%aa%e6%8a%80%e6%9c%af%0d%0a%e8%87%aa%e7%84%b6%e5%85%bb%e7%8c%aa%e6%8a%80%e6%9c%af%0d%0a%e5%ae%9d%e8%8e%b2%e7%81%af%e5%89%8d%0d%0a%e5%ae%9d%e8%8e%b2%e7%81%af%e5%89%8d%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e6%bd%9c%e4%bc%8f2%e5%89%91%e8%b0%8d10%0d%0a%e6%bd%9c%e4%bc%8f2%e5%89%91%e8%b0%8d10%0d%0a%e5%be%ae%e7%ac%91%e5%9c%a8%e6%88%91%e5%bf%83%0d%0a%e5%be%ae%e7%ac%91%e5%9c%a8%e6%88%91%e5%bf%83%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e5%90%8e%e7%be%bf%e5%b0%84%e6%97%a5%0d%0a%e5%90%8e%e7%be%bf%e5%b0%84%e6%97%a5%0d%0a010%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e5%a4%a9%e5%a4%a9%e5%a4%a9%e6%99%b4%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e8%8b%b1%e9%9b%84%0d%0a%e8%93%9d%e8%89%b2%e6%a1%a3%e6%a1%88%e5%85%a8%e9%9b%86%0d%0a%e5%a6%bb%e5%ad%90%e7%9a%84%0d%0a%e7%81%ab%e5%bd%b1%e5%bf%8d%e8%80%85194%0d%0a%e7%81%ab%e5%bd%b1%e5%bf%8d%e8%80%85194%0d%0a%e5%a5%b3%e4%ba%ba%e8%8a%b106a%0d%0a%e5%a5%b3%e4%ba%ba%e8%8a%b106a%0d%0a%e5%90%b8%e8%a1%80%e9%ac%bc%0d%0a%e6%ad%bb%e7%a5%9e%0d%0a%e6%ad%bb%e7%a5%9e%0d%0aac%e7%b1%b3%e5%85%b0%0d%0a7%0d%0a7%0d%0a%e7%88%b1%e4%b8%8d%e7%88%b1%e6%88%91%0d%0a%e7%88%b1%e4%b8%8d%e7%88%b1%e6%88%91%0d%0a%e4%b8%83%e9%be%99%e7%8f%a0%0d%0a%e7%bb%87%e5%8f%98%e6%9b%b4%ef%bc%88%e4%b8%80%ef%bc%89%0d%0a%e7%bb%87%e5%8f%98%e6%9b%b4%ef%bc%88%e4%b8%80%ef%bc%89%0d%0a%e5%a4%8d%e4%bb%87%0d%0a%e5%a4%8d%e4%bb%87%0d%0a%e6%96%b0%e7%89%9b%e9%83%8e%e7%bb%87%e5%a5%b3%0d%0a%e6%9b%be%e5%93%a5%e5%a4%a7%e5%b1%95%e6%ad%8c%e5%96%89%0d%0a%e6%9b%be%e5%93%a5%e5%a4%a7%e5%b1%95%e6%ad%8c%e5%96%89%0d%0a%e5%a5%8b%e6%96%97%0d%0a%e9%94%81%e6%b8%85%e7%a7%8b%0d%0a%e9%94%81%e6%b8%85%e7%a7%8b%0d%0a%e5%a4%ab%e5%a6%bb%e4%b8%80%e5%9c%ba%0d%0a%e5%88%86%e6%89%8b%0d%0a%e6%98%a5%e5%8e%bb%e6%98%a5%e5%8f%88%e5%9b%9e%0d%0a%e6%88%91%e7%88%b1%e6%e6%88%91%e7%9a%84%e4%b8%91%e5%a8%98%0d%0a%e6%88%91%e7%9a%84%e4%b8%91%e5%a8%98%0d%0a%e7%88%b8%e7%88%b8%0d%0amv%0d%0a%e7%8a%ac%e5%a4%9c%e5%8f%89101%0d%0a%e7%8a%ac%e5%a4%9c%e5%8f%89101%0d%0a%e7%bb%88%e6%9e%81%e4%b8%89%e5%9b%bd15%0d%0a%e7%bb%88%e6%9e%81%e4%b8%89%e5%9b%bd15%0d%0a%e7%8b%99%e5%87%bb%0d%0a%e7%94%9f%e6%ad%bb%e7%ba%bf%0d%0a%e7%8b%99%e5%87%bb%0d%0a%e7%94%9f%e6%ad%bb%e7%ba%bf%0d%0a%e6%88%91%e6%98%af%e4%b8%80%e6%a3%b5%e5%b0%8f%e8%8d%89%0d%0a%e6%88%91%e6%98%af%e4%b8%80%e6%a3%b5%e5%b0%8f%e8%8d%89%0d%0a%e5%a5%8b%e6%96%97%0d%0a%e7%bb%88%e6%9e%81%e4%b8%80%e5%ae%b651%0d%0a%e7%bb%88%e6%9e%81%e4%b8%80%e5%ae%b651%0d%0a%e5%b9%bf%e5%91%8a%0d%0a%e5%b9%bf%e5%91%8a%0d%0a%e7%aa%81%e5%8f%91%e4%ba%8b%e4%bb%b6%0d%0a%e7%a5%9e%e5%8c%bb%e5%a4%a7%e9%81%93%e5%85%ac%0d%0a%e5%8f%91%e9%85%b5%e5%ba%8a%e5%85%bb%e7%8c%aa%0d%0a%e5%8f%91%e9%85%b5%e5%ba%8a%e5%85%bb%e7%8c%aa%0d%0a%e7%8e%9b%e5%88%a9%e4%ba%9a%e7%8b%82%e7%83%ad2%0d%0a%e7%8e%9b%e5%88%a9%e4%ba%9a%e7%8b%82%e7%83%ad2%0d%0astyle%0d%0a%e7%94%b5%e5%bd%b1%0d%0a%e7%94%b5%e5%bd%b1%0d%0a%e5%ae%88%e6%8a%a4%e7%94%9c%e5%bf%83%0d%0a%e5%ae%88%e6%8a%a4%e7%94%9c%e5%bf%83%0d%0a%e6%81%8b%e4%ba%ba%0d%0a%e5%be%ae%e7%ac%91%e5%9c%a8%e6%88%91%e5%bf%83%0d%0a%e5%be%ae%e7%ac%91%e5%9c%a8%e6%88%91%e5%bf%83%0d%0a%e6%88%91%e6%98%af%e4%b8%80%e6%a3%b5%e5%b0%8f%e8%8d%89%0d%0a%e6%88%91%e6%98%af%e4%b8%80%e6%a3%b5%e5%b0%8f%e8%8d%89%0d%0a%e4%ba%ba%e9%b1%bc%e5%b0%8f%e5%a7%90%0d%0a%e7%81%bf%e7%83%82%e7%9a%84%e9%81%97%e4%ba%a7%0d%0a%e7%81%bf%e7%83%82%e7%9a%84%e9%81%97%e4%ba%a7%0d%0a%e5%8d%81%e4%ba%8c%e7%94%9f%e8%82%96%0d%0a%e5%8d%81%e4%ba%8c%e7%94%9f%e8%82%96%0d%0a%e5%a4%a7%e8%88%9e%e5%8f%b0%0d%0a%e5%90%8c%e4%b8%80%e5%b1%8b%e6%aa%90%e4%b8%8b%0d%0a%e6%bd%9c%e4%bc%8f%0d%0a%e6%bd%9c%e4%bc%8f%0d%0a%e8%8f%9c%e9%b8%9f%e5%88%91%e8%ad%a6%0d%0a%e8%8f%9c%e9%b8%9f%e5%88%91%e8%ad%a6%0d%0a%e6%9a%97%e9%a6%99%0d%0a%e6%9a%97%e9%a6%99%0d%0a%e4%b9%90%e6%b4%bb%e5%ae%b6%e5%ba%ad%0d%0a%e7%88%b1%e4%bd%a0%e5%8d%83%e4%b8%87%e6%ac%a1%0d%0aqq%e4%b8%89%e5%9b%bd%e5%88%b7%e9%92%b1%0d%0aqq%e4%b8%89%e5%9b%bd%e5%88%b7%e9%92%b1%0d%0a%e9%9f%a9%e5%ba%9a%0d%0a%e9%9f%a9%e5%ba%9a%0d%0a%e7%88%b1%e4%bd%a0%e5%ae%9e%e5%9c%a8%e5%a4%aa%e7%b4%af%0d%0aktv%0d%0a%e6%9d%a8%e9%97%a8%e8%99%8e%e5%b0%86%0d%0a%e6%95%a6%e7%85%8c%0d%0a%e6%95%a6%e7%85%8c%0d%0a%e6%95%a6%e7%85%8c%0d%0a%e6%95%a6%e7%85%8c%0d%0a%e6%95%a6%e7%85%8c%0d%0a%e6%95%a6%e7%85%8c%0d%0a%e6%95%a6%e7%85%8c%0d%0a1%e5%8a%aa%e5%b0%94%e5%93%88%e8%b5%a4%0d%0a1%e5%8a%aa%e5%b0%94%e5%93%88%e8%b5%a4%0d%0a%e5%bc%a0%e9%9d%93%e9%a2%96-%e7%94%bb%e5%bf%83%0d%0a%e5%bc%a0%e9%9d%93%e9%a2%96-%e7%94%bb%e5%bf%83%0d%0a%e7%88%b1%e4%bd%a0%e7%9a%84365%e5%a4%a9%0d%0a%e8%af%b8%e8%91%9b%e4%ba%ae%e7%9a%84%e6%88%98%e8%bd%a6%0d%0a%e7%ba%af%e6%b4%81%e7%9a%84%e4%bd%a0%0d%0a%e6%af%8d%e4%ba%b2%0d%0a%e9%9f%a9%e5%89%a7%e8%8a%b1%e6%a0%b7%e7%94%b7%e5%ad%90%0d%0a";
		try {
			s = URLDecoder.decode(s, "utf-8");
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		System.out.println(s);
	}
	
	public void test28() {
//		String s = System.getProperty("java.library.path");
//		System.out.println(s);
		
		long time = SystemTime.get();
		this.printDate(time);
		
		System.out.println("----- data site local time-----------");
		time = 138221489221571L;
		this.printDate(time);
		
		System.out.println("------ network time ----------");
		time = 138221506046747L;
		this.printDate(time);
	}
	
	public void printDate(long time) {
		java.util.Date date = SimpleTimestamp.format(time);
		System.out.println(date);
		java.util.Calendar dar = Calendar.getInstance();
		dar.setTime(date);
		System.out.printf("year:%d\n", dar.get(Calendar.YEAR));
		System.out.printf("month:%d\n", dar.get(Calendar.MONTH) + 1);
		System.out.printf("day:%d\n", dar.get(Calendar.DAY_OF_MONTH));
		System.out.printf("hour:%d\n", dar.get(Calendar.HOUR_OF_DAY));
		System.out.printf("minute:%d\n", dar.get(Calendar.MINUTE));
		System.out.printf("second:%d\n", dar.get(Calendar.SECOND));
		System.out.printf("milli-second:%d\n", dar.get(Calendar.MILLISECOND));
	}
	
	public void test29(int a, int b) {
		int num = a % b;
		while (a >= b) {
			a = a - b;
		}
		System.out.printf("%d - %d - %d\n", num, a, b);

//		System.out
	}
	
	public void test30() {
		int size = 1024 * 1024;
		long count = size * 2;
		Map<Long, Integer> map1 = new HashMap<Long, Integer>(size);
		long time = System.currentTimeMillis();
		for(long since = 1; since < count; since+=2) {
			map1.put(since, 1);
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("hashmap long usedtime:%d, capacity:%d\n", usedtime, map1.size());
		map1.clear();
		
		Map<Long, Integer> map2 = new TreeMap<Long, Integer>();
		time = System.currentTimeMillis();
		for (long since = 1; since < count; since += 2) {
			map2.put(since, 1);
		}
		usedtime = System.currentTimeMillis() - time;
		System.out.printf("treemap long usedtime:%d, capacity:%d\n\n", usedtime, map2.size());
		map2.clear();
	}

	public void test31() {
		int size = 1024 * 1024;
		int count = size * 2;
		Map<Integer, Integer> map1 = new HashMap<Integer, Integer>(size);
		long time = System.currentTimeMillis();
		for(int since = 1; since < count; since+=2) {
			map1.put(since, 1);
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("hashmap int usedtime:%d, capacity:%d\n", usedtime, map1.size());
		map1.clear();
		
		Map<Integer, Integer> map2 = new TreeMap<Integer, Integer>();
		time = System.currentTimeMillis();
		for(int since = 1; since < count; since+=2) {
			map2.put(since, 1);
		}
		usedtime = System.currentTimeMillis() - time;
		System.out.printf("treemap int usedtime:%d, capacity:%d\n\n", usedtime, map2.size());
		map2.clear();
	}
	
	public void test32() {
		int size = 1024 * 1024;
		int count = size * 2;
		
		Map<Integer, Block> map2 = new TreeMap<Integer, Block>();
		long time = System.currentTimeMillis();
		for(int since = 1; since < count; since+=2) {
			Block s = new Block(10L);
			map2.put(since, s);
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("block treemap int usedtime:%d, capacity:%d\n", usedtime, map2.size());
		map2.clear();
		
		Map<Integer, Block> map1 = new HashMap<Integer, Block>(count);
		time = System.currentTimeMillis();
		for(int since = 1; since < count; since+=2) {
			Block s = new Block(10L);
			map1.put(since, s);
		}
		usedtime = System.currentTimeMillis() - time;
		System.out.printf("block hashmap int usedtime:%d, capacity:%d\n\n", usedtime, map1.size());
		map1.clear();
	}
	
	public void test33() {
		HashMap<Integer, Integer> map1 = new HashMap<Integer, Integer>();
		for(int i = 0; i < 10; i ++) {
			map1.put(i, i);
		}
		for(int key : map1.keySet()) {
			int value = map1.get(key);
			System.out.printf("%d - %d\n", key, value);
		}
		
		HashMap<String, Integer> map2 = new HashMap<String, Integer>();
		for(int i = 0; i < 10; i ++) {
			String s = String.format("P%d", i);
			map2.put(s, i);
		}
		for(String key : map2.keySet() ) {
			int value = map2.get(key);
			System.out.printf("[%s - %d] - %d\n", key, key.hashCode(), value);
		}
	}
	
	public void test34() {
		String text = "create table video.pentium ( ";
		text = "create database Video char=utf8 nchar=utf16 wchar=utf32";
		text = "create table prime host 1 share host copy 2 chunksize 64m Video.Word(vid long, weight int, type short, site short, high short, show_time short, publish_time int, word nchar not case)";
		int begin = -1;
		for(int index = 0; index < text.length(); index++) {
			char w = text.charAt(index);
			if (com.lexst.util.ASCII.isAlphaDigit(w)) {
				if (begin == -1) begin = index;
			} else {
				if (begin == -1) continue;
				String s = text.substring(begin, index);
				System.out.printf("%s - [%d - %d]\n", s, begin, index - begin);
				begin = -1;
			}
		}
		if(begin > -1) {
			String s = text.substring(begin);
			System.out.printf("last: %s - [%d - %d]\n", s, begin, text.length() - begin);
		}
	}
	
	public void test35() {
		int size = 102400 * 5;
		ArrayList<Integer> array = new ArrayList<Integer>(size);
		Random rnd = new Random(System.currentTimeMillis());
		for (int i = 0; i < size; i++) {
			array.add(rnd.nextInt());
		}

		long time = System.currentTimeMillis();
		java.util.Collections.sort(array);
		long endtime = System.currentTimeMillis();

		System.out.printf("usedtime:%d, size:%d\n", endtime -time, size);
	}
	
	public void test36() {
		int size = 1024 * 1024;
		int half = size / 2;
		int half2 = size / 4;
		byte[] b = new byte[size];
		int count = 1000;
		long t1 = 0, t2 = 0, t3 = 0;
		for (int i = 0; i < count; i++) {
			long time = System.currentTimeMillis();
			for (int off = 0; off < size; off += 8) {
				Numeric.toLong(b, off, 8);
			}
			t1 += System.currentTimeMillis() - time;

			time = System.currentTimeMillis();
			for (int off = 0; off < half; off += 4) {
				Numeric.toInteger(b, off, 4);
			}
			t2 += System.currentTimeMillis() - time;
			
			time = System.currentTimeMillis();
			for(int off = 0; off < half2; off += 2) {
				Numeric.toShort(b, off, 2);
			}
			t3 += System.currentTimeMillis() - time;
		}
		System.out.printf("[%d - %d - %d], long usedtime:%d, int usedtime:%d\n",
				t1, t2, t3, t1/count, t2/count);
	}
	
	public void test37() {
//		int elements = 50048;
//		int size = (3244580 / elements) + 1;
//		byte[] b = new byte[size];
//		
//		CRC32 crc32 = new CRC32();
//		
//		for (int n = 0; n < 10; n++) {
//			long time = System.currentTimeMillis();
//			for (int i = 0; i < elements; i++) {
//				crc32.reset();
//				crc32.update(b, 0, b.length);
//				long value = crc32.getValue();
//			}
//			long endtime = System.currentTimeMillis();
//			System.out.printf("size:%d, usedtime:%d\n", size, endtime - time);
//		}

		int len = 10241;
		byte[] b = new byte[len];
		for (int i = 0; i < len; i++) {
			b[i] = (byte) 'a';
		}
		CRC32 sum = new CRC32();
		sum.update(b, 0, b.length);
		long value = sum.getValue();
		System.out.printf("checksum:%d - %x\n", value, value);
		
	}
	
	public void test38() {
		String filename = "F:/56Download/榴莲飘飘CD1.flv";
		File file = new File(filename);
		
		byte[] data = new byte[(int)file.length()];
		
		long time = System.currentTimeMillis();
		int limit =1;
		try {
		for(int i = 0; i < limit; i++) {
			FileInputStream in =new FileInputStream(file);
			in.read(data, 0, data.length);
			in.close();
		}
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		long usedtime = System.currentTimeMillis() - time;
		System.out.printf("usedtime:%d\n", usedtime);
		
		int M = 1024 * 1024;
		int size = M + 12333;
		size = (size / M) * M + M;
		System.out.printf("size is:%d, %dM\n", size , size /M);
		
	}
	
	public void test39() {
		int capacity = 20000;
		ArrayList<Integer> array = new ArrayList<Integer>(capacity);
		for(int i = 0; i < capacity; i++) {
			array.add(i+2);
		}
		
		System.out.printf("array size is %d\n", array.size());
		
		int check = 2455;
		
		int left = 0;
		int right = array.size() -1;
		while(left <= right) {
			int middle = (right + left) / 2;
			int value = array.get(middle);
			if(value == check) {
				System.out.printf("%d offset is %d - %d\n", check, value, middle);
				break;
			} else if (value < check) {
				left = middle + 1;
			} else {
				right = middle -1;
			}
		}
		System.out.println("find finished!");
	}

	public void test40() {
		Command cmd = new Command(Request.NOTIFY, Request.HELO);
		Command cmd2 = new Command(Request.NOTIFY, Request.HELO);
		Command cmd3 = new Command(Response.ACCEPTED);
		System.out.printf("match is %s\n", cmd.equals(cmd));
		System.out.printf("match 1 is %s\n", cmd.equals(cmd2));
		System.out.printf("match 2 is %s\n", cmd.equals(cmd3));
	}
	
	public void test41() {
//		FixpPacketMonitor monitor = new FixpPacketMonitor();
//		boolean b = monitor.start();
//		System.out.printf("start %s\n", b);
		
		byte[] data = new byte[1024];
		for(int i =0; i < data.length; i++) {
			data[i] = (byte)'a';
		}
		SocketHost remote = null;
		try {
			remote = new SocketHost("localhost", 1989);
		} catch(IOException e) {
			e.printStackTrace();
		}
		Command cmd = new Command(Request.NOTIFY, Request.HELO);
		Packet packet = new Packet(remote, cmd);
		packet.addMessage(Key.SPEAK, "HELLO");
		packet.setData(data);
		
		FixpPacketClient client = new FixpPacketClient();
		client.setSubPacketTimeout(1000);
		client.setReceiveTimeout(20);
		try {
			client.bind();
			Packet resp = client.batch(packet);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		client.close();
	}
	
	public void test42() {
		long[] s = new long[] {10L, 20L, 30L};
		long[] a = new long[s.length];
		System.arraycopy(s, 0, a, 0, a.length);
		for(int i = 0; i < s.length; i ++) {
			System.out.printf("%d - %d\n", s[i], a[i]);
		}
		
		int size = 10;
		Random rnd = new Random();
		for(int i = 0; i < size; i++) {
			int value = rnd.nextInt(size);
			System.out.printf("%d\n", value);
		}
		
		long v1 = Long.MAX_VALUE + Long.MAX_VALUE;
		System.out.printf("max value is:%d\n", v1);
	}
	
	public void test43() {
		String filename = "f:/icons/rsa/server_key.png";
		filename = "f:/icons/rsa/icon_action_calc.png";
		filename = "F:/icons/rsa/holly.gif";
		filename = "F:/icons/rsa/new.gif";
		File file = new File(filename);
		
		byte[] data = new byte[(int)file.length()];
		
		try {
			FileInputStream in =new FileInputStream(file);
			in.read(data, 0, data.length);
			in.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			String s = String.format("%X", data[i]);
			if (s.length() < 2) s = "0" + s;
			
			if (i > 0) {
				sb.append(",");
			}
			sb.append("(byte)0x");
			sb.append(s);
		}
		
		System.out.println(sb.toString());
	}
	
	public void test50() {
		int value = Integer.MIN_VALUE;
		long max = 0xFFFFFFFFL;
		
		long ip = value & max;
		System.out.printf("int:[%d - %X] - IP:[%d - %X]\n", value, value, ip, ip);
	}
	
	public void test51() {
		String path = "java.library.path";
		String s = System.getProperty(path);
		System.out.printf("%s is: %s\n", path, s);
		
		long time = SystemTime.get();
		System.out.printf("time is: %x\n", time);
		
		String address = "www.lexst.com";
		try {
			String ip = java.net.InetAddress.getByName(address).getHostAddress();
			System.out.printf("%s is %s\n", address, ip);
		} catch (java.net.UnknownHostException exp) {
			exp.printStackTrace();
		}
	}
	
	public void test52() throws IOException {
		String value = "tcp://128.9.0.1:9250";
		SocketHost host = new SocketHost(value);
		System.out.printf("%s\n", host);
		
		value = "site://128.0.122.37:1229_2986";
		SiteHost site = new SiteHost(value);
		System.out.printf("%s\n", site);
		
		int min = Integer.MIN_VALUE;
		long param = min & 0xFFFFFFFFL;
		System.out.printf("value:%x - %d\n", param, param);
		
		int m = 1024 * 1024;
		int items = m / 29;
		System.out.printf("item:%d\n", items);
	}
	
	public void test53() {
		Properties keys = System.getProperties();
		Set<Object> set = keys.keySet();
		for (Object key : set) {
			Object value = keys.get(key);
			System.out.printf("%s - %s\n", (String) key, (String) value);
		}
		
		System.out.printf("[%s] [%s]\n", File.pathSeparator, File.separator);
	}

	public void test55() {
		ArrayList<LongAttribute> a = new ArrayList<LongAttribute>();
		ArrayList<LongAttribute> b = new ArrayList<LongAttribute>();
		
		short id = 10;
		LongAttribute field = new LongAttribute();
		field.setColumnId(id);
		
		a.add(field);
		b.add(field);
		
		field.setColumnId(++id);
		
		System.out.printf("a id:%d, b id:%d\n", a.get(0).getColumnId(), b.get(0).getColumnId());
	}
	
	public void test57() {
		Space space = new Space("Video", "Words");
		Table table = new Table(space);
		table.setStorage(Type.DSM);
		table.setChunkSize(64 * 1024 * 1024);
		table.setCopy(3);
		table.setPrimes(1);
		table.setCaching(false);
		table.setMode(Table.SHARE);
		
		short columnId = 1;
		com.lexst.sql.column.attribute.ShortAttribute primary = new com.lexst.sql.column.attribute.ShortAttribute(columnId++, "id", (short)99);
		primary.setKey(Type.PRIME_KEY);
		
		com.lexst.sql.column.attribute.CharAttribute slave = new com.lexst.sql.column.attribute.CharAttribute(columnId, "word", "unix".getBytes());
		slave.setLike(true);
		slave.setKey(Type.SLAVE_KEY);
		
		table.add(primary);
		table.add(slave);
		
		byte[] b = table.build();
		System.out.printf("build byte size:%d\n", b.length);
		
		Table t2 = new Table();
		int len = t2.resolve(b, 0, b.length);
		System.out.printf("resolve size is:%d\n", len);
		System.out.printf("stroage model:%d\n", t2.getStorage());
		System.out.printf("chunk size:%d\n", t2.getChunkSize()/1024/1024);
		
		try {
			FileOutputStream out = new FileOutputStream("d:/head.bin");
			out.write(b);
			out.close();
		} catch(IOException exp) {
			exp.printStackTrace();
		}
	}
	
	public void test578() {
		Space space = new Space("Video", "Words");

		com.lexst.sql.column.Short id1 = new com.lexst.sql.column.Short( (short)1, (short)20 );
		ShortIndex index1 = new ShortIndex((short)20, id1);
		Condition condi1 = new Condition("id", Condition.EQUAL, index1);
		
//		byte[] b = "CHAR1".getBytes();
//		long sortid = com.lexst.util.Sign.sign(b, 0, b.length);
//		System.out.printf("sortid is:%x\n", sortid);

		byte[] b = "CHAR1".toLowerCase().getBytes();
		long sortid = com.lexst.util.Sign.sign(b, 0, b.length);
		System.out.printf("char sortid is:%x\n", sortid);
		
		com.lexst.sql.column.Char id2 = new com.lexst.sql.column.Char((short) 2, b);
		LongIndex index2 = new LongIndex(sortid, id2);
		Condition condi2 = new Condition(Condition.OR, "word", Condition.EQUAL, index2);

		b = "CHAR30".toLowerCase().getBytes();
		sortid = com.lexst.util.Sign.sign(b, 0, b.length);
		System.out.printf("rchar sortid is:%x\n", sortid);
		com.lexst.sql.column.VChar id3 = new com.lexst.sql.column.VChar((short)(0x8000 | 2), b);
		id3.setRange((short)-1, (short)-1);
		LongIndex index3 = new LongIndex(sortid, id3);
		Condition condi3 = new Condition(Condition.OR, "word", Condition.LIKE, index3);
		
		condi1.setLast(condi2);
		condi1.setLast(condi3);
		
		short[] showIds = new short[] { 1, 2 };
		
		Select select = new Select(space);
//		select.setShowId(showIds);
		select.setCondition(condi1);

		b = select.build();
		System.out.printf("select build size is:%d\n", b.length);
		
		Select s2 = new Select();
		int len = s2.resolve(b, 0, b.length);
		System.out.printf("select resovle size is:%d\n", len);
		
		try {
			FileOutputStream s = new FileOutputStream("d:/select.bin");
			s.write(b, 0, b.length);
			s.close();
		} catch (java.io.IOException exp) {
			exp.printStackTrace();
		}
		
		// delete method
		Delete delete = new Delete(space);
		delete.setCondition(condi1);
		b = delete.build();
		System.out.printf("delete build size is:%d\n", b.length);
		
		Delete d2 = new Delete();
		len = d2.resolve(b, 0, b.length);
		System.out.printf("delete resolve size is:%d\n", len);
		
		try {
			FileOutputStream s = new FileOutputStream("d:/delete.bin");
			s.write(b, 0, b.length);
			s.close();
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}
	}

	public void test577() {
		Space space = new Space("Video", "Words");
		short cid = 1;
		Table table = new Table(space);
		table.setStorage(Type.DSM);
		table.setChunkSize(128 * 1024 * 1024);
		table.setCopy(3);
		table.setPrimes(1);
		table.setCaching(false);
		table.setMode(Table.SHARE);
		
		table.add(new com.lexst.sql.column.attribute.ShortAttribute(cid++, "id", (short)99));
		com.lexst.sql.column.attribute.CharAttribute cf = new com.lexst.sql.column.attribute.CharAttribute(cid, "word", "unix".getBytes());
		cf.setLike(true);
		table.add(cf);
		Inject inject = new Inject(table);
		
		for (int i = 0; i < 200; i++) {
			cid = 1;
			com.lexst.sql.column.Short sht = new com.lexst.sql.column.Short(cid++, (short) (i ));
			
			byte[] b = String.format("CHAR%d", i + 1).getBytes();
			com.lexst.sql.column.Char ch = new com.lexst.sql.column.Char(cid++, b);

			Row row = new Row();
			row.add(sht);
			row.add(ch);
			
			inject.add(row);
		}
		
		byte[] b = inject.build();
		System.out.printf("inject size is:%d\n", b.length);
		
		try {
			FileOutputStream out = new FileOutputStream("d:/inject.bin");
			out.write(b);
			out.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

	
//	public void test56() {
//		byte[] b = "Pentium-Unix".getBytes();
//		ISO_8859_1 ascii = new ISO_8859_1();
//		String s = ascii.decode(b);
//		System.out.printf("value is:%s\n", s);
//	}
	
	public void test58() {
		String key = "java.class.path";
		String s = System.getProperty(key);
		System.out.printf("%s is:%s\n", key, s);
		
		String value = s + ";e:\\cloud\\lxregex.jar";
		System.setProperty(key, value);
		
		s = System.getProperty(key);
		System.out.printf("new %s is:%s\n", key, s);

		String class_name = "com.lexst.sql.RegexFrame";
		try {
			Class<?> cls = Class.forName(class_name);
			System.out.printf("CLASS NAME IS :%s\n", cls.getName());
		} catch (ClassNotFoundException exp) {
			exp.printStackTrace();
		}
		
		ClassLoader loader = ClassLoader.getSystemClassLoader();
		System.out.printf("this is:%s\n", loader.getClass().getName());
		
//		System.setProperty(key, s);
//		try {
//			Class<?> cls = Class.forName(class_name);
//			System.out.printf("NEXT CLASS NAME IS :%s\n", cls.getName());
//		} catch (ClassNotFoundException exp) {
//			exp.printStackTrace();
//		}
	}
	
//	public void test59() {
//		String filename = "e:\\cloud\\lxregex.jar";
//		com.lexst.algorithm.collect.ProjectClassLoader loader = new com.lexst.algorithm.collect.ProjectClassLoader();
//		loader.add(filename);
//		
//		String class_name = "com.lexst.sql.RegexFrame";
//		try {
//			Class<?> cls = Class.forName(class_name, true, loader);
//			System.out.printf("CLASS NAME IS :%s\n", cls.getName());
//			
//			class_name = "java.lang.String";
//			cls = Class.forName(class_name, true, loader);
//			System.out.printf("NEXT CLASS NAME Is:%s\n", cls.getName());
//		} catch (ClassNotFoundException exp) {
//			exp.printStackTrace();
//		}
//		
////		com.lexst.live.CollectPool.getInstance().add(filename);
//	}

	public void test60() {
		String path = "E:\\tools\\collect";
		
		java.util.List<String> list = CollectTaskPool.getInstance().load(path);
		for(int i = 0; list !=null && i<list.size(); i++) {
			System.out.printf("name is:%s\n", list.get(i));
		}
		
		String name = "collect-sample";
		
		CollectTask task = CollectTaskPool.getInstance().find(name);
		
		System.out.printf("task name is:%s\n", task.getClass().getName());
		System.out.printf("project name is:%s\n", task.getProject().getClass().getName());
//		task.execute(null, null);
		
		String class_name = "org.lexst.collect.Block";
		Class<?> cls = CollectTaskPool.getInstance().findClass(class_name);
		System.out.printf("class object is %s\n", (cls == null ? "null" : "not null"));
		if(cls != null) {
			System.out.printf("class name:%s\n", cls.getName());
		}
	}
	
	public void test61() {
		String bin = System.getProperty("user.dir");
		if (bin.charAt(bin.length() - 1) == File.separatorChar) {
			bin = bin.substring(0, bin.length() - 1);
		}
		int last = bin.lastIndexOf(File.separatorChar);
		if (last > -1) {
			bin = bin.substring(0, last + 1);
		}
		System.out.printf("bin path:%s\n", bin);
		
		long value = 1024 * 1024 * 1024 ;
//		value += value /3;
		double f = (double) value / (double) 1024;
		System.out.printf("value: %g G\n", f);
	}
	
	public void test62() {
		java.util.List<Naming> array = new ArrayList<Naming>();
		
		array.add(new Naming("pentium"));
		array.add(new Naming("abc"));
		array.add(new Naming("word"));
		array.add(new Naming("Pentium"));
		array.add(new Naming("Abc"));
		array.add(new Naming("Simaple"));
		array.add(new Naming("aBc"));
		
		java.util.Collections.sort(array);
		for(Naming n : array) {
			System.out.println(n);
		}
	}

	public void test63() {
		String s = "LEXST";
		byte[] b = s.getBytes();
		long value = Sign.sign(b, 0, b.length);
		System.out.printf("value is:%x\n", value);
		
		b = s.toLowerCase().getBytes();
		value = Sign.sign(b, 0, b.length);
		System.out.printf("value is:%x\n", value);
	}

	public void test64() {
		String text = "大风起兮云飞扬威加海内兮归故乡安得猛士兮守四方故乡";
		Map<String, Integer> map = new HashMap<String, Integer>(128);
		
		for(int left = 0; left < text.length(); left++) {
			for (int right = text.length(); left < right; right--) {
				String s = text.substring(left, right);
				System.out.println(s);
				Integer value = map.get(s);
				if(value == null) {
					map.put(s, 1);
				} else {
					map.put(s, value+1);
				}
			}
		}
		
		System.out.printf("\nsize:%d\n\n", map.size());
		
		for(String key : map.keySet()) {
			int value = map.get(key);
			if(value > 1) {
				System.out.printf("%s - %d\n", key, value);
			}
		}
		
		Integer i = map.get("加海");
		System.out.printf("result is:%d\n", (i == null ? -1 : i));
		
		short columnId = 1;
		columnId |= 1024;
		System.out.printf("column id:[%d - %x]\n", columnId, columnId);
		
		columnId &= 1023;
		System.out.printf("column id:[%d - %x]\n", columnId, columnId);
		
//		UIResource.
		Object obj = UIManager.get("Default_font");
		System.out.printf("default font is:%s\n", obj);

		for(Object key : UIManager.getDefaults().keySet()) {
			Object value = UIManager.get(key);
			System.out.printf("%s - %s\n", key, value);
		}
		
		String[] fontlist = GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames(); 
		for(i = 0; fontlist !=null && i < fontlist.length; i++) {
			System.out.printf("%s\n", fontlist[i]);
		}
				
		System.out.println("finished!");
	}
	
	public void test65() {
		Process process = null;
		try {
			process = Runtime.getRuntime().exec("cmd /c dir");
			System.out.printf("class name is:%s\n", process.getClass().getName());

			InputStream in = process.getInputStream();
			java.io.ByteArrayOutputStream out = new ByteArrayOutputStream();

			byte[] b = new byte[1024];
			while (true) {
				int len = in.read(b, 0, b.length);
				if (len < 1) break;
				out.write(b, 0, len);
			}

			b = out.toByteArray();
			System.out.printf("array length is:%d\n", (b == null ? -1 : b.length));
			if (b != null && b.length > 0) {
				String s = new String( b, 0, b.length, "gbk");
//				String s = new String(b, 0, b.length);
				System.out.println(s);
			}
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		
//	Linux command		
//		   cmd = "  date -s 20090326";   
//		   Runtime.getRuntime().exec(cmd);   

	}
	
	int __buildLikeId(int columnId) { return columnId | 1024;}
	int __buildNormalId(int likeId) { return likeId & 1023;}
	boolean __isLikeId(int columnId) { return (columnId & 1024) == 1024;}

	public void test66() {
		int colid = 2;
		
		int like_id = this.__buildLikeId(colid);
		System.out.printf("likeid is:%d\n", like_id);
		
		System.out.printf("like id check is:%b\n", this.__isLikeId(like_id));
		
		int normal_id = this.__buildNormalId(like_id);
		System.out.printf("normal id is:%d\n", normal_id);
	}
	
	short buildLikeId2(short columnId) { return (short)(columnId | 0x8000);}
	short buildNormalId2(short likeId) { return (short)(likeId & 0x7FFF);}
	boolean isLikeId2(int columnId) { return (columnId & 0x7FFF) == 0x8000;}

	public void test67 () {
//		System.out.printf("short:%d - %d\n", Short.MIN_VALUE , Short.MAX_VALUE);
//		short mid = 16383;
//		System.out.printf("mode:%d - %x - %x\n", mid, mid, mid+1);
//		short id = 32767;
		
		short columnId = 2;
//		short max = (short)0x8000;
//		
//		short likeId = (short)(columnId | max);
//		System.out.printf("value is:%d - %x\n", likeId, likeId);
//		
//		short value2 = (short)(likeId & 0x7fff);
//		System.out.printf("source is:%d - %x\n", value2, value2);
		
		short likeId = buildLikeId2(columnId);
		com.lexst.sql.column.WChar ch = new com.lexst.sql.column.WChar(likeId, "pentium".getBytes());
		System.out.printf("like id:%d - 0x%x \n",  ch.getId(), ch.getId());
		
		columnId = buildNormalId2(likeId);
		com.lexst.sql.column.WChar ch2 = new com.lexst.sql.column.WChar(columnId, "pentium".getBytes());
		System.out.printf("column id:%d - %x\n", ch2.getId(), ch2.getId());
		
		long g = Long.MAX_VALUE / 1024 / 1024 /1024 / 1024 / 1024 ;
		System.out.printf("64bit G:%d\n", g);
		
	}
	
	public void test68() {
//		String ling = "=|!=|<>|>|>=|<|<=";
//		// =|!=|<>|>|>=|<|<=
//		StringBuilder buff = new StringBuilder();
//		for (int i = 0; i < ling.length(); i++) {
//			char w = ling.charAt(i);
//			if (w == '|') {
//				buff.append(w);
//			} else {
//				String s = String.format("\\x%x", (int) w);
//				buff.append(s);
//			}
//		}
//		System.out.println(buff.toString());
		
		String SQL_WHERE_NUM2  = "^\\s*([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(?i)(!=|<>|>=|<=|>|<|=|LIKE)\\s*(.+)\\s*$"; //数字
		String sql = "id Like 10";
		Pattern pattern = Pattern.compile(SQL_WHERE_NUM2);
		Matcher matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			String s1 = matcher.group(1);
			String s2 = matcher.group(2);
			String s3 = matcher.group(3);
			System.out.printf("%s  %s  %s\n", s1, s2, s3);
		} else {
			System.out.println("error!");
		}
	}
	
	public void test69() {
		RawAttribute field = new RawAttribute((short)1, "resource", "unix-syste".getBytes());
		field.setPacking(Packing.GZIP, Packing.DES, "UNIX".getBytes());
//		field.setPackingPassword("DECRYPT".getBytes());
		field.setValue("VALUE".getBytes());
		field.setIndex("INDEX".getBytes());
		
		byte[] b = field.build();
		System.out.printf("raw build size:%d\n", b.length);
		System.out.println(new String(b));
		
		RawAttribute rf = new RawAttribute();
		int len = rf.resolve(b, 0, b.length);
		System.out.printf("raw resolve size:%d\n",  len);
		
		System.out.println();
		this.test90();
		
		System.out.println();
		this.test91();
		
		System.out.println();
		this.test92();
	}
	
	public void test90() {
		CharAttribute field = new CharAttribute((short)1, "system", "unix-system".getBytes());
		field.setPacking(Packing.GZIP, Packing.DES, "UNIX".getBytes());
//		field.setPackingPassword("COLUMN-PASSWORD".getBytes() );
		field.setValue("VALUE".getBytes());
		field.setIndex("INDEXscan".getBytes());
		
//		DefaultFunction def = new DefaultFunction("NOW", new String[]{"YYYY/MM/DD","年/月/日"});
//		field.setFunction(def);
		
		Now now = new Now();
		field.setFunction(now);
		
		byte[] b = field.build();
		System.out.printf("char build size:%d\n", b.length);
		System.out.println(new String(b));

		CharAttribute cf = new CharAttribute();
		int len = cf.resolve(b, 0, b.length);
		System.out.printf("char resolve size:%d\n",  len);
	}
	
	public void test91() {
		SCharAttribute field = new SCharAttribute((short)1, "system", "unix-system".getBytes());
		field.setPacking(Packing.GZIP, Packing.DES, "UNIX".getBytes());
//		field.setPackingPassword("LEXST-PASSWORD".getBytes() );
		
		byte[] b = field.build();
		System.out.printf("nchar build size:%d\n", b.length);
		System.out.println(new String(b));

		SCharAttribute cf = new SCharAttribute();
		int len = cf.resolve(b, 0 , b.length);
		System.out.printf("nchar resolve size:%d\n",  len);
	}
	
	public void test92() {
		WCharAttribute field = new WCharAttribute((short)1, "system", "unix-system".getBytes());
		field.setPacking(Packing.GZIP, Packing.DES, "UNIX".getBytes());
//		field.setPackingPassword("LEXST-PASSWORD".getBytes() );
		
		byte[] b = field.build();
		System.out.printf("wchar build size:%d\n", b.length);
		System.out.println(new String(b));

		WCharAttribute cf = new WCharAttribute();
		int len = cf.resolve(b, 0, b.length);
		System.out.printf("wchar resolve size:%d\n",  len);
	}
	
	public void test80() {
		StringBuilder buff = new StringBuilder();
		for(char w = 'a'; w<='z'; w++) {
			buff.append(w);
		}
		
		StringBuilder buf = new StringBuilder();
		for(int i =0; i < 100; i++) {
			buf.append(buff.toString());
		}
		try {
			byte[] b = buf.toString().getBytes();
			b = Deflator.gzip(b, 0, b.length);
			System.out.printf("gzip source size:%d, gzip compress size:%d\n", buf.length(), b.length);
			byte[] c = com.lexst.sql.util.Inflator.gzip(b, 0, b.length);
			System.out.printf("gzip uncompress size:%d\n", c.length);

			b  = buff.toString().getBytes();
			b = Deflator.zip(b, 0, b.length);
			System.out.printf("zip source size: %d, zip compress size:%d\n", buf.length(), b.length);
			c = com.lexst.sql.util.Inflator.zip(b, 0, b.length);
			System.out.printf("zip uncompress size:%d\n", c.length);

			b = buf.toString().getBytes();
			b = Deflator.deflate(b, 0, b.length);
			System.out.printf("deflate source size: %d, zip compress size:%d\n", buf.length(), b.length);
			c = com.lexst.sql.util.Inflator.inflate(b, 0, b.length);
			System.out.printf("inflate uncompress size:%d\n", c.length);
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void test81() {
		String filename = "g:/lookup3.c";
		File file = new File(filename);
		try {
		String s = file.toURI().toURL().toString();
		System.out.printf("jar filename is:%s\n", s);
		} catch(Exception exp) {
			exp.printStackTrace();
		}
	}
	
	public void test82() {
		String cls_name = "com.lexst.util.Sleep";
		cls_name = "org.godson.shutdown.Shutdown";
		cls_name = "org.lexst.find.Finder";
		TaskClassLoader loader = new TaskClassLoader();
		
		try {
			for(int i = 0 ; i < 3; i++) {
			Class<?> cls = Class.forName(cls_name, false, loader);
			System.out.printf("class is:%s\n\n", cls.getName().toString());
			
			loader = new TaskClassLoader();
			}
		} catch (java.lang.Exception exp) {
			exp.printStackTrace();
		}
	}
	
	public void test83() {
		CollectTaskPool self = CollectTaskPool.getInstance();
		self.setRoot("E:/lexst/live/task");
		
		self.start();
		
		self.delay(1000);

		Project pj = self.findProject("Collect-DC-Live");
		if(pj != null) {
			System.out.printf("project name:%s, task class:%s\n", pj.getTaskNaming().toString(), pj.getTaskClass());
		}
		CollectTask task = self.find("Collect-DC-Live");
		System.out.printf("live task name is:%s\n", (task == null ? "null" : task.getClass().getName()));

		Class<?> clz = self.findClass("org.lexst.distribute.collect.LiveCollectDCTask");
		System.out.printf("class name is:%s\n", clz.getName());
		
//		URL url = self.findResource("org/lexst/distribute/collect/net.png");
//		if(url == null) System.out.println("this is null resource file");
//		else {
//			System.out.printf("resouce url: %s\n", url.toExternalForm());
//		}
		
		ClassLoader loader = self.getClassLoader();
		URL url = loader.getResource("org/lexst/distribute/collect/net.png");
		if (url == null)
			System.out.println("this is null resource file!");
		else {
			System.out.printf("new resouce url: %s\n", url.toExternalForm());

			InputStream in = loader.getResourceAsStream("org/lexst/distribute/collect/net.png");
			byte[] b = new byte[1024];
			int size = 0;
			try {
				do {
					int len = in.read(b, 0, b.length);
					if (len == -1) break;
					size += len;
				} while (true);
				in.close();
			} catch (IOException exp) {
				exp.printStackTrace();
			}
			System.out.printf("new resource size is:%d\n", size);
		}
		
		System.out.println("\ninto next......\n");
		
//		self.stop();
	}
	
	public void test85() {
		String filename = "E:/lexst/live/task/collect.jar";
		File file = new File(filename);
		try {
		String s = file.toURI().toURL().toExternalForm();
		System.out.printf("file is -> %s\n", s);
		String name = "/collect/picture.jpg";
		String url = "jar:" + file.toURI().toURL().toExternalForm() + "!/" + name;
		System.out.printf("resource is -> %s\n", url);
		} catch (java.net.MalformedURLException exp) {
			exp.printStackTrace();
		}
	}
	
	public void test86() {
		byte[] b = new byte[26];
		int i = 0;
		for(char c = 'a'; c <= 'z'; c++) {
			b[i++] = (byte)c;
		}
		System.out.println(new String(b));
		System.arraycopy(b, 3, b, 0, b.length - 3);
		System.out.println(new String(b));
	}
	
	public void test87() {
		String filename = "c:/m.txt";
		File file = new File(filename);
		byte[] b = new byte[(int)file.length()];
		try {
			java.io.FileInputStream in = new java.io.FileInputStream(file);
			in.read(b);
			in.close();
		} catch(IOException exp) {
			exp.printStackTrace();
		}
		
		
		String s =  "";
		try {
			s = new String(b, "UTF-8");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		s = s.replaceAll("<br>","\r\n");
		s = s.replaceAll("</td>", "\r\n");
		
		System.out.println(s);
//		System.out.printf("file size:%d\n", b.length);
		
		try {
			java.io.FileOutputStream out = new java.io.FileOutputStream(file);
			out.write(s.getBytes("UTF-8"));
			out.close();
		} catch(IOException exp) {
			exp.printStackTrace();
		}
			
//		System.out.println("finished!");
	}
	
	private String stream(byte[] b) {
		StringBuilder buff = new StringBuilder();
		for(int i =0; i < b.length; i++) {
			String s = String.format("%X", b[i] & 0xFF);
			if(s.length() == 1) s = "0"+s;
			if(buff.length()>0) buff.append(",");
			buff.append(s);
		}
		return String.format("[%s]", buff.toString());
	}
	
//	private int[][] ranges() {
//		return new int[][] {
//			{122, 122},
//			{122, 333}
//		};
//	}

//	public void testShortSector() {
////		com.lexst.sql.statement.order 
//		ShortSector sector = new ShortSector();
//		int count = sector.splitting(Short.MIN_VALUE, Short.MAX_VALUE, 5);
//		System.out.printf("split count:%d\n", count);
//		
//		String s = sector.build();
//		System.out.printf("build size:%s\n", s);
//		
//		count = sector.resolve(s.getBytes(), 0, s.length());
//		System.out.printf("resolve count:%d\n", count);
//		
//		for(ShortRange range: sector.list()) {
//			System.out.printf("%s | %X - %X\n", range, range.getBegin(), range.getEnd());
//		}
//		
//		System.out.println("--------------");
//		
//		CharSector charSector = new CharSector();
//		charSector.setPacking(Packing.GZIP, Packing.AES, "UNIXSYSTEM".getBytes());
//		count = charSector.splitting(Integer.MIN_VALUE, Integer.MAX_VALUE, 3);
//		System.out.printf("char sector split:%d\n", count);
//
//		s = charSector.build();
//		System.out.printf("build size:%s\n", s);
//		
//		count = charSector.resolve(s.getBytes(), 0, s.length());
//		System.out.printf("char sector resolve:%d\n", count);
//		
//		for(IntegerRange range: charSector.list()) {
//			System.out.printf("%s | %X - %X\n", range, range.getBegin(), range.getEnd());
//		}
//
//		DoubleSector ds = new DoubleSector();
//		count = ds.splitting(java.lang.Double.MIN_VALUE, java.lang.Double.MAX_VALUE, 5);
//		System.out.printf("double range:%G - %g\n", java.lang.Double.MIN_VALUE, java.lang.Double.MAX_VALUE);
//		
//		s = ds.build();
//		System.out.printf("double build:%s\n", s);
//		
//		ds.clear();
//		ds.resolve(s.getBytes(), 0, s.length());
//		System.out.printf("double sector resolve:%d\n", count);
//		
////		RealSector rs = new RealSector();
////		count = rs.splitting(Float.MIN_VALUE, Float.MAX_VALUE, 5);
////		s = rs.build();
////		System.out.printf("real build:%s\n", s);
////		
////		rs.clear();
////		count = rs.resolve(s.getBytes(), 0, s.length());
////		System.out.printf("float sector resolve:%d\n", count);
////		
////		System.out.printf("float range:%f, %f - %f\n", Float.MIN_VALUE, Float.MIN_VALUE+1, Float.MAX_VALUE);
////		System.out.printf("float range:%G - %g\n", Float.MIN_VALUE, Float.MAX_VALUE);
////		System.out.printf("float range:%X - %X\n", Float.floatToIntBits(Float.MIN_VALUE), Float.floatToIntBits(Float.MAX_VALUE));
//		
////		String ss = "梁";
////		com.lexst.sql.charset.UTF32 utf32 = new com.lexst.sql.charset.UTF32();
////		byte[] bytes = utf32.encode(ss);
////		System.out.printf("%s size: %d | %s\n", ss, bytes.length, stream(bytes));
////		
////		com.lexst.sql.charset.UTF16 utf16 = new com.lexst.sql.charset.UTF16();
////		bytes = utf16.encode(ss);
////		System.out.printf("%s size: %d | %s\n", ss, bytes.length, stream(bytes));
////		
////		com.lexst.sql.charset.UTF8 utf8 = new com.lexst.sql.charset.UTF8();
////		bytes = utf8.encode(ss);
////		System.out.printf("%s size: %d | %s\n", ss, bytes.length, stream(bytes));
////		
////		bytes = new byte[] { (byte)0xD8, (byte)0x69, (byte)0xDE, (byte)0xA5 };
////		bytes = new byte[] { (byte)0x80, (byte)0x7F };
////		ss = utf16.decode(bytes);
////		System.out.printf("end string:'%s', %d\n", ss, ss.length());
//	}
	
	public void testSector() {
//		String s = "eff";
//		ShortSector sector = new ShortSector();
//		short value = sector.toShort(s);
//		System.out.printf("value is:%d - %x\n", value, value);
//		
//		sector.add(new ShortRange((short)0, (short)255));
//		sector.add(new ShortRange((short)300, (short)0x7fff));
//		s = sector.build();
//		System.out.println(s);
//		
//		byte[] b = s.getBytes();
//		ShortSector ss = new ShortSector();
//		int len = ss.resolve(b, 0, b.length);
//		System.out.printf("resolve size:%d\n", len);
		
		ArrayList<Integer> a = new ArrayList<Integer>(5);
		for(int i = 0; i < 10; i++) {
			a.add(i + 100);
		}
		System.out.printf("int size:%d\n", a.size());
		a.ensureCapacity(-1);
		System.out.printf("rebuild size:%d\n", a.size());
	}
	
	public void testCharset() {
//		com.lexst.sql.charset.UTF16 utf16 = new com.lexst.sql.charset.UTF16();
//		
//		byte[] bytes = new byte[] { (byte)0xD9, (byte)0x50, (byte)0xDF, (byte)0x21 };
////		bytes = new byte[] { (byte)0x80, (byte)0x7F };
//		String s = utf16.decode(bytes, 0, bytes.length);
//		System.out.printf("end string:'%s', %d\n", s, s.length());
//		
//		bytes = new byte[] {(byte)0xD8, (byte)0x0, (byte)0xDF, (byte)0x02};
//		s = utf16.decode(bytes, 0, bytes.length);
//		System.out.printf("data:%s\n", s);
//
//		s = "䜩，；！牛";
//		bytes = utf16.encode(s);
//		s = stream(bytes);
//		System.out.printf("%d - %s\n", bytes.length, s);
//		
//		s = "，；！";
//		com.lexst.sql.charset.UTF8 utf8 = new com.lexst.sql.charset.UTF8();
//		bytes = utf8.encode(s);
//		s = stream(bytes);
//		System.out.printf("%d - %s\n", bytes.length, s);
		
//		bytes = new byte[] { (byte) 180 };
//		System.out.printf("size:%d\n", bytes.length);
//		try {
//			s = new String(bytes, 0, bytes.length, "ISO-8859-1");
//			bytes = s.getBytes("UTF-8");
//			System.out.printf("'%s' byte size:%d\n", s, bytes.length);
//			System.out.printf("%s\n", stream(bytes));
//			
//			bytes = new byte[]{(byte)0xD8, (byte)0x69, (byte)0xDE, (byte)0xA5};
//			s = new String(bytes, 0, bytes.length, "UTF-16BE");
//			System.out.printf("%s size: %d\n", s, s.length());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
//		java.io.ByteArrayOutputStream out = new ByteArrayOutputStream();
//		for(int i = 0; i < 0x30; i++ ) {
//			out.write((byte)0x04);
//			out.write((byte)i);
//		}
//		for(int i = 0x70; i < 0xff; i++) {
//			out.write((byte)0x03);
//			out.write((byte)i);
//		}
//		for(int i = 0x90; i < 0xFF; i++) {
//			out.write((byte)0x05);
//			out.write((byte)i);
//		}
//		bytes = out.toByteArray();
//		System.out.printf("%s\n", utf16.decode(bytes));
		
//		byte[] w = new byte[] { (byte)0xD8, (byte)0x69, ()};
		
		char w1 = 0xD869;
		char w2 = 0xDEA5;
		char[] w = new char[] { w1, w2};
		int code = java.lang.Character.codePointAt(w, 0);
		System.out.printf("code point is:%x - %s\n", code, new String(w));
		
		String s = new String(w);
		s = s+s+"朱聿";
		int count = java.lang.Character.codePointCount(s, 0, s.length());
		System.out.printf("code point count:%d\n", count);
		
		char[] origin = s.toCharArray();
//		for(int i = 0; i < count; i++) {
////			code = java.lang.Character.codePointAt(s, i);
//			code = java.lang.Character.codePointAt(origin, i);
//			System.out.printf("code point is:%X\n", code);
//		}
		
		int seek = 0;
		for(int index = 0; index < count; index++) {
			code = java.lang.Character.codePointAt(s, seek);
			int num = java.lang.Character.charCount(code);
			seek += num;
			System.out.printf("code point is:%X\n", code);
		}
		
		UTF16 utf = new UTF16();
		code = utf.codePointAt(3, s);
		System.out.printf("return code is:%X | %X | %X\n", 
				code, s.codePointAt(4), origin[4] & 0xFFFF);
		
		StringBuilder bu = new StringBuilder();
		String str = "千里江山寒色远芦花深处泊孤舟笛在月明楼";
		bu.append(w);
		bu.append(str.substring(0, 3));
		bu.append(w);
		bu.append(str.substring(3));
		str = bu.toString();
		
		String sub = utf.subCodePoints(2, 5, str);
		System.out.printf("sub is:[%s], %d - %d\n",
				sub, sub.length(), utf.codePointCount(sub));
		
		
		
	}
	
	public void testCodePointCounter() {
		Space space = new Space("schema", "table");
		short columnId = 2;
		Docket deck = new Docket(space, columnId);
		CodePointCounter counter = new CodePointCounter(deck);
		
		for(int codePoint = 200; codePoint < 5000; codePoint++) {
			counter.add(codePoint);
		}
		
		byte[] b = counter.build();
		System.out.printf("build stream size is:%d\n", b.length);
		
		CodePointCounter counter2 = new CodePointCounter();
		
		int size = counter2.resolve(b, 0, b.length);
		System.out.printf("resolve stream size is:%d\n", size);
		
		int[] array = counter.paris();
		System.out.printf("pairs size:%d\n", array.length);
		
//		b = VariableGenerator.compress(Packing.GZIP, b, 0, b.length);
//		System.out.printf("compress size:%d\n", b.length);
	}
	
	public void testReturnTag() {
		byte[] b = new byte[100];
		for(int i = 0; i < b.length; i++) {
			b[i] = (byte)'a';
		}
		
		DataClient client = new DataClient(true);
		client.setNumber(1);
		
		DataTrustor trustor = new DataTrustor();
		ReturnTag tag = trustor.getTag();
		tag.setBeginTime(System.currentTimeMillis());
		
		for (int i = 0; i < 3; i++) {
			client.setNumber(i + 1);
			trustor.flushTo(client, 10, b, 0, b.length);
		}
		
//		trustor.flushTo(1, 10, b, 0, b.length);
//		trustor.flushTo(2, 10, b, 0, 0);
//		trustor.flushTo(3, 10, b, 0, 10);
//		trustor.flushTo(4, 10, b, 0, 20);
//		trustor.flushTo(5, 5, b, 0, 30);
		
		byte[] data = trustor.data();
		System.out.printf("data size is:%d\n", data.length);
		
//		ReturnTag tag = new ReturnTag();
//		tag.setBeginTime(System.currentTimeMillis());
//		tag = trustor.getTag();
		
		int seek = tag.resolve(data, 0, data.length);
		System.out.printf("resolve tag size:%d\n", seek);
		
		System.out.printf("items:%d, fields:%d, size:%d, %d-%d usedtime:%d\n",
				tag.getItems(), tag.getFields(), tag.getSize(),
				tag.getBeginTime(), tag.getEndTime(), tag.usedTime());
	}
	
	public void testByteBuffer() {
		
		byte[] b = new byte[32];
		for(int i = 0; i < b.length; i++) {
			b[i] = (byte)'a';
		}
		
		java.nio.ByteBuffer buff = java.nio.ByteBuffer.allocate(b.length);
		buff.put(b);
//		java.nio.ByteBuffer buff = java.nio.ByteBuffer.wrap(b);
		
//		for(int i = 0; i < 3; i++) {
//			System.out.printf("remaing size:%d\n", buff.remaining());
//			buff.put(b, 0, b.length);
//		}
		
		byte[] s = buff.array();
		System.out.printf("new byte array size:%d,%d,%d,%s\n",
				s.length, buff.remaining(), buff.position(), new String(b));
	}
	
	public void testBalance() {
		TimeBalancer balancer = new TimeBalancer();
		for (int i = 100; i <= 300; i += 100) {
			balancer.add(new Integer(i), new Integer(i + 10), 29);
		}

//		balancer.add(200, 500, 20);
//		balancer.add(20, 20, 20);
//		balancer.add(1, 300, 20);

		System.out.printf("balance size:%d\n", balancer.size());
		ColumnSector sector = balancer.balance(5);
		
		byte[] b = sector.build();
		System.out.println(new String(b));
		
		int size = sector.resolve(b, 0, b.length);
		System.out.printf("resolve size:%d, %d\n", size, b.length);
		
//		ArrayList<IntegerRange> ranges = new ArrayList<IntegerRange>();
//		ranges.add(new IntegerRange(20, 30));
//		ranges.add(new IntegerRange(0, 200));
//		ranges.add(new IntegerRange(10, 50));
//		
//		java.util.Collections.sort(ranges);
//		for(IntegerRange range: ranges) {
//			System.out.println(range.toString());
//		}
		
		ArrayList<IntegerRange> array = new ArrayList<IntegerRange>();
		for (int i = 1000; i > 0; i -= 50) {
			array.add(new IntegerRange(i - 50, i));
		}
//		array.add(null);
//		array.add(null);
		System.out.printf("array size:%d\n", array.size());
		
		java.util.Collections.sort(array);
		
		for(IntegerRange range : array) {
			System.out.printf("%s\n", range);
		}
		
		System.out.println("--------------------");
		ShortIndexChart chart = new ShortIndexChart();
		SiteHost host = null;
		try {
			host = new SiteHost("12.23.33.33", 90, 80);
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		long chunkid = 1000;
		short columnid = 12;
		short begin = 100;
		short end = 1000;
		for(int i = 0; i < 10; i++) {
			ShortIndexRange range = new ShortIndexRange(chunkid, columnid, begin++, end ++);
			chart.add(host, range);
		}
		
		ShortIndexRange range = new ShortIndexRange(chunkid, columnid, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE);
		chart.add(host, range);
		
		System.out.printf("short index size:%d\n", chart.size());
		IndexZone[] zones = chart.choice(true);
		System.out.printf("zone size:%d\n",  zones.length);
		
////		Address address = new Address(Long.MAX_VALUE-10, Long.MIN_VALUE-10);
//		Address address = new Address("192.18.12.22");
//		System.out.println( address.toString());
//		
////		address.setAddress(0, Integer.MAX_VALUE - 10);
////		System.out.println( address.toString());
//		
//		
//			java.net.InetAddress inet = address.getAddress();
//			System.out.println(inet.toString());
//			address.setAddress(inet);
//			System.out.println(address.toString());
//		
//		
////		SiteSet set = new SiteSet();
////		SiteSet s2 = (SiteSet) set.clone();
		
	}
	
	public void testWorkDelegate() {
		byte[] b = "千里江山寒色远，芦花深处泊孤舟，笛在月明楼。好风凭借力，送我上青云。粉坠百花洲，香残燕子楼".getBytes();
//		byte[] b = "SYSTEM.UNIX|".getBytes();
		
		WorkTrustor dele = new WorkTrustor();
		WorkClient client = new WorkClient(true);
		
		for (int index = 0; index < 3; index++) {
			client.setNumber(index+1);
			dele.flushTo(client, 0, b, 0, b.length);
		}
		client.setNumber( client.getNumber() +1);
		dele.flushTo(client, 0, null, 0, 0);
		
		byte[] s = dele.data();
		System.out.println(new String(s));
		
		s = dele.data(-1);
//		System.out.println(new String(s));
	}
	
	public void testIntegerRange() {
		IntegerRange a = new IntegerRange(20, 33);
		IntegerRange b = new IntegerRange(28, 63);
		IntegerRange c = a.join(b);
		IntegerRange d = b.join(a);
		System.out.printf("%s | %s | join %s,%s\n",
				a.toString(), b.toString(), c.toString(), d.toString());
		
		String regex = "^\\s*(?i)(IS\\s+NOT\\s+NULL)\\s*$";
		String word = "is nOT null";
		if(word.matches(regex)) {
			System.out.println("match string!");
		}
	}
	
	public void testNetwork() {
		Address address = null;
		try {
			address = new Address("12.98.11.22");
		} catch (java.net.UnknownHostException exp) {
			exp.printStackTrace();
		}
		System.out.printf("any local address is:%s\n", address.isAnyLocalAddress());
		System.out.printf("lookback address is:%s\n", address.isLoopbackAddress());
		System.out.printf("IPV4 is:%s, IPV6 is:%s, %s\n---------\n", 
				address.isIPv4(), address.isIPv6(), address.getSpecification());
		
		String input = "7FFF:FAFF:F0FF:FFF5:7FFF:FFDF:253:FFF6";
//		input = "156.12.90.88";
		try {
			address.resolve(input);
			System.out.printf("IPV4 is:%s, IPV6 is:%s, address is: %s\n",
				address.isIPv4(), address.isIPv6(),	address.getSpecification());
			
			byte[] b = address.bits();
			System.out.printf("address byte size:%d\n", b.length);
			address.setAddress(b);
			InetAddress inet = address.getAddress();
			System.out.printf("%s - %s - %s - %s - %s - %s\n\n", 
					inet.isAnyLocalAddress(), inet.isLinkLocalAddress(), inet.isLoopbackAddress(),
					inet.isSiteLocalAddress(), inet.isMCGlobal(), inet.getHostAddress()); // inet.getCanonicalHostName());
			
//			 inet = InetAddress.getByName(input); //"www.sooget.com");
//			System.out.printf("ip address is:%s\n", inet.getHostAddress());
//			System.out.printf("host dns is:%s\n", inet.getHostName());
			
			System.out.println("---------------------\n");
			
			ByteArrayOutputStream bout = new ByteArrayOutputStream(1024 * 5);
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(address);
			oout.close();
			b = bout.toByteArray();
			System.out.printf(new String(b));

			System.out.printf("\nwrite address byte is:%d\n", b.length);
			// System.out.println("\n----------------------");

			ByteArrayInputStream bin = new ByteArrayInputStream(b);
			ObjectInputStream oin = new ObjectInputStream(bin);
			address = (Address) oin.readObject();
			oin.close();
			bin.close();
			
			System.out.println( Address.select().toString());
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testReal() {
		float min = -3.40E+38f;
		float max = +3.40E+38f;

		FloatRange f1 = new FloatRange(min, max);
		FloatRange f2 = new FloatRange(Float.MIN_VALUE, Float.MAX_VALUE);
		System.out.printf("float compare is:%d\n", f1.compareTo(f2));
		
		double begin = -1.79E+308;
		double end =  +1.79E+308;
		DoubleRange d1 = new DoubleRange(begin, end);
		DoubleRange d2 = new DoubleRange(Double.MIN_VALUE, Double.MAX_VALUE);
		System.out.printf("double compare is:%d\n", d1.compareTo(d2));
	}
	
	public void testDataLocal() {
		String filename = "e:/lexst/data/local.xml";
		
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);

		Element element = (Element) document.getElementsByTagName("chunk-directory").item(0);
		String build = xml.getValue(element, "build");
		String cache = xml.getValue(element, "cache");
		String[] paths = xml.getXMLValues(element.getElementsByTagName("store"));
		
		NodeList list = element.getElementsByTagName("store");
		System.out.printf("\nnode size is:%d\n", list.getLength());
		
		System.out.printf("build:%s\n", build);
		System.out.printf("cache:%s\n", cache);
		for(String path : paths) {
			System.out.printf("store:%s\n", path);
		}
		
		System.out.println("-----------");
		
		filename = "d:/cloud/call/bin/../../call/deploy/init";
		File file = new File(filename);
		try {
			System.out.printf("%s, %s\n", file.getAbsolutePath(),file.getCanonicalPath());
			System.out.printf("%s\n", System.getProperty("user.dir"));

			File sub = new File(file, "subs");
			System.out.printf("%s - %s\n", sub.getCanonicalPath(),sub.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Debug debug = new Debug();
////		debug.test();
//		int num = 50;
//		for (int i = 0; i < num; i++) {
////			debug.test2();
//		}

//		debug.test3();

//		debug.test5();
//		debug.test7();
//		debug.test6();

//		debug.test8();

//		debug.test9();
//		debug.test10();

//		debug.test11();

//		debug.test13();
//		debug.test12();
//		debug.test15();

//		debug.test19();
//		debug.test21();
//		debug.test28();
		
//		debug.test29(1255, 3);

		for (int i = 0; i < 10; i++) {
//			debug.test30();
		}
		for (int i = 0; i < 10; i++) {
//			debug.test31();
		}
		for (int i = 0; i < 10; i++) {
//			debug.test32();
		}
		
//		debug.test80();

//		debug.test578();
		
//		debug.test83();
		
//		debug.test86();
		
//		debug.test57();
		
		for (int i = 0; i < 10; i++) {
//			debug.binaryChop();
		}
		
//		debug.test87();
		
//		debug.testShortSector();
		
//		debug.test69();
		
//		debug.testCharset();
		
//		debug.testCodePointCounter();
		
//		debug.testReturnTag();
		
//		debug.testByteBuffer();
		
//		debug.testBalance();
		
//		System.out.println("---------------");
		
//		debug.testWorkDelegate();
		
//		debug.testSector();
		
//		debug.testIntegerRange();
		
//		debug.testNetwork();
		
//		debug.testReal();
		
		debug.testDataLocal();
	}

}
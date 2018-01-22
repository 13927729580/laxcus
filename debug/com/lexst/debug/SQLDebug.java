/**
 * 
 */
package com.lexst.debug;

import java.io.*;
import java.math.*;
import java.util.*;
import java.util.regex.*;

import com.lexst.sql.*;
import com.lexst.sql.account.*;
import com.lexst.sql.charset.*;
import com.lexst.sql.column.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.function.*;
import com.lexst.sql.index.*;
import com.lexst.sql.index.balance.*;
import com.lexst.sql.index.section.*;
import com.lexst.sql.parse.*;
import com.lexst.sql.parse.result.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;
import com.lexst.sql.statement.*;
import com.lexst.sql.statement.select.*;
import com.lexst.sql.statement.sort.*;
import com.lexst.sql.util.*;

/**
 * @author siven
 *
 */
public class SQLDebug {

	/**
	 * 测试器
	 */
	public SQLDebug() {
		// TODO Auto-generated constructor stub
	}

//	public static void main(String[] args) {
//	Space space = new Space("Video", "Word");
//	Table table = new Table(space);
//	
//	short colid = 2;
//	for(int i = 0; i < 5; i++) {
//		String naming = String.format("SHORT_%d", colid);
//		SmallField field = new SmallField(colid, naming, Short.MAX_VALUE);
//		table.add(field);
//		colid++;
//	}
//	
//	colid = 1;
//	String naming = String.format("SHORT%d", 199);
//	SmallField field = new SmallField(colid, naming, Short.MAX_VALUE);
//	table.add(field);
//	
//	for(Field value : table.values()) {
//		System.out.printf("%s - %d\n", value.getName(), value.getColumnId());
//	}
//	
//	colid = 3;
//	Field value = table.find(colid);
//	if (value != null) {
//		System.out.printf("find:%s - %d\n", value.getName(), value.getColumnId());
//	} else {
//		System.out.printf("%d is null!\n", colid);
//	}
//	
//	Clusters cs = table.getClusters();
//	cs.addIP("www.lexst.com");
//	cs.addIP("192.168.1.22");
//	cs.addIP("10.12.233.26");
//	
//	byte[] b = table.build();
//	System.out.printf("build byte size:%d\n", b.length);
//	
//	Table t2 = new Table();
//	int len = t2.resolve(b, 0);
//	System.out.printf("resolve byte size:%d\n", len);
//}

//private void print(byte[] b) {
//	java.lang.StringBuilder sb = new java.lang.StringBuilder();
//	for (int i = 0; i < b.length; i++) {
//		String s = String.format("%X", b[i] & 0xff);
//		if (s.length() == 1) s = "0" + s;
//		if (sb.length() > 0) sb.append(" ");
//		sb.append(s);
//	}
//	System.out.printf("%s", sb.toString());
//}
//
//public static void main(String[] args) {
//	Space space = new Space("PC", "ThinkPad");
//	short colid = 1;
//	byte[] s = "ABCDEFGH".getBytes();
//
//	int dt = com.lexst.util.datetime.SimpleDate.format(new Date());
//	int st = com.lexst.util.datetime.SimpleTime.format(new Date());
//	long stamp = com.lexst.util.datetime.SimpleTimeStamp.format(new Date());
//
//	Table table = new Table(space);
//	SmallField sht = new SmallField(colid++, "SHORT", Short.MAX_VALUE);
//	sht.setIndexType(Type.PRIMARY_INDEX);
//	RawField raw = new RawField(colid++, "RAW", s);
//	raw.setIndexType(Type.SLAVE_INDEX);
//	table.add(raw); // new RawField(colid++, "RAW", s));
//	table.add(new CharField(colid++, "CHAR", s));
////	table.add(new NCharField(colid++, "NCHAR", s));
////	table.add(new WCharField(colid++, "WCHAR", s));
//	table.add(sht); // new SmallField(colid++, "SHORT", Short.MAX_VALUE));
////	table.add(new IntField(colid++, "INT", Integer.MAX_VALUE));
////	table.add(new BigField(colid++, "LONG", Long.MAX_VALUE));
////	table.add(new RealField(colid++, "REAL", 999.0f));
////	table.add(new DoubleField(colid++, "DOUBLE", 999.25f));
////	table.add(new DateField(colid++, "DATE", (byte)0, dt));
////	table.add(new TimeField(colid++, "TIME", (byte)0, st));
////	table.add(new TimeStampField(colid++, "STAMP", (byte)0, stamp));
//
//	Layout layout = new Layout( (short)1, (byte)1 );
//	for (colid = 2; colid < 10; colid++) {
//		Layout l2 = new Layout((short) 2, (byte) 1);
//		layout.setLast(l2);
//	}
//	table.setLayout(layout);
//
//	byte[] b = table.build();
////	table.print(b);
//
//	try {
//		java.io.FileOutputStream out = new java.io.FileOutputStream("c:/head.bin");
//		out.write(b, 0, b.length);
//		out.close();
//	} catch (java.io.IOException exp) {
//		exp.printStackTrace();
//	}
//
////	System.out.printf("\npath separator [%s] file seprator [%s]\n", File.pathSeparator, File.separator );
//
//	int end = table.resolve(b, 0);
//	System.out.printf("build size: %d, end is %d\n", b.length, end);
//
////	try {
////		String a = "ABC";
////		byte[] as = a.getBytes("UTF-16BE");
////		for (int i = 0; i < as.length; i++) {
////			System.out.printf("%d ", as[i] & 0xff);
////		}
////	} catch (java.lang.Exception exp) {
////		exp.printStackTrace();
////	}
//}

	
//	public static void main(String[] args) {
//		SQLChecker checker = new SQLChecker();
//		String sql = "inject into video.system(word, id, weight)values('char', 1, 1),('nchar', 2, 1),('wchar', 3, 1)";
//		Space space = checker.getInjectSpace(sql);
//		System.out.println(space);
//		
//		Table table = new Table(space);
//		short id = 1;
//		table.add(new com.lexst.db.field.CharField(id++, "word"));
//		table.add(new com.lexst.db.field.IntegerField(id++, "id", 0));
//		table.add(new com.lexst.db.field.IntegerField(id++, "weight", 0));
//		
//		SQLCharset set = new SQLCharset();
//		set.setChar(new UTF8());
//		set.setNChar(new UTF16());
//		set.setWChar(new UTF32());
//		
//		SQLParser parser = new SQLParser();
//		sql = "inject into video.system(word, id, weight)values('char', 1, 1),('nchar', 2, 1),('wchar', 3, 1)";
//		parser.splitInject(set, table, sql);
//		
//		sql = "insert into video.system(word,id,weight)values('lexst',1,1)";
//		parser.splitInsert(set, table, sql);
//	}
	
//	public static void main(String[] args) {
//		String sql = "create user steven password=linux-system@pentium.com";
//		SQLParser parser = new SQLParser();
//		User user = parser.splitCreateUser(sql);
//		System.out.printf("create username:%s, password:%s\n", user.getHexUsername(), user.getHexPassword());
//		
//		sql = "alter user pentium password = linux-system@126.com.cn";
//		user = parser.splitAlterUser(sql);
//		System.out.printf("alter username:%s, password:%s\n", user.getHexUsername(), user.getHexPassword());
//		
//		sql = "drop sha1 user 51229ED0CFA4C47ADB2941E2867303A27808C09C";
//		
//		SQLChecker checker = new SQLChecker();
//		boolean f = checker.isDropSHA1User(sql);
//		user = parser.splitDropSHA1User(sql);
//		System.out.printf("drop username:%s, password:%s, result:%b\n",
//				user.getHexUsername(), user.getHexPassword(), f);
//	}
	
//	public static void main(String[] args) {
//		String sql = "show data site from 12.9.28.28";
//		SQLParser parser = new SQLParser();
//		Object[] params = parser.splitShowSite(sql);
//	}
	
//	public static void main(String[] args) {
//		String sql = " drop schema ";
//		SQLParser parser = new SQLParser();
//		boolean b = parser.isDropSchemaOption(sql);
//		System.out.printf("result is : %b\n", b);
//		
//		sql = "grant pentium, unix on schema video to lext";
//		Permit permit = parser.splitGrantSchema(sql);
//		
////		sql = "revoke penti, unix, linux, on schema video from lexst";
////		Permit permit = parser.splitRevokeSchema(sql);
//	}
	
//	public static void main(String[] args) {
//		String prefix = "clusters=12 primehost=3 hostmode=share chunksize=10m chunkcopy=3 hostcache=yes  ";
//		SQLParser parser = new SQLParser();
//		Table table = new Table();
//		parser.splitTablePrefix(prefix, table);
//		
//		System.out.printf("cluster number:%d\n", table.getClusters().getNumber());
//		System.out.printf("host mode is:%d\n", table.getMode());
//		System.out.printf("host cache:%b\n", table.isCaching());
//		System.out.printf("chunk size:%d\n", table.getChunkSize());
//		System.out.printf("prime host:%d\n", table.getPrimes());
//		System.out.printf("chunk copy:%d\n", table.getCopy());
//	}
	
	public  void testParseInject() {
		Space space = new Space("video", "word");
		Table table = new Table(space);
		table.add(new com.lexst.sql.column.attribute.CharAttribute((short)1, "word"));
		table.add(new com.lexst.sql.column.attribute.IntegerAttribute((short)2, "id", 0));
		table.add(new com.lexst.sql.column.attribute.IntegerAttribute((short)3, "weight", 0));
		table.add(new com.lexst.sql.column.attribute.RawAttribute((short)4, "data"));
		table.add(new com.lexst.sql.column.attribute.DateAttribute((short)5, "today", 89900));
		
		String sql = "inject into video.word (word, id, weight, data, today) values "
				+ "('Lexst 中办中休偿', 100, 10, 0x2398af, '2013-12-9'), ('Unix', 122, 1, 0x2389adf, '2015-12-8')";
		
		DebugSQLChooser chooser = new DebugSQLChooser();
		chooser.setTable(table);
		
//		Map<Space, Table> tables = new HashMap<Space, Table>();
//		tables.put(space, table);
		
		InsertParser parser = new InsertParser();
		
		Inject inject = parser.splitInject(sql, chooser);

		System.out.println("inject finished!");
		System.out.printf("row size:%d\n", inject.size());
		
		byte[] bs = inject.build();
//		byte[] ts = inject.build2();
		
//		System.out.printf("inject stream match is:%s\n\n", java.util.Arrays.equals(bs, ts));
		
		InsertFlag flag = inject.resolveFlag(bs, 0, bs.length);
		System.out.printf("%d, flag is:%s, flag size:%d, total size:%d, %s\n\n", 
				bs.length, flag != null, flag.getFlagSize(), flag.getTotalSize(), flag.getSpace());
		
		sql = " insert into video.word (word, id, weight, data, today) values ('Pentium千里江山UNIX', 100, 10, 0x2398af, '2013-12-9')";
		Insert insert = parser.splitInsert(sql, chooser);
		System.out.println("insert finished!");
		System.out.printf("column size:%d\n", insert.getRow().size());
		
		bs = insert.build();
		flag = inject.resolveFlag(bs, 0, bs.length);
		System.out.printf("%d, flag is:%s, flag size:%d, total size:%d, %s\n\n", 
				bs.length, flag != null, flag.getFlagSize(), flag.getTotalSize(), flag.getSpace());

//		ts = insert.build2();
//		System.out.printf("insert stream match is:%s\n\n", java.util.Arrays.equals(bs, ts));
		
		sql = "UPDATE video.word SET word='pentium' , today='2015-12-8', data=0x8999af, id = 155 where id=133 or id=188 and word='pentium'";
		UpdateParser uparser = new UpdateParser();
		Update update = uparser.split(sql, chooser);
		System.out.printf("update finished! size:%d", update.values().size());
		
//		short c_id = 1;
//		for(Row row : inject.list()) {
//			Char word = (Char)row.get(c_id);
//			byte[] b = set.getChar().encode(word.getValue());
//			System.out.println(new String(b));
//			
//			com.lexst.db.column.Integer ig = (com.lexst.db.column.Integer)row.get((short)(c_id+1));
//			System.out.printf("id:%d\n", ig.getValue());
//			
//			ig = (com.lexst.db.column.Integer)row.get((short)(c_id+2));
//			System.out.printf("weight:%d\n", ig.getValue());
//			
//			com.lexst.db.column.Raw raw = (com.lexst.db.column.Raw)row.get((short)(c_id+3));
//			System.out.printf("raw size:%d - ",  raw.getValue().length );
//			for(int i = 0; i < raw.getValue().length; i++) {
//				System.out.printf("%X",raw.getValue()[i] &0xff);
//			}
//			System.out.println("\r\n");
//		}
	}
	
	
	public void textParseSelect() {
		Space space = new Space("video", "word");
		Table table = new Table(space);
		table.add(new com.lexst.sql.column.attribute.CharAttribute((short)1, "word"));
		table.add(new com.lexst.sql.column.attribute.IntegerAttribute((short)2, "id", 0));
		table.add(new com.lexst.sql.column.attribute.IntegerAttribute((short)3, "weight", 0));
		table.add(new com.lexst.sql.column.attribute.RawAttribute((short)4, "data"));
		table.add(new com.lexst.sql.column.attribute.DateAttribute((short)5, "today", 89900));
		for (ColumnAttribute attribute : table.values()) {
			if (attribute.getColumnId() == 1) {
				attribute.setKey(Type.PRIME_KEY);
			} else {
				attribute.setKey(Type.SLAVE_KEY);
			}
		}
		
		Map<Space, Table> tables = new HashMap<Space, Table>();
		tables.put(space, table);

		String sql = "select word, id, data, today as thisday, sum(weight) , sum(id) as sumprice "
				+ "from video.word where id>23 "
				+ "GROUP BY id , weight Having SUM(id) > 10 "
				+ "ORDER BY id , weight desc";
		SelectParser parser = new SelectParser();
		Select select = parser.split(sql, null);
		System.out.printf("select space:%s\n", select.getSpace());
	}


//	public static void main(String[] args) {
//		// PACKING des3:'unix'
//		String sql_table = "create table primehost=1 hostmode=share chunksize=128m chunkcopy=1 hostcache=yes video.word(Word char not case like  , id int, weight int, memo char packing aes :'peniutm@126.com'' )";
//		String sql_index = "create index video.WORD (word(20) primary, id)";
////		String sql_index = "create index video.word ( ID primary, WORD(21) )";
//
//		SQLParser parser = new SQLParser();
//		Table table = parser.splitCreateTable(null, sql_table, sql_index, null);
//		
//		for(short c_id : table.idSet()) {
//			Field field = table.find(c_id);
//			System.out.printf("%s\n", field.getClass().getName() );
//			
//			if(field.getClass() == CharField.class) {
//				CharField cf = (CharField)field;
//				System.out.printf("sensitive is:%b, packing:%d\n",
//						cf.isSentient(), cf.getPacking());
//			}
//		}
//	}

//	public static void main(String[] args) {
//		String sql = "select * from video.word where word LIKE '___\\%LEXST\\%__'";
//		sql = "SELECT * FROM VIDEO.WORD WHERE WORD='lexst' or WORD='Pentium' ";
//		sql = "SELECT * FROM VIDEO.WORD WHERE WORD='lexst' and (id=2 or id=32)";
//		sql = "SELECT * FROM VIDEO.WORD WHERE word like '%EN%' and ( id>=1 and weight>12)";
//		
//		SQLCharset charset = new SQLCharset();
//		charset.setChar(new UTF8());
//
//		Table table = new Table();
//		com.lexst.db.field.CharField field = new com.lexst.db.field.CharField((short)1, "word");
//		field.setSentient(false);
//		field.setLike(true);
//		field.setIndexType(Type.PRIME_INDEX);
//		table.add(field);
//		com.lexst.db.field.IntegerField field2 = new com.lexst.db.field.IntegerField((short)2, "id", 0);
//		field2.setIndexType(Type.PRIME_INDEX);
//		table.add(field2);
//		com.lexst.db.field.IntegerField field3 = new com.lexst.db.field.IntegerField((short)3, "weight", 0);
//		table.add(field3);
//		
//		SQLParser parser = new SQLParser();
//		Select select = parser.splitSelect(charset, table, sql);
//
//		Condition condi = select.getCondition();
//		while (condi != null) {
//			IndexColumn index = condi.getValue();
//			Column col = index.getColumn();
//			
//			System.out.printf("class name is:%s, seq id:%d, outside relate:%d, relate:%d, name is:%s\n",
//					col.getClass().getName(), col.getId(), condi.getOutsideRelate(), condi.getRelate(), condi.getColumnName());
//
//			for(Condition sub : condi.getFriends()) {
//				index = sub.getValue();
//				col = index.getColumn();
//				System.out.printf("friend class name is:%s, seq id:%d, outside relate:%d, relate:%d, name is:%s\n",
//						col.getClass().getName(), col.getId(), sub.getOutsideRelate(), sub.getRelate(), sub.getColumnName());
//
//			}
//			
//			condi = condi.getNext();
//		}
//	}


//	public static void main(String[] args) {
////		String sql = "UPDATE VIDEO.WORD SET ID=1,WEIGHT=33.3, WORD='PENXIUM' WHERE   Id=3 OR word='UnixSys' and ( WORD='LEXST' AND ID=3 )";
//		
//		String sql = "UPDATE VIDEO.WORD SET ID=1,WEIGHT=33.3, WORD='PENXIUM' WHERE   Id=3 or WORD='SysUnix'";
//		SQLCharset charset = new SQLCharset();
//		charset.setChar(new UTF8());
//
//		Table table = new Table();
//		com.lexst.db.field.CharField field1 = new com.lexst.db.field.CharField((short) 1, "word");
//		com.lexst.db.field.IntegerField field2 = new com.lexst.db.field.IntegerField((short) 2, "id", 0);
//		com.lexst.db.field.DoubleField field3 = new com.lexst.db.field.DoubleField((short) 3, "weight", 0.0);
//		field1.setSentient(false);
//		field1.setLike(true);
//		table.add(field1);
//		table.add(field2);
//		table.add(field3);
//
//		SQLParser parser = new SQLParser();
//		Update update = parser.splitUpdate(charset, table, sql);
//		
//		System.out.printf("UPDATE IS %s\n", (update!=null?"OK":"ERR"));
//		for(Column col : update.values()) {
//			System.out.printf("set class name is:%s\n", col.getClass().getName());
//		}
//		
//		System.out.println();
//		
//		Condition condi = update.getCondition();
//		while(condi != null) {
//			IndexColumn ic = condi.getValue();
//			Column col = ic.getColumn();
//			System.out.printf("condition class name is:%s\n", col.getClass().getName());
//			for(Condition friend :	condi.getFriends()) {
//				System.out.printf("friend condition class name is:%s\n", friend.getValue().getColumn().getClass().getName());
//			}
//			condi = condi.getNext();
//		}
//
//		sql = "now date default '120913'";
//		Field field = parser.splitTableField(sql);
//		System.out.printf("value is:%d\n", ((DateField)field ).getValue());
//	}

	public void test1() {
//		String sql = "create tableshare host copy 3 video.item (char pid, int cid)";
//		String cb = "^\\s*(?i)CREATE\\s+(?i)TABLE([\\p{Print}]*)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*\\((.+)\\)\\s*$";
//
//		Pattern pattern = Pattern.compile(cb);
//		Matcher matcher = pattern.matcher(sql);
//		if (!matcher.matches()) { System.out.println("invalid"); return;}
//
//		int count = matcher.groupCount();
//		System.out.printf("count is:%d\n", count);
//		
//		for (int i = 1; i <= count; i++) {
//			String s = matcher.group(i);
//			System.out.printf("[%s]\n", s);
//		}
		
//		String sql = "create table exclusive host copy 5 chunksize 64m Video.Word(vid long , offset int, weight int, word nchar not case, type short, site short, show_time int, publish_time timestamp)";
//		SQLParser parser = new SQLParser();
//		parser.splitTable(sql, null);
		
//		SQLParser parser = new SQLParser();
		
//		String prefix = "clusters='12.56.89.234, 35.68.89.125' primehost=3 hostmode=share chunksize=68m chunkcopy=3 hostcache=yes";
//		Table table = new Table();
//		parser.splitTablePrefix(prefix, table);
//		System.out.printf("chunk size:%d\n", table.getChunkSize() / 1024 / 1024);

		String show_db = "show database pentium";
		String show_table = "show table video.item";
		
		SchemaParser p1 = new SchemaParser();
		String db = p1.splitShowSchema(show_db, false, null);
		
		TableParser p2 = new TableParser();
		Space space = p2.splitShowTable(show_table, false, null);
		
		System.out.println("database " + db);
		System.out.println("space " + space);

//		create database Video char='UTF-8' nchar='UTF-16' wchar='UTF-32'
//		String sql = "Create   schema   video pwd 'penti@@##um$#' char '#pentium' char 'gbk' nchar 'utf-16' wchar 'utf32'";
		String sql = "create schema Video char=UTF-8 nchar=UTF-16 wchar=UTF-32";
		p1.splitCreateSchema(sql, false, null);

//		Table table = new Table();
//		String prefix = "not cache prime host 12 share host copy 5 chunksize 64m";
//		parser.splitTablePrefix(prefix, table);
//		String task = "build task hunk to 12.0.3.44";
//		parser.splitBuildTask(task);

//		String sql = "load optimize video.word ";
//		parser.splitLoadOptimize(sql);

//		String table = "create table exclusive host copy 3 chunksize 64m Video.Word(vid long , offset int, weight int, word nchar not case, type short, site short, show_time int, publish_time timestamp)";
//		String index = "create index Video.Word (word primary, type, site, show_time, publish_time)";
//		String layout = " ";//create layout Video.Word ( type, site, show_time, publish_time)";
//		
//		table = "create table share host copy 0 chunksize 128m Video.Hunk(word nchar not case, hash int, count int, data raw)";
//		index = "create index Video.Hunk (word primary)";
//
//		Table object = parser.splitCreateTable(table, index, layout);
//		System.out.printf("split object is:%d\n", (object == null ? -1 : 1));
	}
	
	public void test2() {
		Space space = new Space("video", "word");
		Table table = new Table(space);
		CharAttribute field1 = new com.lexst.sql.column.attribute.CharAttribute((short)1, "word");
		IntegerAttribute field2 = new IntegerAttribute((short)2, "ABC", 0);
		IntegerAttribute field3 = new IntegerAttribute((short)3, "weight", 0);
		table.add(field1);
		table.add(field2);
		table.add(field3);
		
		byte[] b =  new UTF8().encode("Unix");
		Char ch1 = new Char((short)1,  b);
		com.lexst.sql.column.Integer id = new com.lexst.sql.column.Integer((short)2, 1);
		com.lexst.sql.column.Integer weight = new com.lexst.sql.column.Integer((short)3, 1);
		
		Row row = new Row();
		row.add(ch1);
		row.add(id);
		row.add(weight);
		
		Inject inject = new Inject(table);
		inject.add(row);
		
		b = row.build();
		System.out.printf("row byte size:%d\n", b.length);

		com.lexst.sql.column.Integer column = new com.lexst.sql.column.Integer((short)3, 2);		
		row.replace(column);
		b = row.build();
		System.out.printf("new row byte size:%d\n", b.length);
		
		byte[] data = inject.build();
		System.out.printf("data len is:%d\n", data.length);
		
		try {
			FileOutputStream out = new FileOutputStream("c:/ab.bin");
			out.write(data);
			out.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}
	
	
	
	public void testTable() {
		String sql_table = 
			"create table sm=dsm primehosts=1 hostmodel=share chunksize=128m chunkcopy=1 " +
			"hostcache=yes clusters = '12.89.12.88, 12.12.13.122' " +
			"video.word (Word char not case like prime key(20) default 'pentium' , " +
			"id int default 12, weight int, " +
			"birday date slave key default NOW() , " +
			"memo char packing aes:'peniutm@126.com'' and GZIP )";
		
//		String sql_index = "create index video.WORD (word(20) primary, id)";
		
//		String sql_table = "create table primehost=1 hostmode=share chunksize=128m chunkcopy=1 hostcache=yes video.word(Word char not case like  , id int, weight int, memo char packing aes :'peniutm@126.com'' )";
//		String sql_index = "create index video.WORD (word(20) primary, id)";

		
		TableParser parser = new TableParser();
		Table table = parser.splitCreateTable(sql_table, false, null);
		System.out.printf("space is %s\n\n\n", table.getSpace());
		
		sql_table = "create table sm=nsm primehosts=1 hostmodel=share "
				+ "chunksize=64m chunkcopy=1 hostcache=yes video.system "
				+ "(word char not case prime key(20) ,id int slave key default 1, weight int)";
		table = parser.splitCreateTable(sql_table, false, null);
		System.out.printf("space is %s\n", table.getSpace());
		
		String sql_permit = "GRANT create schema, create table, drop schema,drop table, drop user to scott.liang";
		GrantParser grant = new GrantParser();
		Permit permit = grant.split(sql_permit, false, null);
		System.out.printf("grant result is:%s\n", permit != null);
		
		sql_permit = "Revoke create schema, drop schema, drop table, create table , drop user from scott.liang";
		RevokeParser revoke = new RevokeParser();
		permit = revoke.split(sql_permit, false, null);
		System.out.printf("revoke result is:%s\n", permit != null);
		

//		String hostmode = "^\\s*(?i)HOSTMODE\\s*=\\s*(?i)(SHARE|EXCLUSIVE)\\s+(.*)$";
//		String hostmode = "^\\s*(?i)HOSTMODE\\s*=\\s*(?i)(SHARE|EXCLUSIVE)\\s*(.*)$";
//		String sql_prefix = "hostmode=share  sm=dsm";
//		Pattern pattern = Pattern.compile(hostmode);
//		Matcher matcher = pattern.matcher(sql_prefix);
//		if(matcher.matches()) {
//			String mode = matcher.group(1);
//			sql_prefix = matcher.group(2);
//			
//			System.out.printf("[%s]\n[%s]\n", mode, sql_prefix);
//		}
	}
	
	public void testPacking() {
		CharAttribute attribute = new CharAttribute((short)1, "system", "unix-system".getBytes());
		attribute.setPacking(Packing.ZIP, Packing.DES3, "regular express".getBytes());
//		attribute.setPacking(VariableAttribute.GZIP, 0, null);
		
		byte[] data = "UNIXSYSTEM-LINUXSYSTE-RESULT".getBytes();
		
//		byte[] b = Inflator.gzip(data, 0, data.length);
//		System.out.printf("compress gzip size:%d\n", b.length);
//		
//		b = Deflator.gzip(b, 0, b.length);
//		System.out.printf("uncompress gzip size:%d, %s\n", b.length, new String(b));
		try {
		byte[] result = VariableGenerator.enpacking(attribute, data, 0, data.length);
		System.out.printf("enpacking result size:%d\n", result.length);
		
		byte[] src = VariableGenerator.depacking(attribute, result, 0, result.length);
		System.out.printf("depacking result:%s\n", new String(src));
		} catch(IOException e){
			e.printStackTrace();
		}
	}
	
//	public static void main(String[] args) {
////		String s = "create layout pc.thinkpad (pentium desc, name asc, dist desc) ";
////		SQLParser parser = new SQLParser();
//////		parser.splitLayout(null, s);
////		s = "create database PC char='utf-8' nchar='utf-16' wchar='utf-32'";
////		parser.splitDatabase1(s);
//		
//		String sqlInsert = "insert into 56pc.thinkpad (vid, word, weight, site, stime, ptime, total) values(9999, 'pentium', 122, 1, 122, 2333, 1222)";
//		SQLParser parser = new  SQLParser();
////		parser.splitInsert(null, null, sqlInsert);
//		
////		SQLChecker checker = new SQLChecker();
////		checker.isInsert(null, null, sqlInsert);
//		
//		String sqldate = "2010.12.23";
//		int num = parser.splitDate(sqldate);
//		System.out.printf("date value is %d", num);
//	}

//	public static void main(String[] args) {
//////		String regex = "^\\s*(?i)CREATE\\s+(?i)DATABASE\\s*$";
////		String sql = "\'__UNIX__\'";
////		String regex = SQLParser.SQL_WHERE_LIKE1;
////		Pattern pattern = Pattern.compile(regex);
////		Matcher matcher = pattern.matcher(sql);
////		if(!matcher.matches()) {
////			System.out.println("not match!");
////			return;
////		}
////
////		String s1 = matcher.group(1);
////		String s2 = matcher.group(2);
//////		String s3 = matcher.group(3);
////		System.out.printf("[%s] - [%s]\n", s1, s2);
//
////		String value = "\'____UNIX____\'";
////		SQLParser parser = new SQLParser();
////		String[] s = parser.splitWhereChar(value);
////		if(s == null) {
////			System.out.println("not match!");
////			return;
////		}
////		System.out.printf("[%s] - [%s] - [%s]\n", s[0], s[1], s[2]);
//
//
//		String sql = "U58696千里不NIX";
//
//		SQLParser parser = new SQLParser();
//		String[] s = parser.splitWhereLikeElement(sql);
////		String[] s = parser.splitWhereLike1(sql);
////		if (s == null) s = parser.splitWhereLike2(sql);
////		if (s == null) s = parser.splitWhereLike3(sql);
//
//		if (s != null) {
//			System.out.printf("[%s]  [%s]  [%s]\n", s[0], s[1], s[2]);
//		}
//
////		String[] regexs = { SQL_WHERE_LIKE11, SQL_WHERE_LIKE12, SQL_WHERE_LIKE13, SQL_WHERE_LIKE14 };
////		for (int i = 0; i < regexs.length; i++) {
////			Pattern pattern = Pattern.compile(regexs[i]);
////			Matcher matcher = pattern.matcher(sql);
////			if(!matcher.matches()) continue;
////
////			String s1 = matcher.group(1);
////			String s2 = matcher.group(2);
////			String s3 = matcher.group(3);
////			System.out.printf("[%s]  [%s]  [%s]\n", s1, s2, s3);
////		}
//
////		String sql = "  ___%%%U58696千里不NIX% ";
////		String regex = "^\\s*(\\S*)(%)\\s*$";
////		Pattern pattern = Pattern.compile(regex);
////		Matcher matcher = pattern.matcher(sql);
////		if(!matcher.matches()) {
////			System.out.println("not match!");
////			return;
////		}
////		String s1 = matcher.group(1);
////		String s2 = matcher.group(2);
////		System.out.printf("[%s] - [%s]\n", s1, s2);
//	}

//	public static void main(String[] args) {
//		SQLParser parser = new SQLParser();
//
////		String sqlInsert = " INSERT INTO PC.THinkpad (name, age) VALUES ('pentium', 144)";
////		Insert insert = parser.splitInsert(null, null, sqlInsert);
//
//		String text = " 'pentium', 'linux', 12.34 , 'value' , 233 ";
//		String[] all = parser.splitValues(text);
//		for(String s : all) {
//			System.out.printf("%s\n", s);
//		}
//	}

//	public static void main(String[] args) {
//		// private final static String SQL_WHERE_ELEMENT_PREFIX = "^\\s*(?i)(AND|OR)\\s+(.+)\\s*$";
//
//		String where = " and col>1 or (col=2 or col=3 and col=1) OR col=5 and col6 = 6 AND (col7<7 or col8=8) or col9<>9";
//
//		Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_ELEMENT_PREFIX);
//		Matcher matcher = pattern.matcher(where);
//		if (matcher.matches()) {
//			String symbol = matcher.group(1);
//			byte outsideRelate = Condition.getRelated(symbol);
//			where = matcher.group(2);
//		}
//
//		SQLParser parser = new SQLParser();
////		ArrayList<String> array = new ArrayList<String>();
////		parser.splitWhereGroups(where, array);
////		System.out.println(where);
////		System.out.printf("size is:%d\n", array.size() );
////		for(String s : array) {
////			System.out.printf("[%s]\n", s);
////		}
//
//		String element = " name='pentium' ";// and pid=122 and product='hijava'";
//		String[] all = parser.splitWhereElement(element);
//
//		System.out.printf("\n[%s]\n", element);
//		for(String s : all) {
//			System.out.printf("%s\n", s);
//		}
//
////		String[] all = parser.splitWHERE(where, array);
////
////		System.out.printf("size is:%d\n", (all == null ? -1 : all.length));
////		if (all != null) {
////			for (int i = 0; i < all.length; i++) {
////				System.out.printf("%s\n", all[i]);
////			}
////		}
//	}

//	public static void main(String[] args) {
//		String sqlTop = " top 123 ";
//		String sqlRange = " range ( 1223 , 255) ";
//		SQLParser parser = new SQLParser();
//		Select select = new Select();
//		parser.splitSelectPrefixTop(null, select, sqlTop);
//		parser.splitSelectPrefixRange(null, select, sqlRange);
//
//		Table table = new Table();
//		table.add(new CharField((short) 1, "name", "value".getBytes()));
//		table.add(new CharField((short) 2, "id", "id".getBytes()));
//
//		SQLCharSet charset = new SQLCharSet();
//	}

//	public static void main(String[] args) {
////		String text = "Pentium (20) primary";
////		SQLParser sql = new SQLParser();
////		boolean b = sql.splitIndex(null, text, 0);
////		System.out.printf("result is %b\n", b);
//
////		String sqlDB = "Create   Database   sooget pwd 'penti@@##um$#' char '#pentium' char 'gbk' nchar 'utf-16' wchar 'utf32'";
//
//		String grant = "grant select,delete,update,all on db.table to pentium";
//		String revoke = "revoke all, select, delete from username, pentium ";
//		String revoke2 = "revoke all, select on list.table from username, pentium ";
//		String user = " Create User pentium IDENTIFIED BY 'system'";
////		user = "Create User pentium PASSWORD 'list'";
//		String sqlDB = "Create   Database   sooget char 'pentium' char 'gbk' nchar 'utf-16' wchar 'utf32'";
//		String sqlTable = "Create Table db.table (wid int not null default -1.22  , word char)";
//		String sqlIndex = "create index db.table (wid primary, word)";
//		SQLParser sql = new SQLParser();
////		sql.splitTable(sqlTable, sqlIndex);
////		sql.splitDatabase(sqlDB);
////		String text = "wid integer";
////		sql.splitField(text, 0);
//		sql.splitCreateUser(user);
////		sql.splitGrant(grant);
////		sql.splitRevoke(revoke2);
//	}
	
	public void testADC() {
		String sql = 
				"conduct init naming:system-init helo(string)='cosplay' " +
				" from naming:diffuse-adc sites:1, begin(int)=1, end(int)=1222 " +
				" to naming:aggregate-adc sites:2, say(string)='pentium' " +
				" subto naming:subto-adc sites:12, display(bool)=true "+
				" balance naming:balance-adc check(bool)=true " +
				" collect naming:collect-adc writeto:\"/system/cloud/raw.bin\" ";
		ConductParser parser = new ConductParser();
		parser.split(sql, null);
		System.out.println("adc resolve finished!\n");
		
		sql = 
			"direct init naming:system-init helo(string)='cosplay' " +
			" from naming:diffuse-adc sites:1, begin(int)=1, end(int)=1222 " +
			" to naming:aggregate-adc sites:2, say(string)='pentium' checks(date)='12/12/2013' " +
//			" subto naming:subto-adc sites:12, display(bool)=true "+
//			" balance naming:balance-adc check(bool)=true " +
			" collect naming:collect-adc writeto:\"/system/cloud/raw.bin\" ";
		
//		DirectParser parser2 = new DirectParser();
//		parser2.split(sql, null);
//		System.out.println("dc resolve finished!");
		
//		String WRITETO = "^\\s*(?i)WRITETO\\s*:\\s*([a-zA-Z/]{1,}[/\\:.a-zA-Z0-9]*)\\s*(.*)$";
//		sql = "writeto: f:/cloud/raw.bin values:\"bool=true, num=122, scs='system',element='scott'\"";
//		Pattern pattern = Pattern.compile(WRITETO);
//		Matcher matcher = pattern.matcher(sql);
//		if(matcher.matches()) {
//			String s1 = matcher.group(1);
//			String s2 = matcher.group(2);
//			System.out.printf("%s\n%s\n", s1, s2);
//		}
	}
	
	public void testRebuild() {
		String sql = " rebuild schema.table to 12.122.4.22, 122.90.22.33";
		RebuildParser parser = new RebuildParser();
		RebuildHostResult host = parser.split(sql, null);
		System.out.println(host.getSpace());
	}
	
	public void testRebuildTime() {
		String sql = "set rebuild time schema.table hourly '10:12'";
		SwitchTimeParser parser = new SwitchTimeParser();
		SwitchTime time = parser.split(sql, null);
		System.out.println(sql);
		System.out.printf("expired is:%s\n", time.isTouched());
		
		com.lexst.sql.column.Long value = new com.lexst.sql.column.Long();
		value.setValue( 1);// java.lang.Integer.MAX_VALUE);
//		com.lexst.sql.column.Column column = value;
		Object column = value;
		System.out.printf("String is:%s\n", column.toString());
		
		double d = 122.332f;
		System.out.printf("%.5f - %.5g\n",d, d);
		
	}

	public void testOrderby() {
		short columnId = 1;
		
		java.util.ArrayList<Row> array = new java.util.ArrayList<Row>();
		
		java.util.Random rnd = new java.util.Random(System.currentTimeMillis());
		for (int i = 0; i < 10; i++) {
			Row row = new Row();
			if (i > 0 && i % 3 == 0) {
				com.lexst.sql.column.Short sht = new com.lexst.sql.column.Short(
						columnId);
				row.add(sht);
			} else {
				short value = (short) (rnd.nextInt() & 0xffff);
				com.lexst.sql.column.Short sht = new com.lexst.sql.column.Short(
						columnId, value);
				row.add(sht);
			}
			array.add(row);
		}
		
		OrderBy order = new OrderBy(columnId, OrderBy.DESC);
		com.lexst.sql.statement.sort.OrderBySorter instance = new com.lexst.sql.statement.sort.OrderBySorter(order);
		
		java.util.Collections.sort(array, instance);
		
		for(Row row : array) {
			com.lexst.sql.column.Short sht = (com.lexst.sql.column.Short)row.find(columnId);
			System.out.printf("%s | %d\n", (sht.isNull() ? "YES" : "NO"), sht.getValue());
		}
		
	}
	
	public void testChar() {
		short columnId = 10;
		Char c1 = new Char(columnId);
		c1.setValue("value".getBytes());
		c1.setIndex("index".getBytes());
		short left = 1, right = 10;
		c1.addVWord(new VChar(columnId, left, right, "vword-stream".getBytes()));

		ByteArrayOutputStream stream = new ByteArrayOutputStream();

		c1.build(stream);

		byte[] b = stream.toByteArray();

		System.out.printf("build size:%d\n", b.length);
		System.out.printf("%s\n\n", new String(b));

		Char c2 = new Char(columnId);
		int len = c2.resolve(b, 0, b.length);

		System.out.printf("parse size: %d\n", len);
	}
	
	public void testRow() {
		Row row = new Row();

		Sheet sheet = new Sheet();
		for (short columnId = 1; columnId <= 2000; columnId++) {	
			// char
			byte[] value = String.format("INFO:%d", columnId).getBytes();
			byte[] index = String.format("IDX:%d", columnId).getBytes();
			Char word = new Char(columnId, value, index);
			word.addVWord(new VChar( columnId, (short)12, (short)1, "VWORD".getBytes()));
			row.add(word);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.CharAttribute(columnId, "so_so"));
			
			//wchar
			columnId++;
			WChar wword = new WChar(columnId, value, index);
			row.add(wword);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.WCharAttribute(columnId, "colow" ,value));
			
			// raw
			columnId++;
			Raw raw = new Raw(columnId, index);
			row.add(raw);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.RawAttribute(columnId, "ss".getBytes()));
			
			// integer
			columnId++;
			com.lexst.sql.column.Integer val = new com.lexst.sql.column.Integer(columnId, 10);
			row.add(val);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.IntegerAttribute(columnId, 122));
			
			// short
			columnId++;
			com.lexst.sql.column.Short sht = new com.lexst.sql.column.Short(columnId, (short)990);
			row.add(sht);
			sheet.add(columnId-1, new com.lexst.sql.column.attribute.ShortAttribute(columnId, (short)12));
			
			// long
			columnId++;
			com.lexst.sql.column.Long lng = new com.lexst.sql.column.Long(columnId, 1222L);
			row.add(lng);
			sheet.add(columnId-1, new com.lexst.sql.column.attribute.LongAttribute(columnId, 1233L));
			
			// float
			columnId++;
			com.lexst.sql.column.Float real = new com.lexst.sql.column.Float(columnId, 12.3f);
			row.add(real);
			sheet.add(columnId-1, new com.lexst.sql.column.attribute.FloatAttribute(columnId, 122f));
			
			// double
			columnId++;
			com.lexst.sql.column.Double ble = new com.lexst.sql.column.Double(columnId, 122.33f);
			row.add(ble);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.DoubleAttribute(columnId, 1223f));
			
			// timestamp
			columnId++;
			com.lexst.sql.column.Timestamp ts = new com.lexst.sql.column.Timestamp(columnId, System.currentTimeMillis());
			row.add(ts);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.TimestampAttribute(columnId, 0L));
			
			// date
			columnId++;
			com.lexst.sql.column.Date date = new com.lexst.sql.column.Date(columnId, 199);
			row.add(date);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.DateAttribute(columnId, 122));
			
			// time
			columnId++;
			com.lexst.sql.column.Time time = new com.lexst.sql.column.Time(columnId, 333);
			row.add(time);
			sheet.add(columnId - 1, new com.lexst.sql.column.attribute.TimeAttribute(columnId, 122));
		}

//		byte[] b = null;
//		long time = System.currentTimeMillis();
//		for (int i = 0; i < 1000; i++) {
//			b = row.build();
//		}
//		long usedtime = System.currentTimeMillis() - time;
		
		int capacity = 1024 * 1024 * 2;
		int maxlen = 1024 * 30;
		ByteArrayOutputStream buff = new ByteArrayOutputStream(capacity);
		long time = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			row.build(maxlen, false, buff);
		}
		long usedtime = System.currentTimeMillis() - time;
		byte[] b = buff.toByteArray();
		
		System.out.printf("build byte size is:%d, usedtime:%d\n", b.length, usedtime);
		
		byte[] s = null;
		try {
		s = com.lexst.sql.util.VariableGenerator.compress(Packing.GZIP, b, 0, b.length);
		System.out.printf("gzip compress size:%d, %d\n", s.length, s.length/1024/1024);
		} catch(IOException e) {
			e.printStackTrace();
		}

		Row row2 = new Row();
		time = System.currentTimeMillis();
		int size = row2.resolve(sheet, b, 0, b.length);
		usedtime = System.currentTimeMillis() - time;
		System.out.printf("resolve usedtime:%d, end is %d\n", usedtime, size);
		
//		capacity = 1024 * 1024 * 2;
//		s = Arrays.copyOf(s, capacity);
//		System.out.printf("capacity is:%d, %Xm\n", capacity, capacity/1024/1024);
		
		s = "ABC千里江山寒色远，芦花深处泊秀，笛在朋明楼。pentiums".getBytes();		
		s = Arrays.copyOfRange(s, 3, s.length);
		System.out.println(new String(s));
		
	}
	
	public void testWChar() {
		short columnId = 2;
		byte[] value = "WCHAR VALUE".getBytes(); // String.format("INFO:%d",
													// columnId).getBytes();
		byte[] index = "WCHAR INDEX".getBytes(); // String.format("IDX:%d",
													// columnId).getBytes();
		WChar word = new WChar(columnId, value, index);
		short left = 1, right = 1;
		for (int i = 0; i < 10; i++) {
			byte[] b = String.format("VWCHAR%d", i + 1).getBytes();
			VChar vc = new VChar(columnId, left++, right++, b);
			word.addVWord(vc);
		}

		ByteArrayOutputStream stream =  new ByteArrayOutputStream();
		word.build(stream);
		byte[] b = stream.toByteArray();
		System.out.printf("build size:%d\n", b.length);
		
		System.out.printf("%s\n", new String(b));
		
		try {
			FileOutputStream writer = new FileOutputStream("c:/wchar.bin");
			writer.write(b, 0, b.length);
			writer.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}

		WChar word2 = new WChar();
		int size = word2.resolve(b, 0, b.length);
		System.out.printf("resolve size:%d\n", size);
	}
	
	public void testNumber() {
		short columnId = 1;
		ByteArrayOutputStream buff =  new ByteArrayOutputStream();
		
		for(int i = 0; i < 10; i++) {
			com.lexst.sql.column.Float real = new com.lexst.sql.column.Float(columnId, 12.32f);
			real.build(buff);
		}

		byte[] b = buff.toByteArray();
		System.out.printf("buff size:%d\n", b.length);
		
		for(int off = 0; off < b.length; ) {
			com.lexst.sql.column.Float real = new com.lexst.sql.column.Float();
			int len = real.resolve(b, off, b.length - off);
			off += len;
			System.out.printf("resolve size:%d, value:%f\n", len, real.getValue());
		}
		
	}

	public void testCreateTable() {
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
		slave.setFunction(new Count());
		
//		slave.setLike(true);
//		slave.setSentient(false);
		
		table.add(primary);
		table.add(slave);
		
		byte[] b = table.build();
		System.out.printf("build byte size:%d\n", b.length);
		
		Table t2 = new Table();
		int len = t2.resolve(b, 0, b.length);
		System.out.printf("resolve size is:%d\n", len);
		System.out.printf("stroage model:%d\n", t2.getStorage());
		System.out.printf("chunk size:%d\n", t2.getChunkSize()/1024/1024);
		
		// temporary code, start
		if(len != 0) return;
		// end
		
		try {
			FileOutputStream out = new FileOutputStream("d:/head.bin");
			out.write(b);
			out.close();
		} catch(IOException exp) {
			exp.printStackTrace();
		}
		
		
		ByteArrayOutputStream buff =  new ByteArrayOutputStream();
		for(int i = 0; i < 3; i++) {
			columnId = 1;
			com.lexst.sql.column.Short id1 = new com.lexst.sql.column.Short(columnId++, columnId);
			
			byte[] value = String.format("VALUE%d", i).getBytes();
			byte[] index = String.format("INDEX%d", i).getBytes();
			com.lexst.sql.column.Char id2 = new com.lexst.sql.column.Char(columnId, value, index);
			
			Row row = new Row();
			row.add(id1);
			row.add(id2);
			
			b = row.build();
			buff.write(b, 0, b.length);
		}
		
		b = buff.toByteArray();
		System.out.printf("row build size:%d\n", b.length);
		
		try {
			FileOutputStream out = new FileOutputStream("d:/rows.bin");
			out.write(b);
			out.close();
		} catch(IOException exp) {
			exp.printStackTrace();
		}
		
		for (int off = 0; off < b.length;) {
			Row row = new Row();
			len = row.resolve(table, b, off, b.length - off);
			System.out.printf("row resolve size:%d\n", len);
			off += len;
		}
	}
	
	public void testInject() {
		String short_name = "id";
		String char_name = "word";
		
		Space space = new Space("Video", "Words");
		Table table = new Table(space);
		table.setStorage(Type.DSM);
		table.setChunkSize(64 * 1024 * 1024);
		table.setCopy(3);
		table.setPrimes(1);
		table.setCaching(false);
		table.setMode(Table.SHARE);
		
		short columnId = 1;
		com.lexst.sql.column.attribute.ShortAttribute primary = new com.lexst.sql.column.attribute.ShortAttribute(columnId++, short_name, (short)99);
		primary.setKey(Type.PRIME_KEY);
		
		com.lexst.sql.column.attribute.CharAttribute slave = new com.lexst.sql.column.attribute.CharAttribute(columnId, char_name, "unix".getBytes());
		slave.setLike(true);
		slave.setKey(Type.SLAVE_KEY);

		table.add(primary);
		table.add(slave);

		byte[] b = table.build();
		System.out.printf("table [%s] buile byte size:%d\n", table.getSpace(), b.length);

//		Table t2 = new Table();
//		int len = t2.resolve(b, 0, b.length);
//		System.out.printf("resolve size is:%d\n", len);
//		System.out.printf("stroage model:%d\n", t2.getStorage());
//		System.out.printf("chunk size:%d\n", t2.getChunkSize()/1024/1024);

		// generate "INJECT"
		Inject inject = new Inject(table);
		for(short i = 0; i < 20; i++) {
			columnId = 1;
			com.lexst.sql.column.Short sht = new com.lexst.sql.column.Short(columnId, i);

			String value = String.format("VALUE%d", i);
			String index = String.format("INDEX%d", i);
			columnId++;
			byte[] data = value.getBytes();
			byte[] key = index.getBytes();
			com.lexst.sql.column.Char word = new com.lexst.sql.column.Char(columnId, data, key);
						
			short likeid = (short)(columnId | 0x8000);
			short left = 1, right = 1;
			byte[] vindex = String.format("fuzzy%d", i).getBytes();
			VChar vague = new VChar(likeid, left, right, vindex);
			word.addVWord(vague);

			Row row = new Row();
			row.add(sht);
			row.add(word);
			
			inject.add(row);
		}
		
		b = inject.build();
		System.out.printf("inject [%s] build size:%d\n", space, b.length);
		
		try {
			FileOutputStream out = new FileOutputStream("d:/inject.bin");
			out.write(b);
			out.close();
		} catch(IOException exp) {
			exp.printStackTrace();
		}

		// generate "SELECT"
		columnId = 1;
		com.lexst.sql.column.Short sht = new com.lexst.sql.column.Short(columnId, (short)2);
		ShortIndex index1 = new ShortIndex((short)2, sht);
		Condition condi1 = new Condition(short_name, Condition.EQUAL, index1);

		columnId++;
		byte[] index = String.format("INDEX3").getBytes();
		com.lexst.sql.column.Char word = new com.lexst.sql.column.Char(columnId, index);
		b = word.getValid();
		long sortid = com.lexst.util.Sign.sign(b, 0, b.length);
		LongIndex index2 = new LongIndex(sortid, word);
		Condition condi2 = new Condition(Condition.OR, char_name, Condition.EQUAL, index2);

		System.out.printf("char [%s] sortid is:%x\n", new String(b), sortid);

		condi1.setLast(condi2);
		
		ShowSheet sheet = new ShowSheet();
		sheet.add(new ColumnElement(primary.getSpike(), "short"));
		sheet.add(new ColumnElement(slave.getSpike(), "char"));
		
		Select select = new Select(space);
//		short[] showIds = new short[] { 1, 2 };
//		select.setShowId(showIds);
		select.setCondition(condi1);
		select.setShowSheet(sheet);

		b = select.build();
		System.out.printf("select [%s] build size is:%d\n", space, b.length);
		
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
		
		// generate "DELETE"
		Delete delete = new Delete(space);
		delete.setCondition(condi1);
		b = delete.build();
		System.out.printf("delete [%s] build size is:%d\n", space, b.length);
		
//		Delete d2 = new Delete();
//		int len = d2.resolve(b, 0, b.length);
//		System.out.printf("delete resolve size is:%d\n", len);
		
		try {
			FileOutputStream s = new FileOutputStream("d:/delete.bin");
			s.write(b, 0, b.length);
			s.close();
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}
	}
	
	public void testRowParse() {
		String filename = "d:/ColumnSelectResult.bin";
		File file = new File(filename);
		byte[] b = new byte[(int) file.length()];
		System.out.printf("%s size is:%d\n", filename, file.length());

		try {
			FileInputStream in = new FileInputStream(file);
			in.read(b, 0, b.length);
			in.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		AnswerFlag flag = new AnswerFlag();
		final int size = flag.resolve(b, 0, b.length);
		System.out.printf("resolve size:%d\n", size);
		System.out.printf("data size:%d\n", flag.getSize());
		System.out.printf("DSM:%s, NSM:%s\n", flag.isDSM(), flag.isNSM() );
		System.out.printf("space is:%s\n", flag.getSpace());
		System.out.printf("row count:%d, column count:%d\n\n", flag.getRows(), flag.getColumns());
		
		byte[] s = flag.build();
		System.out.printf("build size:%d\n", s.length);
		
		for (int i = 0; i < s.length; i++) {
			if (b[i] != s[i]) {
				System.out.printf("cannot match, at %d\n", i);
			}
		}
		
		
		short columnId = 1;
		com.lexst.sql.column.attribute.ShortAttribute primary = new com.lexst.sql.column.attribute.ShortAttribute(columnId++, "id", (short)99);
		primary.setKey(Type.PRIME_KEY);
		
		com.lexst.sql.column.attribute.CharAttribute slave = new com.lexst.sql.column.attribute.CharAttribute(columnId, "word", "unix".getBytes());
		slave.setLike(true);
		slave.setKey(Type.SLAVE_KEY);

		Sheet sheet = new Sheet();
		sheet.add(0, primary);
		sheet.add(1, slave);
		
//		RowParser parser = new RowParser(sheet, b, 0, b.length);
//		
//		java.util.List<Row> rows = parser.split(b, size, b.length - size);
//		System.out.printf("split row count:%d\n", rows.size());
//		
//		rows = parser.finish();
//		System.out.printf("finish row count:%d\n", rows.size());

		
		RowParser res = new RowParser(flag, sheet);
		int len = res.split(b, size, b.length - size);
		System.out.printf("resolve is:%d + %d = %d\n", size, len , b.length);
		java.util.List<Row> rows = res.flush();
		System.out.printf("flush size:%d\n", rows.size());
		rows = res.flush();
		System.out.printf("flush size:%d\n", rows.size());

	}
	
//	public void testSQLFunction() {
//		com.lexst.sql.function.Format format = new com.lexst.sql.function.Format();
//		Object[] params = new Object[2];
//		params[0] = new com.lexst.sql.function.Now();
//		params[1] = new String("YYYY/MM/DD");
//		
//		format.setParams(params);
//		
//		Object[] res = format.getParams();
//		for(int i = 0; i < res.length; i++){
//			System.out.printf("class name:%s\n", res[i].getClass().getName());
//		}
//	}
	
//	public void testWhereParse() {
//		String sql = "id1=122 and (id2=122 or id2==900) or (id3=135 and id5=876)";
//		sql = "id1=unix and id3 = 122";
//		sql = "id1>12 and (id9<>'8999' and id2 LIKE 'un\\'()ix' and id2 Like 'pen(ti[]um') or (id5=23 and id6=67) ";
//		sql = "(id>12 or id=23) and (id=9 or id=09)  or (id=98 or id=12)";
//		WhereParser parser = new WhereParser();
//		String[] s = parser.splitGroup(sql);
//		
//		System.out.println(sql);
//		for(int i = 0; i < s.length; i++) {
//			System.out.println(s[i]);
//		}
//		
//		System.out.println();
//		
//		String reg = "^\\s+(?i)(?:AND|OR)\\s+$";
//		reg = "\\s+(?i)(?:AND|OR)\\s+";
//		sql = "id>12 and id<122 or id=13";
//		Pattern pattern = Pattern.compile(reg);
//		s = pattern.split(sql);
//		System.out.println(sql);
//		
//		s = parser.splitByLogic(sql);
//		for(int i =0; i<s.length; i++) {
//			System.out.println(s[i]);
//		}
////		Matcher matcher = pattern.matcher(sql);
//
//		
////		s = parser.splitPair(s[0]);
////		System.out.println();
////		for(int i = 0; i < s.length; i++) {
////			System.out.println(s[i]);
////		}
//	}
	
	public void testWhereQuery() {
		Space space = new Space("video", "word");
		Table table = new Table(space);
		
		ShortAttribute a1 = new ShortAttribute((short)1, "id", (short)0);
		CharAttribute a2 = new CharAttribute((short)2, "word");
		IntegerAttribute a3 = new IntegerAttribute((short)3, "weight", 0);
		a1.setKey(Type.PRIME_KEY);
		a2.setKey(Type.SLAVE_KEY);
		a3.setKey(Type.SLAVE_KEY);
		
		table.add(a2);
		table.add(a1);
		table.add(a3);
		
//		Select select = new Select(space);
		
		String sql = "(id>12 or id=23) and   (id=9 or id=09)   or (id=98 or id=12)";
//		where = "id=100 and word='unixsystem' AND word NOT BETWEEN   'value1' and 'value2'  and (word='1222' OR id=122 and (word='unix' or word='09') and id=18)   or (id=98 or id=12)";
		sql = "id=100 and word='unixsystem' or id in (12, 23)  and (word <> '1222' OR id=122 and (word='unix' or word!='09') and id=18)   or (id=98 or id=12)";
		System.out.printf("%s\n\n", sql);
		
		WhereParser parser = new WhereParser();
		Condition condi = parser.split(table, null, sql);
		printCondition(condi);
		
		System.out.println("\n\n-----------\n");
		
		Map<Space, Table> tables = new TreeMap<Space, Table>();
		tables.put(space, table);
		
		sql = "id=100 or word in (select word from video.word where id =122 and word='千里江山')";

		sql = "word in (select word from video.word where word='pentium' AND id=(select id from video.word where id=(select id from video.word where word <> '千里江山')))";
		condi = parser.split(table, null, sql);

		System.out.println(sql);
//		this.printCondition(condi);
		
		System.out.printf("\non select condition is:%s\n", condi.onSubSelect() ? "YES" : "FALSE");
		
//		WhereIndex selectIndex = condi.getLastSelectIndex();

		System.out.println("last select condition is:");
		condi = Condition.findLastSelectCondition(condi);
		if (condi != null) {
			this.printCondition(condi);
		}

//		Select select2  = Condition.findLastSelect( condi );//Type.SELECT_INDEX);
////		WhereIndex selectIndex = condi.getValue();
////		System.out.printf("find result is:%s\n\n", condi != null ? "FOUND" : "NOT FOUND");
//		
//		if(select2 != null) {
////			SelectIndex index = (SelectIndex)condi.getValue();
////			System.out.printf("%s\n",  condi.getColumnName());
//			this.printCondition( select2.getCondition() );// index.getSelect().getCondition() );
//		}

//		short value = parser.splitShort("+12300");
//		String s = java.lang.Long.toString( java.lang.Long.MAX_VALUE , 16 );
//		System.out.printf("\nvalue:%d, %s size:%d\n", value, s, s.length());
//		
//		BigDecimal dec = new BigDecimal("-12.56");
//		System.out.printf("%f - %f - %f\n", dec.floatValue(), java.lang.Float.MIN_VALUE, java.lang.Float.MAX_VALUE);
//		if(java.lang.Float.MIN_VALUE <= dec.floatValue() && dec.floatValue() <= java.lang.Float.MAX_VALUE ){
//			System.out.println("OKD!");
//		}
	}
	
	public void testHavingQuery() {
		Space space = new Space("video", "word");
		Table table = new Table(space);
		
		ShortAttribute a1 = new ShortAttribute((short)1, "id", (short)0);
		CharAttribute a2 = new CharAttribute((short)2, "word");
		IntegerAttribute a3 = new IntegerAttribute((short)3, "weight", 0);

		table.add(a2);
		table.add(a1);
		table.add(a3);
		
		String having = "sum(weight) > 1 and (sum(weight) < 2 or sum(weight) < 3  and (sum(weight)>6) ) or sum(weight) > 4";
		System.err.printf("%s\n", having);
		
		HavingParser parser = new HavingParser();
		Situation situa = parser.split(table, having);
		printSutiation(situa);
//		System.out.println("finishE!");
	}
	
	private void printCondition(Condition condi) {
		if(condi == null) return;
		
		System.out.printf("\n%s |%s |%s |%s",
				Condition.translateLogic(condi.getOutsideRelation()),
				Condition.translateLogic(condi.getRelation()),
				condi.getColumnName(),
				Condition.translateCompare(condi.getCompare()));
		
		for(Condition partner : condi.getPartners()) {
			printCondition(partner);
		}
		this.printCondition(condi.getNext());
	}
	
	private void printSutiation(Situation condi) {
		if(condi == null) return;
		
		System.out.printf("\nclass name:%s - %s - %s", 
				condi.getFunction().getClass().getName(),
				Situation.translateLogic( condi.getOutsideRelation()),
				Situation.translateLogic(	condi.getRelation()));
		for(Situation partner : condi.getPartners()) {
			printSutiation(partner);
		}
		this.printSutiation(condi.getNext());
	}

	public void testWCharAttribute() {
		String text = "建T表时，如果是DSM存储，全部未定义KEY的列都是SLAVE KEY";
		WCharAttribute attri = new WCharAttribute();
		attri.setColumnId((short)1);
		attri.setKey(Type.SLAVE_KEY);
		attri.setSentient(false);
		attri.setLike(true);
		attri.setIndexSize(4);
		
		attri.setPacking(Packing.GZIP, Packing.DES, "Pentium@126.com".getBytes());
		
		try {
		Charset charset = VariableGenerator.getCharset(attri);
		byte[] value = VariableGenerator.toValue(attri, text);
		
		value = VariableGenerator.depacking(attri, value, 0, value.length);
		System.out.printf("decode value:%s\n", charset.decode(value, 0, value.length));
		
		byte[] index = VariableGenerator.toIndex(true, attri, text);
		index = VariableGenerator.depacking(attri, index, 0, index.length);
		System.out.printf("decode index:%s\n", charset.decode(index, 0, index.length));
		
		java.util.List<VWord> list = VariableGenerator.toVWord(attri, charset.decode(index, 0, index.length) );
		for(VWord vword: list) {
			byte[] b = vword.getIndex();
			b = VariableGenerator.depacking(attri, b, 0, b.length);
			System.out.printf("%d - %d | %s\n", vword.getLeft(), vword.getRight(), charset.decode(b, 0, b.length) );
		}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		text = "1222333af989d92";
//		TableParser parser = new TableParser();
//		byte[] b = parser.hexToBytes(text);
//		System.out.printf("text size:%d, b size:%d\n", text.length(), b.length);
	}
	
	public void testWCharSector() {
		WCharSector sector = new WCharSector();
		for (int begin = 0; begin < 1000; begin += 100) {
			sector.add(begin, begin + 99);
		}
		
		byte[] b = sector.build();
		System.out.printf("build size:%d\n%s\n",
				b.length, new String(b, 0, b.length));
		
		WCharSector sector2 = new WCharSector();
		int size = sector2.resolve(b, 0, b.length);
		System.out.printf("resolve size:%d\n", size);
		
		try {
			java.io.ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
			java.io.ObjectOutputStream o = new ObjectOutputStream(buff);
			
			o.writeObject(sector);
			o.flush();
			
			b = buff.toByteArray();
			System.out.printf("buff size is:%s\n", b.length);
			
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		
		Class<?>[] clses = ColumnSector.class.getInterfaces();
		for(int i = 0; clses!=null && i <clses.length; i++) {
			System.out.printf("%s\n", clses[i].getName());
		}
		
		clses = ColumnSector.class.getDeclaredClasses();
		for(int i = 0; clses!=null && i <clses.length; i++) {
			System.out.printf("%s\n", clses[i].getName());
		}
		
		
		Class<?> cls = ColumnSector.class.getEnclosingClass();
//		System.out.printf("name:%s\n", cls.getName());
	}
	
	public void testUser() {
		String username = "Lexst";
		String password = "www.sooget.com";
		User user = new User(username, password);
		System.out.printf("%s\n", user);
		
		User second = (User)user.clone();
		System.out.printf("compare is:%d\n", second.compareTo(user));
		
		Administrator admin = new Administrator(username, password);
		
		System.out.printf("compareTo:%d\n", user.compareTo(admin));
		System.out.printf("compareTo:%d\n", admin.compareTo(second));
		System.out.printf("equal is:%s\n", user.equals(admin));
		
		System.out.printf("equals is:%s\n", Arrays.equals(user.getUsername(), admin.getUsername()));
//		user.matchUsername( admin.getUsername() ));
		
//		String users = " Pentium , Linux , System, Order";
//		String[] all = users.split("\\s*,\\s*");
//		for(String s : all) {
//			System.out.printf("[%s]\n", s);
//		}
		
		ArrayList<User> a1 = new ArrayList<User>();
		a1.add(user);
		ArrayList<User> a2 = new ArrayList<User>();
//		a2.addAll(a1);
//		a2.add((User)a1.get(0).clone());
		a2.add(user);
		System.out.printf("result is:%s\n", a1.get(0) == a2.get(0));
		
		String u2 = username;
		System.out.printf("string result:%s\n", u2 == username);
		
//		private final static String IPAS = "10.0.0.0";
//		private final static String IPAE = "10.255.255.255";
//		private final static String IPBS = "172.16.0.0";
//		private final static String IPBE = "172.31.255.255";
//		private final static String IPCS = "192.168.0.0";
//		private final static String IPCE = "192.168.255.255";
		int address = 0;
		boolean fs = 
			(((address >>> 24) & 0xFF) == 10) || 
			((((address >>> 24) & 0xFF) == 172) && (((address >>> 16) & 0xF0) == 16)) || 
			((((address >>> 24) & 0xFF) == 192) && (((address >>> 16) & 0xFF) == 168));
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SQLDebug debug = new SQLDebug();
//		debug.test2();
//		debug.testADC();
//		debug.testRebuild();
//		debug.testRebuildTime();
		
//		debug.testTable();
		
//		debug.testPacking();
		
//		debug.testOrderby();
		
//		debug.testChar();
//		debug.testRow();
		
//		debug.testWChar();
//		debug.testNumber();
		
//		debug.testCreateTable();
//		debug.testInject();
		
//		debug.testRowParse();
		
//		debug.testSQLFunction();
		
//		debug.testWhereParse();
		
//		debug.testWhereQuery();
		
		debug.testParseInject();
		
//		debug.testHavingQuery();
		
//		debug.testWCharAttribute();
		
//		debug.textParseSelect();
		
//		debug.testWCharSector();
		
//		debug.testUser();
	}

}
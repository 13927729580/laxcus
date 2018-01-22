/**
 *
 */
package com.lexst.debug;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author siven
 *
 */
public class Parrent {

	/**
	 *
	 */
	public Parrent() {
		// TODO Auto-generated constructor stub
	}

	public void testCreateIndex() {
		String regex = "^\\s*(?i)create\\s*(?i)index\\s*(\\p{Graph}+)\\s*\\((\\p{Print}+)\\)\\s*$";
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);

		String s = "CREATE INDEX CPU.Linux (id parmary, word(12), tag)";
		java.util.regex.Matcher matcher = pattern.matcher(s);
		if (!matcher.matches()) {
			System.out.println("invalid!");
			return;
		}
		int count = matcher.groupCount();
		for(int i = 1; i<=count; i++) {
			String f = matcher.group(i);
			System.out.println(f);
		}
	}

	public void testCreateTable() {
//		String regex = "^\\s*create(?i)\\s*table(?i)\\s*(\\p{Print}+)\\s*$";
		String regex = "^\\s*(?i)create\\s*(?i)table\\s*(\\p{Graph}+)\\s*\\((\\p{Print}+)\\)\\s*$";
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);

		// word nchar not null not like not case packing 'ast' default 'abc'

		// packing是数据压缩算法

		String s = "CREATE TABLE CPU.Alphone (id int, word char, ntag nchar, wtag wchar)";
		java.util.regex.Matcher matcher = pattern.matcher(s);
		if (!matcher.matches()) {
			System.out.println("invalid!");
			return;
		}
		int count = matcher.groupCount();
		System.out.printf("create table count:%d\n", count);
		for (int i = 1; i <= count; i++) {
			String f = matcher.group(i);
			System.out.println(f);
		}
	}

	public void splitT() {
		//\( [^()]* \)
//		String regex = "^\\s*(.+),(.+)$";
		String regex = "^,$";
		String s = "wid int not, null case default 100, CPU char not null case like default '1222', LINUX long null case like default 9999";

		String[] ss = s.split(",");
		for(String text: ss) {
			System.out.printf("%s\n", text);
		}

		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
		ss = pattern.split(regex);
		for(String text: ss) {
			System.out.printf("%s\n", text);
		}

//		java.util.regex.Matcher matcher = pattern.matcher(s);
//		if (!matcher.matches()) {
//			System.out.println("invalid!");
//			return;
//		}
//		int count = matcher.groupCount();
//		System.out.printf("create table count:%d\n", count);
//		for (int i = 1; i <= count; i++) {
//			String f = matcher.group(i);
//			System.out.println(f);
//		}
	}

	public void testUnit() {
		String regex = "^\\s*(\\p{Graph}+)\\s+(\\p{Graph}+)\\s+(\\p{Print}+)\\s*$";
		String s = "Word NCHAR not null";

		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
		java.util.regex.Matcher matcher = pattern.matcher(s);
		if (!matcher.matches()) {
			System.out.println("invalid!");
			return;
		}
		int count = matcher.groupCount();
		System.out.printf("create table count:%d\n", count);
		for (int i = 1; i <= count; i++) {
			String f = matcher.group(i);
			System.out.println(f);
		}
	}

	public void testUnit3() {
		// |nchar|wchar|small|smalint|int|bigint|long|real|double|date|time|timestamp
		String prefix = "^\\s*(?i)([a-zA-Z]{1,}[_a-zA-Z0-9]*)\\s+(?i)(RAW|BINARY|CHAR|NCHAR|WCHAR|SHORT|SMALLINT|INTEGER|INT|LONG|BIGINT|REAL|DOUBLE|TIMESTAMP|DATE|TIME{1})(.+)$";
//		String prefix = "^\\s*(?i)([a-zA-Z]{1,}[_a-zA-Z0-9]*)\\s+(?i)([SHORT]{1}|[SMALL]{1}|[SMALLINT]{1}|[INT]{1}|[LONG]{1}|[BIGINT]{1})(.+)$";
		String text = "WORD3_235I WChar NOT NULL NOT LIKE NOT CASE DEFAULT 'PENTIUM' ";

		Pattern pattern = Pattern.compile(prefix);
		Matcher matcher = pattern.matcher(text);
		if (!matcher.matches()) {
			System.out.println("invalid!");
			return;
		}
		int count = matcher.groupCount();
		if (count != 3) {
			return;
		}
		System.out.printf("create table count:%d\n", count);
		String colname = matcher.group(1);
		String dataType = matcher.group(2);
		String suffix = matcher.group(3);

		for (int i = 1; i <= count; i++) {
			String f = matcher.group(i);
			System.out.printf("[%s] %d - %d\n", f, matcher.start(i), matcher.end(i));
		}

//		String regex1 = "^\\s*(?i)(NULL)(.+)$";
//		String regex3 = "^\\s*(?i)(LIKE)(.+)$";
//		String regex5 = "^\\s*(?i)(CASE)(.+)$"; //sentient
//		// 压缩类型
//		String regex9 = "^\\s*(?i)(PACKING)\\s*([a-zA-Z]{1,}[-_a-zA-Z0-9]*)(.+)$";
//
//		String regex2 = "^\\s*(?i)(NOT)\\s+(?i)(NULL)(.+)$";
//		String regex4 = "^\\s*(?i)(NOT)\\s+(?i)(LIKE)(.+)$";
//		String regex6 = "^\\s*(?i)(NOT)\\s+(?i)(CASE)(.+)$";
//
//		// 缺省值
//		String regex10 = "^\\s*(?i)(DEFAULT)\\s+([\\p{Print}]+)(.+)$";
//		String regex11 = "^\\s*(?i)(DEFAULT)\\s+\\'(\\p{Print}+)\\'(.+)$";
//		String regex12 = "^\\s*(?i)(DEFAULT)\\s+\\'(.+)\\'(.+)$";
//		String regex13 = "^\\s*(?i)(DEFAULT)\\s+([0-9]*[\\.]{0,1}[0-9]*)(.+)$";
//
//		String[] regex = {regex1, regex2};
//
//		for (int i = 0; i < regex.length; i++) {
//			Pattern pattern = Pattern.compile(regex[i]);
//		}
	}

	public void testUnit2() {
		String regex1 = "^\\s*(?i)(NULL)(.+)$";
		String regex3 = "^\\s*(?i)(LIKE)(.+)$";
		String regex5 = "^\\s*(?i)(CASE)(.+)$";

		String regex2 = "^\\s*(?i)(NOT)\\s+(?i)(NULL)(.+)$";
		String regex4 = "^\\s*(?i)(NOT)\\s+(?i)(LIKE)(.+)$";
		String regex6 = "^\\s*(?i)(NOT)\\s+(?i)(CASE)(.+)$";

		// 压缩类型
		String regex9 = "^\\s*(?i)(PACKING)\\s*([a-zA-Z]{1,}[-_a-zA-Z0-9]*)(.+)$";
		// 缺省值
		String regex10 = "^\\s*(?i)(DEFAULT)\\s+([\\p{Print}]+)(.+)$";
		String regex11 = "^\\s*(?i)(DEFAULT)\\s+\\'(\\p{Print}+)\\'(.+)$";
		String regex12 = "^\\s*(?i)(DEFAULT)\\s+\\'(.+)\\'(.+)$";
		String regex13 = "^\\s*(?i)(DEFAULT)\\s+([0-9]*[\\.]{0,1}[0-9]*)(.+)$";
//		String regex10 = "^\\s*(?i)(DEFAULT)\\s+([\\']|[\\p{Print}]+)$";

//		String regex7 = "^\\s*(?i)(PACKING)\\s+\\(\\'(\\p{ALNUM}+)\\'\\)(.+)%";
//[\\']
//		String regex9 = "^\\s*(?i)(PACKING)\\s*([a-zA-Z0-9_-]+)(.+)$";
//		String regex = "^\\s*(?i)(not null)|(?i)(not like)\\s*$";
//		String text = "not    null pentium unix  ";
//		String text = " packing  CRC_32 UNIX";
//		String text = " default 'Pentium' 中'国'  ";
		String text = " default 100052.366 人档要要 pentium wisk";

		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex13);
		java.util.regex.Matcher matcher = pattern.matcher(text);
		if (!matcher.matches()) {
			System.out.println("invalid!");
			return;
		}
		int count = matcher.groupCount();
		System.out.printf("create table count:%d\n", count);
		for (int i = 1; i <= count; i++) {
			String f = matcher.group(i);
			System.out.printf("[%s]\n", f);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Parrent p = new Parrent();
//		p.testCreateTable();
//		p.testCreateIndex();
//		p.testUnit();
//		p.testUnit2();
		p.testUnit3();
//		p.splitT();
	}

}

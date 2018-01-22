/**
 * 
 */
package com.lexst.debug;

import java.io.*;

import com.lexst.sql.conduct.*;
import com.lexst.sql.conduct.matrix.*;
import com.lexst.util.host.*;

/**
 * @author siven
 *
 */
public class SplitDCTable {

	/**
	 * 
	 */
	public SplitDCTable() {
		// TODO Auto-generated constructor stub
	}

	public void test() throws java.io.IOException {
		long jobid = 1000L;
		int timeout = 5000;

		SiteHost host1 = new SiteHost("192.168.1.100", 100, 100);
		SiteHost host2 = new SiteHost("192.168.1.200", 200, 200);
		SiteHost host3 = new SiteHost("192.198.1.200", 100, 100);

		NetDomain table1 = new NetDomain();
		table1.add(jobid, host1, timeout, new DiskField(2, 0, 999));
		table1.add(jobid, host2, timeout, new DiskField(5, 1000, 1999));
		table1.add(jobid, host3, timeout, new DiskField(7, 1000, 1999));
		
//		byte[] b = table1.build();
//		System.out.printf("element size:%d, build byte size:%d\n", table1.size(), b.length);
//		NetDomain nt = new NetDomain();
//		int size = nt.resolve(b, 0, b.length);
//		System.out.printf("resolve size:%d\n\n", size);

		jobid++;
		NetDomain table2 = new NetDomain();
		table2.add(jobid, host1, timeout, new DiskField(2, 0, 999));
		table2.add(jobid, host2, timeout, new DiskField(5, 1000, 1999));
		table2.add(jobid, host3, timeout, new DiskField(7, 1000, 1999));

		NetMatrix module = new NetMatrix();
		for (SiteHost host : table1.keySet()) {
			module.add(table1.get(host));
		}
		for (SiteHost host : table2.keySet()) {
			module.add(table2.get(host));
		}

		System.out.printf("element is:%d\n", module.size());
		System.out.printf("chunk byte size:%d\n\n", module.length());
		
		for (int mod : module.keySet()) {
			System.out.printf("mod:%d\n", mod);
		}

		NetDomain[] dts = module.balance(2);
		System.out.printf("\ndctable size:%d\n", dts.length);
		for (int i = 0; i < dts.length; i++) {
			System.out.printf("length:%d\n", dts[i].length());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SplitDCTable sd = new SplitDCTable();
		try {
			sd.test();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
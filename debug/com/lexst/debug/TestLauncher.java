/**
 * 
 */
package com.lexst.debug;

import java.io.*;

import com.lexst.data.*;
import com.lexst.thread.*;
import com.lexst.util.*;


public class TestLauncher extends JobLauncher {
	
	private static TestLauncher selfHandle = new TestLauncher();

	/**
	 * 
	 */
	private TestLauncher() {
		super();
	}
	
	public static TestLauncher getInstance() {
		return TestLauncher.selfHandle;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// TODO Auto-generated method stub
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// TODO Auto-generated method stub

	}
	
	public void test() throws IOException {
		String blockPath = "/store/block";
		String chunkPath = "/store/chunk";
		String buildPath = "/store/build";
		
		String headFile = "/home/liang/head.bin";
		String dataFile = "/home/liang/push.bin";
		
		File file = new File(headFile);
		if(!file.exists()) {
			System.out.printf("cannot find %s\n", headFile);
			return;
		}
		byte[] head = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(file);
		in.read(head);
		in.close();
		
		file = new File(dataFile);
		if(!file.exists()) {
			System.out.printf("cannot find %s\n", dataFile);
			return;
		}
		byte[] data = new byte[(int)file.length()];
		in = new FileInputStream(file);
		in.read(data);
		in.close();
		
		int ret = Install.initialize();
		System.out.printf("init install result %d\n", ret);
		
		Install.setCacheRoot(blockPath.getBytes());
		Install.setChunkRoot(chunkPath.getBytes());
		Install.setBuildRoot(buildPath.getBytes());
		
		long chunkId = Long.MIN_VALUE;
		for(int i = 0; i < 500; i++) {
			Install.addChunkId(chunkId++);
		}
				
		ret = Install.launch();
		System.out.printf("launch result is %d\n", ret);
		
		ret = Install.createSpace(head, true);
		System.out.printf("create space result %d\n", ret);

		int count = 100;
		int levels = 10000;
		
		for (int i = 0; i < count; i++) {
			for (int j = 0; j < levels; j++) {
//				ret = Install.insert(data);
//				if (ret < 0) {
//					System.out.printf("insert failed! [%d - %d]\n", i, j);
//				}
			}
		}
		
		System.out.println("insert finished! are you ready?");
		System.in.read();
		
		Install.stop();
		
		System.out.println("launcher finished!");
	}

	public void demoInsert() {
		byte[] data = new byte[1024];
		
		byte[] b = new byte[2222]; // Install.insert(data);
		
		int off = 0;
		int items = Numeric.toInteger(b, off, 4);
		off += 4;
		int state = b[off++];
		
		if (state == 0) { //success
			int db_size = b[off++];
			int table_size = b[off++];
			
			byte[] db = new byte[db_size];
			byte[] table = new byte[table_size];
			System.arraycopy(b, off, db, 0, db_size);
			off += db_size;
			System.arraycopy(b, off, table, 0, table_size);
			off += table_size;

			long blockId = Numeric.toLong(b, off, 8);
			// Distributer.getInstance().distCache(new String(db), new String(table), blockId);
		} else {
			// on failed, 
		}
	}
	
	public void demoDelete() {
		byte[] sql = new byte[1024];
		
		byte[] b = new byte[1024];
		
		int off = 0;
		int items = Numeric.toInteger(b, off, 4);
		off += 4;
		int state = b[off++];
		
		if(state == 0) {
			int db_size = b[off++];
			int table_size = b[off++];
			
			byte[] db = new byte[db_size];
			byte[] table = new byte[table_size];
			System.arraycopy(b, off, db, 0, db_size);
			off += db_size;
			System.arraycopy(b, off, table, 0, table_size);
			off += table_size;
			
			String db1 = new String(db);
			String table1 = new String(table);
			
			long blockId = Numeric.toLong(b, off, 8);
			off += 8;
			if(blockId != 0) {
				// Distributor.getInstance().distCache(db1, table1, blockId);
			}
			while(off < b.length) {
				long chunkId = Numeric.toLong(b, off, 8);
				off += 8;
				// Distributor.getInstance().distChunk(db1, table1, chunkId);
			}
		} else {
			// on failed
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TestLauncher.getInstance().test();
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

}

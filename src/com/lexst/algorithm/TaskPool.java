/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2010 lexst.com. All rights reserved
 * 
 * distribute task manager
 * 
 * @author scott.liang lexst@126.com
 * 
 * @version 1.0 10/19/2010
 * 
 * @see com.lexst.algorithm
 * 
 * @license GNU Lesser General Public License (LGPL)
 */
package com.lexst.algorithm;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.jar.*;

import org.w3c.dom.*;

import com.lexst.log.client.*;
import com.lexst.pool.*;
import com.lexst.util.naming.*;
import com.lexst.xml.*;

/**
 * JAR包热发布监视/管理器，定时检查指定目录下的JAR包状态。发生变化时更新内存记录。<br><br>
 * 
 * TASK 规则<br>
 * 1. jar包下必须有一个TASK-INF目录，这个目录下放一个tasks.xml，做为配置文件<br>
 * 2. "TASK-INF"必须全大写<br>
 * 3. "tasks.xml"必须全小写 <br>
 * 4. tasks.xml里面的配置规则见"task"标准(在BasicTask)<br>
 * 5. 与task中的类相关的类文件可以放在不同包中,但是必须保证能够被找到 <br>
 * 6. 如果在启动目录下定义"task"目录,运行时自动加载里面的配置文件 <br>
 */
public class TaskPool extends Pool {

	/** JAR任务目录(在jar包中) **/
	private final static String TAG = "TASK-INF/tasks.xml";

	/** JAR文件正则表达式 **/
	private final static String JAR_REGEX = "^\\s*(.+)(?i)(\\.JAR)\\s*$";

	/** Task事件监听句柄  */
	private TaskEventListener eventListener;
	
	/** 存储命名任务JAR包的根目录  */
	private File root;
	/** 其它命名任务JAR包的目录 **/
	private List<File> paths = new ArrayList<File>();

	/** 任务加载类  **/
	private TaskClassLoader loader = new TaskClassLoader();
	
	/** 任务命名 -> 项目类  **/
	private Map<Naming, Project> projects = new HashMap<Naming, Project>();
	
	/** 磁盘文件名  -> JAR档案实例 **/
	private Map<String, JarArchive> archives = new HashMap<String, JarArchive>();
	
	/**
	 * default constrctor
	 */
	protected TaskPool() {
		super();
		this.setSleep(60);
	}

	/**
	 * @return
	 */
	public ClassLoader getClassLoader() {
		return this.loader;
	}
	
	/**
	 * 设置命名任务通知接口
	 * @param s
	 */
	public void setTaskEventListener(TaskEventListener s) {
		this.eventListener = s;
	}

	/**
	 * 返回命名任务通知接口
	 * @return
	 */
	public TaskEventListener getTaskEventListener() {
		return this.eventListener;
	}
	
	/**
	 * 定义命名任务根目录，如果目录不存在不建立
	 * @param path
	 */
	public boolean setRoot(File dir) {
		boolean success = (dir.exists() && dir.isDirectory());
		if (success) {
			try {
				this.root = dir.getCanonicalFile();
			} catch (IOException e) {
				this.root = dir.getAbsoluteFile();
			}
		}
		return success;
	}
	
	/**
	 * 设置命名任务根目录
	 * @param path
	 * @return
	 */
	public boolean setRoot(String path) {
		return this.setRoot(new File(path));
	}

	/**
	 * 返回命名任务根目录
	 * @return
	 */
	public File getRoot() {
		return this.root;
	}
	
	/**
	 * 返回当前保存的全部任务命名
	 * @return
	 */
	public Set<Naming> getNamings() {
		Set<Naming> set = new TreeSet<Naming>();
		this.lockMulti();
		try {
			set.addAll(projects.keySet());
		} catch(Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockMulti();
		}
		return set;
	}
	
	/**
	 * 根据命名，查找对应的项目配置
	 * @param naming
	 * @return
	 */
	public Project findProject(Naming naming) {
		this.lockMulti();
		try {
			return projects.get(naming);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockMulti();
		}
		return null;
	}

	/**
	 * find a project
	 * @param naming
	 * @return
	 */
	public Project findProject(String naming) {
		return findProject(new Naming(naming));
	}

	/**
	 * find a class by name
	 * 
	 * @return
	 */
	public Class<?> findClass(String class_name) {
		this.lockMulti();
		try {
			return Class.forName(class_name, true, loader);
		} catch (ClassNotFoundException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockMulti();
		}
		return null;
	}
	
	/**
	 * get resource url
	 * @param name
	 * @return
	 */
	public URL findResource(String name) {
		this.lockMulti();
		try {
			return loader.findResource(name);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockMulti();
		}
		return null;
	}
	
	/**
	 * get resouce stream
	 * @param name
	 * @return
	 */
	public InputStream getResourceAsStream(String name) {
		this.lockMulti();
		try {
			return loader.getResourceAsStream(name);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockMulti();
		}
		return null;
	}
	
	/**
	 * 根据任务命名，返回任务实例(子类调用)
	 * 
	 * @param naming
	 * @return
	 */
	protected BasicTask findTask(Naming naming) {
		this.lockMulti();
		try {
			Project project = projects.get(naming);
			if (project != null) {
				String task_class = project.getTaskClass();
				BasicTask task = (BasicTask) Class.forName(task_class, true, loader).newInstance();
				task.setProject(project);
				return task;
			}
		} catch (InstantiationException exp) {
			Logger.error(exp);
		} catch (IllegalAccessException exp) {
			Logger.error(exp);
		} catch (ClassNotFoundException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockMulti();
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		if (root == null) {
			// "bin" 目录
			String bin = System.getProperty("user.dir");
			// 在bin目录下设置发布目录
			this.setRoot(new File(bin, "deploy"));
		}
		Logger.info("TaskPool.init, deploy root directory is '%s'", this.getRoot());

		// 加载任务命名
		this.update();
		
		return true;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("TaskPool.process, into...");
		
		while (!isInterrupted()) {
			// 检查JAR包更新
			boolean success = update();
			// 更新成功，通知命名事件监听器，更新任务命名
			if (eventListener != null && success) {
				eventListener.updateNaming();
			}
			// 线程进行等待状态
			sleep();
		}
		
		Logger.info("TaskPool.process, exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		paths.clear();
		projects.clear();
		archives.clear();
	}

	/**
	 * 取文件名
	 * @param file
	 * @return
	 */
	private String filename(File file) {
		try {
			return file.getCanonicalPath();
		} catch(IOException e) {
			return file.getAbsolutePath();
		}
	}
	
	/**
	 * 从其它目录加载JAR配置
	 * 
	 * @param path
	 * @return
	 */
	public List<String> load(String path) {		
		File dir = new File(path);
		if(!dir.isDirectory()) {
			return null;
		}
		if(paths.contains(dir)) return null;		
		
		File[] files = dir.listFiles();
		if (files == null || files.length == 0) return null;
		
		List<String> array = new ArrayList<String>();
		for(File file : files) {
			if(!file.isFile()) continue;
			String filename = filename(file);
			if(!filename.matches(TaskPool.JAR_REGEX)) continue;

			JarArchive ja1 = new JarArchive(filename, file.length(), file.lastModified());
			JarArchive ja2 = archives.get(filename);
			if(ja2 == null || !ja1.match(ja2)) array.add(filename);
		}
		
		this.paths.add(dir);
		this.wakeup();
		return array;
	}
	
	/**
	 * 从目录中提取JAR文件并且返回文件列表
	 * @return
	 */
	private List<JarArchive> listJarFile() {
		List<JarArchive> array = new ArrayList<JarArchive>();
		// 筛选全部发布目录
		ArrayList<File> dirs = new ArrayList<File>();
		if (this.root != null) {
			dirs.add(this.root);
		}
		dirs.addAll(paths);
		
		// 从目录中读JAR文件
		for(File dir : dirs) {
			File[] files = dir.listFiles();
			if (files == null || files.length == 0) continue;
			for(File file : files) {
				if(!file.isFile()) continue;
				String filename = filename(file);
				if(!filename.matches(TaskPool.JAR_REGEX)) continue;
				JarArchive ja = new JarArchive(filename, file.length(), file.lastModified());
				array.add(ja);
			}
		}
		
		return array;
	}

	/**
	 * 检查并且更新目录下的JAR包
	 * 
	 * @return
	 */
	private boolean update() {
		List<JarArchive> array = listJarFile();
		// 空记录
		if (array.isEmpty() && archives.isEmpty()) {
			return false;
		}
		
		boolean match = (array.size() == archives.size());
		if(match) {
			match = false;
			for(JarArchive archive : array) {
				JarArchive jar = archives.get(archive.getFilename());
				match = jar.match(archive);
				if(!match) break;
			}
		}
		if(match) return false;
		
		Logger.info("TaskPool.update, deploy task archive...");
		
		// show jar archive naming and class path
		Map<String, DataEntry> map = new HashMap<String, DataEntry>();
		archives.clear();
		for(JarArchive archive : array) {
			this.resolve(archive);
			map.putAll(archive.entrys());
			archives.put(archive.getFilename(), archive);
		}

		// 更新项目和命名
		this.lockSingle();
		try {
			// 释放全部旧项目
			this.projects.clear();
			
			// 重新初始化任务类加载器 
			this.loader = new TaskClassLoader(map);
			
			// 解析XML配置
			for (JarArchive archive : array) {
				byte[] b = archive.getTaskText();
				if (b == null) continue;
				XMLocal xml = new XMLocal();
				Document document = xml.loadXMLSource(b);
				if (document == null) continue;

				// 解析 task.xml
				NodeList list = document.getElementsByTagName("task");
				int size = list.getLength();
				for (int i = 0; i < size; i++) {
					Element elem = (Element) list.item(i);
					// 任务命名
					String name = xml.getValue(elem, "naming");
					// 任务类路径
					String task_class = xml.getValue(elem, "class");
					// 资源配置(任意字符串格式.具体由用户的Project子类解释)
					String resource = xml.getValue(elem, "resource");
					// 数据库表集合
					String spaces = xml.getValue(elem, "spaces");
					// 项目类路径(从Project.class派生)
					String project_class = xml.getValue(elem, "project-class");

					Naming naming = new Naming(name);
					archive.addNaming(naming);
					
					Class<?> clzz = Class.forName(project_class, true, this.loader);
					Project project = (Project) clzz.newInstance();

					project.setTaskNaming(naming);
					project.setTaskClass(task_class);
					project.setSpaces(spaces);
					project.setResource(resource);

					// 保存命名项目
					this.projects.put(naming, project);
				}
			}
			return true;
		} catch (ClassNotFoundException exp) {
			Logger.error(exp);
		} catch (IllegalAccessException exp) {
			Logger.error(exp);
		} catch (InstantiationException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			this.unlockSingle();
		}
		return false;
	}

	/**
	 * resolve task.xml and class file from jar file
	 * @param archive
	 */
	private void resolve(JarArchive archive) { 
		String filename = archive.getFilename();
		File file = new File(filename);
		final String sf = ".class";
		
		try {
			FileInputStream fi = new FileInputStream(file);
			JarInputStream in = new JarInputStream(fi);
			while (true) {
				JarEntry entry = in.getNextJarEntry();
				if (entry == null) break;

				String name = entry.getName();
				
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				byte[] b = new byte[1024];
				while(true) {
					int len = in.read(b, 0, b.length);
					if(len == -1) break;
					out.write(b, 0, len);
				}
				b = out.toByteArray();
				if(b == null || b.length == 0) continue;

				if (TaskPool.TAG.equals(name)) { // tag file (tasks.xml)
					archive.setTaskText(b);
				} else if (name.endsWith(sf)) { // class file
					name = name.substring(0, name.length() - sf.length());
					name = name.replace('/', '.');
					archive.addEntry(new DataEntry(name, b));
				} else { // resource file ( jar:<url>!/{entry} )
					String url = "jar:" + file.toURI().toURL().toExternalForm() + "!/" + name;
					archive.addEntry(new DataEntry(name, url, b));
				}
			}
			in.close();
			fi.close();
		} catch (IOException exp) {
			Logger.error(exp);
		}
	}

}
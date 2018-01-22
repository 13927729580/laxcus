/**
 *
 */
package com.lexst.xml;

import java.io.*;
import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * 本地XML文档解析器
 * 
 *
 */
public class XMLocal {

	/**
	 * default
	 */
	public XMLocal() {
		super();
	}

	/**
	 * 过滤空格
	 * @param s
	 * @return
	 */
	private String trim(String s) {
		if (s == null) return "";
		return s.trim();
	}
	
	/**
	 *
	 * @param parent
	 * @param tag
	 * @return
	 */
	public String getValue(Element parent, String tag) {
		NodeList list = parent.getElementsByTagName(tag);
		if(list == null) return "";
		Element elem = (Element) list.item(0);
		if(elem == null) return "";
		return trim(elem.getTextContent());
	}

	/**
	 * 返回XML配置表某组中第一项数据
	 *
	 * @param NodeList
	 * @return String
	 */
	public String getXMLValue(NodeList nodes) {
		if (nodes == null || nodes.getLength() < 1) return "";
		Element element = (Element) nodes.item(0);
		if (element == null) return "";
		return trim(element.getTextContent());
	}

	/**
	 * 返回XML配置表某组的全部数据
	 * @param nodes
	 * @return
	 */
	public String[] getXMLValues(NodeList nodes) {
		int size = nodes.getLength();
		String[] s = new String[size];

		for (int index = 0; index < size; index++) {
			Element element = (Element) nodes.item(index);
			if (element == null) continue;
			s[index] = trim(element.getTextContent());
		}
		return s;
	}
	
//	public String[] getXMLValues(NodeList nodes) {
//		int size = nodes.getLength();
//		ArrayList<String> a = new ArrayList<String>(size);
//		for (int i = 0; i < size; i++) {
//			Element element = (Element) nodes.item(i);
//			if (element == null) continue;
//			
//			
//			
//			System.out.printf("%s-%s-[%s]\n", element.getTagName(), element.getNodeValue(), element.getTextContent());
//			
//			NodeList c = element.getChildNodes();
//			if (c.getLength() < 1) continue;
//			
//			Node node = c.item(0);
//			System.out.printf("%s-%s\n", node.getNextSibling().getNodeValue(), node.getTextContent());
////			node.getNextSibling()
//			
//			String s = getString(node.getNodeValue());
//			a.add(s);
//		}
//
//		// if(a.isEmpty()) return null;
//		String[] s = new String[a.size()];
//		return a.toArray(s);
//	}


	
	/**
	 * 根据文件名,装载XML配置文件
	 * @param text
	 * @return
	 */
	public Document loadXMLSource(String filename) {
		File file = new File(filename);
		return loadXMLSource(file);
	}

	/**
	 * 加载XML文件
	 * @param file
	 * @return
	 */
	public Document loadXMLSource(File file) {
		if (!file.exists() || file.isDirectory()) {
			return null;
		}
		byte[] b = new byte[(int) file.length()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(b);
			in.close();
			return loadXMLSource(b, 0, b.length);
		} catch (IOException exp) {
			exp.printStackTrace();
		}
		return null;
	}

	/**
	 * 以字节流的形式加载XML文档并且返回Document对象
	 * 
	 * @param b
	 * @param off
	 * @param len
	 * @return
	 */
	public Document loadXMLSource(byte[] b, int off, int len) {
		try {
			ByteArrayInputStream bin = new ByteArrayInputStream(b, off, len);
			// 生成对象
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			return builder.parse(bin);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 以字节流的形式加载XML文档
	 * 
	 * @param b
	 * @return
	 */
	public Document loadXMLSource(byte[] b) {
		return loadXMLSource(b, 0, (b == null ? 0 : b.length));
	}

}
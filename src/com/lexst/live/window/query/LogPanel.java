/**
 * 
 */
package com.lexst.live.window.query;

import java.awt.*;
import java.awt.event.*;
import java.util.*;

import javax.swing.*;
import javax.swing.border.*;

import com.lexst.log.client.*;

public class LogPanel extends JPanel implements ActionListener,  LogPrinter {

	private static final long serialVersionUID = 1L;

	private JTextArea txtLog = new JTextArea();
	
	private int maxlog;
	private java.util.List<String> array = new ArrayList<String>();

	/**
	 * 
	 */
	public LogPanel() {
		super();
		setMaxLog(2000);
	}

	/**
	 * @param value
	 */
	public void setMaxLog(int value) {
		this.maxlog = value;
	}

	public int getMaxLog() {
		return this.maxlog;
	}
	
	public class LogKeyAdapter extends KeyAdapter {
		public void keyReleased(KeyEvent e) {
			int size = 0;
			if (e.getModifiersEx() == (KeyEvent.SHIFT_DOWN_MASK | KeyEvent.CTRL_DOWN_MASK)) {
				switch (e.getKeyChar()) {
				case '<':
					size = txtLog.getFont().getSize();
					size = (size > 12 ? size - 1 : 0);
					break;
				case '>':
					size = txtLog.getFont().getSize();
					size = (size < 26 ? size + 1 : 0);
					break;
				}
			} 
//			else if (e.getModifiersEx() == KeyEvent.CTRL_DOWN_MASK) {
//				switch (e.getKeyCode()) {
//				case '[':
//					size = txtLog.getFont().getSize();
//					size = (size > 10 ? size - 1 : 0);
//					break;
//				case ']':
//					size = txtLog.getFont().getSize();
//					size = (size < 26 ? size + 1 : 0);
//					break;
//				}
//			}
			if (size != 0) {
				Font font = txtLog.getFont();
				font = new Font(font.getName(), font.getStyle(), size);
				txtLog.setFont(font);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	@Override
	public void actionPerformed(ActionEvent arg) {
		
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.log.client.LogPrinter#print(java.lang.String)
	 */
	@Override
	public void print(String text) {
		if (array.size() >= maxlog) {
			array.remove(0);
			StringBuilder buffer = new StringBuilder();
			for (String s : array) {
				buffer.append(s);
			}
			txtLog.setText(buffer.toString());
		}
		
		array.add(text);
		txtLog.append(text);
	}

	public void init() {
		String html = "<html><body>Log View<br>SHIFT+CTRL+&lt; | SHIFT+CTRL+&gt;</body></html>";
		
		txtLog.setEditable(false);
		txtLog.setRows(3);
		txtLog.setToolTipText(html);
		txtLog.setForeground(Color.blue);
		txtLog.setBorder(new EmptyBorder(2, 2, 2, 2));
		
		setLayout(new BorderLayout());
		JScrollPane bottom = new JScrollPane(txtLog);
		add(bottom, BorderLayout.CENTER);
		
		txtLog.addKeyListener(new LogKeyAdapter());
	}
}
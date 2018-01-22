/**
 * 
 */
package com.lexst.live.window;

import java.awt.*;
import java.awt.event.*;

import javax.swing.*;
import javax.swing.border.*;

import com.lexst.util.res.*;


public class SQLHelp extends JDialog implements ActionListener {

	private static final long serialVersionUID = 1L;

	private JTextPane html = new JTextPane();

	private JButton cmdOK = new JButton();

	/**
	 * 
	 */
	public SQLHelp() {
		// TODO Auto-generated constructor stub
		super();
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public SQLHelp(Frame arg0, boolean arg1) {
		super(arg0, arg1);
	}

	/* (non-Javadoc)
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	@Override
	public void actionPerformed(ActionEvent arg) {
		if(arg.getSource() == cmdOK) {
			this.dispose();
		}
	}

	private JPanel initPane() {
		html.setEditable(false);
		html.setText("text/html");
		try {
			ResourceLoader loader = new ResourceLoader();
			java.net.URL url = loader.findURL("conf/terminal/html/window/help.html");
			if (url != null) html.setPage(url);
		} catch (Throwable exp) {
			
		}

		cmdOK.setText("OK");
		cmdOK.addActionListener(this);
		cmdOK.setMnemonic('o');

		JScrollPane p2 = new JScrollPane(html);
		p2.setBorder(new EmptyBorder(5, 5, 3, 5));

		JPanel p3 = new JPanel();
		p3.setLayout(new java.awt.FlowLayout());
		p3.add(cmdOK);
		
		JPanel p5 = new JPanel();
		p5.setLayout(new BorderLayout());
		p5.add(new JPanel(), BorderLayout.EAST);
		p5.add(p3, BorderLayout.CENTER);
		p5.add(new JPanel(), BorderLayout.WEST);
		
		JPanel pane = new JPanel();
		pane.setLayout(new BorderLayout());
		pane.add(p2, BorderLayout.CENTER);
		pane.add(p5, BorderLayout.SOUTH);
		return pane;
	}

	private String findUIClass(String clsname) {
		UIManager.LookAndFeelInfo[] total = UIManager.getInstalledLookAndFeels();
		for (int i = 0; total != null && i < total.length; i++) {
			final String class_name = total[i].getClassName();

			int index = class_name.lastIndexOf('.');
			if (index == -1) continue;
			String suffix = class_name.substring(index + 1);
			index = suffix.indexOf("LookAndFeel");
			if (index > -1) {
				suffix = suffix.substring(0, index);
				if(suffix.equalsIgnoreCase(clsname)) {
					return class_name;
				}
			}
		}
		return null;
	}
	
	private void updateUI(String className) {
		if (className == null) return;
	    try {
	    	UIManager.setLookAndFeel(className);
	    	SwingUtilities.updateComponentTreeUI(getContentPane());
		} catch (UnsupportedLookAndFeelException exp) {
			//exp.printStackTrace();
		} catch (Exception exp) {
			//exp.printStackTrace();
		}
	}
	
	public void showDialog() {
		JPanel pane = initPane();
		
		Container canvas = this.getContentPane();
		canvas.setLayout(new BorderLayout(0, 0));
		canvas.add(pane, BorderLayout.CENTER);

		// set window bound
		Dimension size = Toolkit.getDefaultToolkit().getScreenSize();
		int width = (int)(size.getWidth() * 0.39);
		int height = (int)(size.getHeight() * 0.36);
		int x = (size.width - width)/2;
		int y = (size.height - height)/2;
		this.setBounds(new Rectangle(x, y, width, height));
		this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
		// set Nimbus UI
		String clsname = findUIClass("Nimbus");
		if(clsname != null) {
			this.updateUI(clsname);
		}
		this.setTitle("Help");
		
		this.setMinimumSize(new Dimension(380, 180));
		this.setAlwaysOnTop(true);
		this.setVisible(true);
	}
}
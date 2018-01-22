/**
 * 
 */
package com.lexst.live.window.query;

import java.awt.*;
import java.io.*;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.table.*;

import com.lexst.algorithm.collect.*;
import com.lexst.log.client.*;
import com.lexst.live.LiveUtil;
import com.lexst.sql.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.row.*;
import com.lexst.sql.schema.*;

public class TabPanel extends JPanel implements ChangeListener, PrintTerminal {

	private static final long serialVersionUID = 1L;
	
	private JTabbedPane tabPanel = new JTabbedPane();
	
	private JTextArea txtTip = new JTextArea();
	
	private StructModel model = new StructModel();
	private JTable table = new JTable();

	private LogPanel log = new LogPanel();
	
	/**
	 * 
	 */
	public TabPanel() {
		super();
	}

	/* (non-Javadoc)
	 * @see javax.swing.event.ChangeListener#stateChanged(javax.swing.event.ChangeEvent)
	 */
	@Override
	public void stateChanged(ChangeEvent arg) {
		
	}
	
	public LogPrinter getLogPrinter() {
		return log;
	}
	
	private JScrollPane initTip() {
		Font font = txtTip.getFont();
		font = new Font(font.getName(), font.getStyle(), font.getSize() + 4);
		txtTip.setFont(font);
		
		txtTip.setEditable(false);
		txtTip.setToolTipText("SQL Execute Report");
		txtTip.setBorder(new EmptyBorder(2, 2, 2, 2));
		
		return new JScrollPane(txtTip);
	}

	private JScrollPane initSelect() {
		table.setModel(model);
		table.setRowHeight(23);
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		table.setRowSelectionAllowed(true);
		table.setShowGrid(true);
		table.getTableHeader().setReorderingAllowed(false);
		table.setSelectionMode(DefaultListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		table.setIntercellSpacing(new Dimension(3, 3));
		table.setColumnSelectionAllowed(true);
		table.setSurrendersFocusOnKeystroke(true);

		TableColumnModel columnModel = table.getColumnModel();
		int count = columnModel.getColumnCount();
		for (int n = 0; n < count; n++) {
			columnModel.getColumn(n).setPreferredWidth(120);
		}

		String title = "SQL Query Result";
		table.setToolTipText(title);
		table.setBorder(new EmptyBorder(2, 2, 2, 2));
		
		return new JScrollPane(table);
	}
	
	public void init() {
		JScrollPane tip = this.initTip();
		JScrollPane tab = this.initSelect();
		log.init();

		tabPanel.addChangeListener(this);
		tabPanel.addTab("Report", tip);
		tabPanel.addTab("Query", tab);
		tabPanel.addTab("Logs", log);
		tabPanel.setBorder(new EmptyBorder(1, 1, 1, 1));
		
		this.setLayout(new BorderLayout(5, 5));
		this.add(tabPanel, BorderLayout.CENTER);
	}



	public void showFault(String format, Object... args) {
		String s = String.format(format, args);
		showFault(s);
	}



	public void showMessage(String format, Object... args) {
		String s = String.format(format, args);
		showMessage(s);
	}
	
	public void deleteTip() {
		if (!txtTip.getText().isEmpty()) {
			txtTip.setText("");
		}
	}

	public void clearItems() {
		model.clear();
	}

	/**
	 * 清除表格的全部信息
	 */
	public void clearTable() {
		this.clearItems();
		TableColumnModel m = table.getColumnModel();
		int count = m.getColumnCount();
		for (int i = 0; i < count; i++) {
			m.removeColumn(m.getColumn(0));
		}
	}

	public void updateTable(String[] titles) {
		// clear table
		clearTable();
		// reload column
		for (int index = 0; index < titles.length; index++) {
			TableColumn col = new TableColumn(index);
			col.setHeaderValue(titles[index]);
			table.addColumn(col);
		}
	}
	
	/**
	 * 更新表格标题
	 * 
	 * @param sheet
	 */
	public int updateTitle(Sheet sheet) {
		this.clearTable();
		int index = 0;
		for(ColumnAttribute attribute :	sheet.values()) {
			int width = 30;
			switch (attribute.getType()) {
			case Type.RAW:
			case Type.CHAR:
			case Type.SCHAR:
			case Type.WCHAR:
			case Type.TIMESTAMP:
				width = 130;
				break;
			case Type.SHORT:
			case Type.INTEGER:
			case Type.LONG:
				width = 85;
				break;
			case Type.FLOAT:
			case Type.DOUBLE:
			case Type.DATE:
			case Type.TIME:
				width = 100;
				break;
			}
			TableColumn column = new TableColumn(index);
			column.setHeaderValue(attribute.getName());
			table.addColumn(column);
			table.getColumnModel().getColumn(index).setPreferredWidth(width);
			index++;
		}
		return index;
	}

	/**
	 * 更新表标题
	 * 
	 * @param table
	 */
	public void updateTable(Table table) {
		this.updateTitle(table.getSheet());
	}
	

	public void focusItem() {
		tabPanel.setSelectedIndex(1);
	}

	public void focusMessage() {
		tabPanel.setSelectedIndex(0);
	}
	
	/**
	 * 根据顺序表显示一行记录
	 * @param sheet
	 * @param row
	 * @return
	 */
	public int addItem(Sheet sheet, Row row) {
		int select = tabPanel.getSelectedIndex();
		if (select != 1) tabPanel.setSelectedIndex(1);
		
		String[] s = LiveUtil.showRow(sheet, row);		
		model.addRow(s);
		return s.length;
	}
	
	/**
	 * 根据数据表的列属性显示一条记录
	 * @param table
	 * @param row
	 * @return
	 */
	public int addItem(Table table, Row row) {
		return addItem(table.getSheet(), row);
	}
	
	/*
	 * 显示错误信息
	 * @see com.lexst.algorithm.collect.PrintTerminal#showFault(java.lang.String)
	 */
	@Override
	public void showFault(String s) {
//		Color red = txtTip.getForeground();
//		if (!red.equals(Color.red)) {
//			txtTip.setText("");
//		}
		txtTip.setForeground(Color.red);
		txtTip.setText(s);
		tabPanel.setSelectedIndex(0);
	}

	/*
	 * 显示堆栈错误信息
	 * @see com.lexst.algorithm.collect.PrintTerminal#showFault(java.lang.Throwable)
	 */
	@Override
	public void showFault(Throwable t) {
		ByteArrayOutputStream buff = new ByteArrayOutputStream(1024);
		PrintStream s = new PrintStream(buff, true);
		t.printStackTrace(s);
		byte[] data = buff.toByteArray();
		showFault(new String(data, 0, data.length));
	}
	
	/*
	 * 显示标准信息
	 * @see com.lexst.algorithm.collect.PrintTerminal#showMessage(java.lang.String)
	 */
	@Override
	public void showMessage(String s) {
		txtTip.setForeground(new Color(0, 128, 64));
		txtTip.setText(s);
		tabPanel.setSelectedIndex(0);
	}
	
	/*
	 * 显示表格标题
	 * @see com.lexst.algorithm.collect.PrintTerminal#showTitle(com.lexst.sql.schema.Sheet)
	 */
	@Override
	public int showTitle(Sheet sheet) {
		return this.updateTitle(sheet) ;
	}

	/*
	 * 表格中增加一行记录
	 * @see com.lexst.algorithm.collect.PrintTerminal#showRow(com.lexst.sql.schema.Sheet, com.lexst.sql.row.Row)
	 */
	@Override
	public int showRow(Sheet sheet, Row row) {
		return this.addItem(sheet, row);
	}
}
/**
 *
 */
package com.lexst.live.window.query;

import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.List;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;

import com.lexst.sql.*;
import com.lexst.sql.column.attribute.*;
import com.lexst.sql.schema.*;
import com.lexst.util.res.*;


public class LeftPanel extends JPanel implements ActionListener, TreeSelectionListener {

	private static final long serialVersionUID = 1L;

	private DefaultMutableTreeNode root = new DefaultMutableTreeNode("Schema", true);
	private DefaultTreeModel treeModel;
	private JTree tree;

	/**
	 *
	 */
	public LeftPanel() {
		super();
	}

	/* (non-Javadoc)
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	@Override
	public void actionPerformed(ActionEvent e) {

	}

	/* (non-Javadoc)
	 * @see javax.swing.event.TreeSelectionListener#valueChanged(javax.swing.event.TreeSelectionEvent)
	 */
	public void valueChanged(TreeSelectionEvent e) {

	}

	public void init() {
		String[] names = {"db_16.gif", "table_16.png","primary_16.png", "index_16.png", "column_16.png" };
		Icon[] icons = new ImageIcon[names.length];
		ResourceLoader loader = new ResourceLoader("conf/terminal/image/window/query/");
		for (int i = 0; i < names.length; i++) {
			icons[i] = loader.findImage(names[i]);
		}
		
		RankTreeCellRenderer ren = new RankTreeCellRenderer(icons[0], icons[1],
				icons[2], icons[3], icons[4]);

		treeModel = new DefaultTreeModel(root);
		tree = new JTree(treeModel);
		tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
	 	tree.setBorder(new EmptyBorder(5, 3, 5, 3));
		tree.setRowHeight(-1);
		tree.setToggleClickCount(1);
		tree.setShowsRootHandles(true);
		tree.setRootVisible(false);
		tree.addTreeSelectionListener(this);
		tree.setEditable(false);
		tree.setCellRenderer(ren);

		JScrollPane sp1 = new JScrollPane(tree);
		setLayout(new BorderLayout(0, 10));
		add(sp1, BorderLayout.CENTER);
	}

	/**
	 * remove all information
	 */
	public void clear() {
		int count = root.getChildCount();
		for (int index = 0; index < count; index++) {
			root.remove(0);
		}
		treeModel.reload();
	}

	/**
	 * review table configure
	 * @param schemas
	 * @param tables
	 */
	public void review(List<String> schemas, List<Table> tables) {
		this.clear();
		if (schemas == null || schemas.isEmpty()) {
			return;
		}
		if (tables == null) {
			tables = new ArrayList<Table>();
		}

		Collections.sort(schemas);
		for(String db : schemas) {
			SelectMutableTreeNode parent = new SelectMutableTreeNode(db, true, SelectMutableTreeNode.SCHEMA);
			root.add(parent);
			for(Table tb : tables) {
				if(!db.equalsIgnoreCase(tb.getSpace().getSchema())) {
					continue;
				}
				String tablename = tb.getSpace().getTable();
				SelectMutableTreeNode child = new SelectMutableTreeNode(tablename, true, SelectMutableTreeNode.TABLE);
				// 找到后,显示列
				for (short columnId : tb.idSet()) {
					ColumnAttribute field = tb.find(columnId);
					String fieldName = field.getName();
					String type = Type.showDataType(field.getType());
					fieldName = String.format("%s  (%s)", fieldName, type); // Type.translate(field.getType()));
					if (Type.isVariable(field.getType())) {
						int indexSize = ((VariableAttribute) field).getIndexSize();
						if (indexSize > 0) {
							fieldName = String.format("%s - %d", fieldName, indexSize);
						}
					}
					SelectMutableTreeNode sub = null;
					switch (field.getKey()) {
					case Type.PRIME_KEY:
						sub = new SelectMutableTreeNode(fieldName, false,
								SelectMutableTreeNode.PRIME_FIELD);
						break;
					case Type.SLAVE_KEY:
						sub = new SelectMutableTreeNode(fieldName, false,
								SelectMutableTreeNode.INDEX_FIELD);
						break;
					default:
						sub = new SelectMutableTreeNode(fieldName, false,
								SelectMutableTreeNode.FIELD);
					}
					sub.setToolTip(type);
					int count = child.getChildCount();
					treeModel.insertNodeInto(sub, child, count);
				}
				int count = parent.getChildCount();
				treeModel.insertNodeInto(child, parent, count);
			}
		}
		// review tree
		treeModel.reload();
	}


}
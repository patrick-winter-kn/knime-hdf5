package org.knime.hdf5.nodes.writer.edit;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class ColumnNodeEdit extends TreeNodeEdit {

	public static final ColumnNodeMenu COLUMN_EDIT_MENU = new ColumnNodeMenu(true);
	
	public static final ColumnNodeMenu COLUMN_MENU = new ColumnNodeMenu(false);
	
	private final DataColumnSpec m_columnSpec;

	public ColumnNodeEdit(DataColumnSpec columnSpec) throws UnsupportedDataTypeException {
		super(columnSpec.getName());
		m_columnSpec = columnSpec;
	}

	DataColumnSpec getColumnSpec() {
		return m_columnSpec;
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		settings.addDataType("columnSpec", m_columnSpec.getType());
	}

	public static ColumnNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		ColumnNodeEdit edit = null;
		
		try {
			edit = new ColumnNodeEdit(new DataColumnSpecCreator(settings.getString("name"), settings.getDataType("columnSpec")).createSpec());
		
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
		
		return edit;
	}
	
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		parentNode.add(new DefaultMutableTreeNode(this));
	}
	
	public static class ColumnNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = 7696321716083384515L;

    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private ColumnNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
        		JMenuItem itemDelete = new JMenuItem("Delete column");
        		itemDelete.addActionListener(new ActionListener() {
    				
    				@Override
    				public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof ColumnNodeEdit) {
							ColumnNodeEdit edit = (ColumnNodeEdit) userObject;
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
	                    	DataSetNodeEdit parentEdit = (DataSetNodeEdit) parent.getUserObject();
	                    	parentEdit.removeColumnNodeEdit(edit);
	                    	parent.remove(m_node);
	        				((DefaultTreeModel) (m_tree.getModel())).reload();
	        				m_tree.makeVisible(new TreePath(parent.getPath()));
	                    	if (parent.getChildCount() == 0) {
		                    	DefaultMutableTreeNode grandParent = (DefaultMutableTreeNode) parent.getParent();
								Object grandParentObject = grandParent.getUserObject();
		                    	if (grandParentObject instanceof GroupNodeEdit) {
			                    	((GroupNodeEdit) grandParentObject).removeDataSetNodeEdit(parentEdit);
		                    	} else {
		                    		m_editTreeConfig.removeDataSetNodeEdit(parentEdit);
		                    	}
		                    	grandParent.remove(parent);
		        				((DefaultTreeModel) (m_tree.getModel())).reload();
		        				m_tree.makeVisible(new TreePath(grandParent.getPath()));
	                    	}
						}
    				}
    			});
        		add(itemDelete);
    		}
    	}
		
		public void initMenu(JTree tree, EditTreeConfiguration editTreeConfig, DefaultMutableTreeNode node) {
			m_tree = tree;
			m_editTreeConfig = editTreeConfig;
			m_node = node;
		}
	}
}

package org.knime.hdf5.nodes.writer.edit;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Map;

import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;

public class ColumnNodeEdit extends TreeNodeEdit {
	
	private final ColumnNodeMenu m_columnNodeMenu;
	
	private final DataColumnSpec m_columnSpec;

	public ColumnNodeEdit(ColumnNodeEdit copyColumn, DataSetNodeEdit parent) {
		this(copyColumn.getColumnSpec(), parent, false);
		setEditAction(copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
	}
	
	public ColumnNodeEdit(DataColumnSpec columnSpec, DataSetNodeEdit parent, boolean isCreate) {
		this(columnSpec.getName(), columnSpec, parent);
		setEditAction(isCreate ? EditAction.CREATE : EditAction.NO_ACTION);
	}
	
	ColumnNodeEdit(String inputPathFromFileWithName, DataColumnSpec columnSpec, DataSetNodeEdit parent) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), columnSpec.getName());
		m_columnNodeMenu = new ColumnNodeMenu();
		m_columnSpec = columnSpec;
		parent.addColumnNodeEdit(this);
	}
	
	public ColumnNodeMenu getColumnEditMenu() {
		return m_columnNodeMenu;
	}

	DataColumnSpec getColumnSpec() {
		return m_columnSpec;
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		settings.addDataType(SettingsKey.COLUMN_SPEC_TYPE.getKey(), m_columnSpec.getType());
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		parentNode.add(m_treeNode);
	}
	
	@Override
	protected boolean getValidation() {
		return true;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return false;
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		return false;
	}
	
	@Override
	protected boolean copyAction() {
		return false;
	}

	@Override
	protected boolean deleteAction() {
		return false;
	}

	@Override
	protected boolean modifyAction() {
		return false;
	}
	
	public class ColumnNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = 7696321716083384515L;

    	private JTree m_tree;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private ColumnNodeMenu() {
    		JMenuItem itemDelete = new JMenuItem("Delete");
    		itemDelete.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					Object userObject = m_node.getUserObject();
					if (userObject instanceof ColumnNodeEdit) {
						ColumnNodeEdit edit = (ColumnNodeEdit) userObject;
                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
                    	DataSetNodeEdit parentEdit = (DataSetNodeEdit) parent.getUserObject();
						if (edit.getEditAction().isCreateOrCopyAction()) {
							parentEdit.removeColumnNodeEdit(edit);
                    	} else {
                    		edit.setEditAction(EditAction.DELETE);
						}
						
                    	if (parent.getChildCount() == 0) {
	                    	DefaultMutableTreeNode grandParent = (DefaultMutableTreeNode) parent.getParent();
							if (parentEdit.getEditAction().isCreateOrCopyAction()) {
		                    	((GroupNodeEdit) grandParent.getUserObject()).removeDataSetNodeEdit(parentEdit);
	                    	} else {
	                    		parentEdit.setEditAction(EditAction.DELETE);
	                    	}
                    	}
                    	
        				((DefaultTreeModel) (m_tree.getModel())).reload();
        				TreePath path = new TreePath(parent.getPath());
        				path = parent.getChildCount() > 0 ? path.pathByAddingChild(parent.getFirstChild()) : path;
        				m_tree.makeVisible(path);
					}
				}
			});
    		add(itemDelete);
    	}
		
		public void initMenu(JTree tree, DefaultMutableTreeNode node) {
			m_tree = tree;
			m_node = node;
		}
	}
}

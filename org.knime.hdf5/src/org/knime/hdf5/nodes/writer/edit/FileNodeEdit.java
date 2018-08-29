package org.knime.hdf5.nodes.writer.edit;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5File;

public class FileNodeEdit extends GroupNodeEdit {
	
	private final String m_filePath;
	
	private JTree m_tree;
	
	public FileNodeEdit(Hdf5File file) {
		this(file.getFilePath(), EditAction.NO_ACTION);
		setHdfObject(file);
	}
	
	public FileNodeEdit(String filePath) {
		this(filePath, EditAction.CREATE);
	}
	
	private FileNodeEdit(String filePath, EditAction editAction) {
		super(null, null, filePath.substring(filePath.lastIndexOf(File.separator) + 1), editAction);
		m_filePath = filePath;
	}
	
	public String getFilePath() {
		return m_filePath;
	}
	
	GroupNodeEdit getGroupEditByPath(String inputPathFromFileWithName) {
		if (!inputPathFromFileWithName.isEmpty()) {
			int pathLength = inputPathFromFileWithName.lastIndexOf("/");
			GroupNodeEdit parent = pathLength >= 0 ? getGroupEditByPath(inputPathFromFileWithName.substring(0, pathLength)) : this;
			
			return parent != null ? parent.getGroupNodeEdit(inputPathFromFileWithName) : null;
			
		} else {
			return this;
		}
	}

	DataSetNodeEdit getDataSetEditByPath(String inputPathFromFileWithName) {
		int pathLength = inputPathFromFileWithName.lastIndexOf("/");
		GroupNodeEdit parent = pathLength >= 0 ? getGroupEditByPath(inputPathFromFileWithName.substring(0, pathLength)) : this;
		
		return parent != null ? parent.getDataSetNodeEdit(inputPathFromFileWithName) : null;
	}

	ColumnNodeEdit getColumnEditByPath(String inputPathFromFileWithName, int inputColumnIndex) {
		DataSetNodeEdit parent = getDataSetEditByPath(inputPathFromFileWithName);
		
		return parent != null ? parent.getColumnNodeEdit(inputPathFromFileWithName, inputColumnIndex) : null;
	}

	AttributeNodeEdit getAttributeEditByPath(String inputPathFromFileWithName) {
		AttributeNodeEdit attributeEdit = null;
		
		String[] pathAndName = Hdf5Attribute.getPathAndName(inputPathFromFileWithName);
		GroupNodeEdit parentGroup = getGroupEditByPath(pathAndName[0]);
		
		if (parentGroup != null) {
			attributeEdit = parentGroup.getAttributeNodeEdit(inputPathFromFileWithName, EditAction.NO_ACTION);
			
		} else {
			DataSetNodeEdit parentDataSet = getDataSetEditByPath(pathAndName[0]);
			attributeEdit = parentDataSet != null ? parentDataSet.getAttributeNodeEdit(inputPathFromFileWithName, EditAction.NO_ACTION) : null;
		}
		
		return attributeEdit;
	}
	
	public static FileNodeEdit useFileSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		Hdf5File file = null;
		FileNodeEdit edit = new FileNodeEdit(settings.getString(SettingsKey.FILE_PATH.getKey()),
				EditAction.get(settings.getString(SettingsKey.EDIT_ACTION.getKey())));
		try {
	        if (!edit.getEditAction().isCreateOrCopyAction()) {
				try {
					file = Hdf5File.openFile(edit.getFilePath(), Hdf5File.READ_ONLY_ACCESS);
				} catch (IOException ioe) {
					throw new InvalidSettingsException(ioe.getMessage(), ioe);
				}
			}
			edit.setHdfObject(file);
	        edit.loadSettings(settings);
	        edit.updateIncompleteCopies();
	        edit.validate();
	        
		} finally {
			if (file != null) {
				file.close();
			}
		}
		
        return edit;
	}
	
	@Override
	public void integrate(GroupNodeEdit copyEdit, long inputRowCount) {
		if (copyEdit instanceof FileNodeEdit) {
			super.integrate(copyEdit, inputRowCount);
			validate();
		} else {
			throw new IllegalStateException("Cannot integrate group (which is not a file) in a file");
		}
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
        settings.addString(SettingsKey.FILE_PATH.getKey(), m_filePath);
	}
	
	@Override
	public void loadChildrenOfHdfObject() throws IOException {
		super.loadChildrenOfHdfObject();
		validate();
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		// not needed here; instead use setEditAsRootOfTree[JTree]
	}
	
	public void setEditAsRootOfTree(JTree tree) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		((DefaultTreeModel) tree.getModel()).setRoot(m_treeNode);
		m_tree = tree;
		
		reloadTree();
	}

	public void reloadTree() {
		validate();
		((DefaultTreeModel) (m_tree.getModel())).reload();
	}
	
	public void makeTreeNodeVisible(TreeNodeEdit edit) {
		if (edit.getTreeNode().isNodeAncestor(getTreeNode())) {
			m_tree.makeVisible(new TreePath(edit.getTreeNode().getPath()));
		} else if (edit.getParent().getTreeNode().getChildCount() != 0) {
			m_tree.makeVisible(new TreePath(edit.getParent().getTreeNode().getPath())
					.pathByAddingChild(edit.getParent().getTreeNode().getFirstChild()));
		} else {
			makeTreeNodeVisible(edit.getParent());
		}
	}
	
	@Override
	protected InvalidCause validateEditInternal() {
		return m_filePath.endsWith(".h5") || m_filePath.endsWith(".hdf5") ? null : InvalidCause.FILE_EXTENSION;
	}
	
	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof FileNodeEdit && !edit.equals(this) && ((FileNodeEdit) edit).getFilePath().equals(getFilePath())
				&& edit.getName().equals(getName());
	}
	
	@Override
	public boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		try {
			boolean success = true;
			if (!getEditAction().isCreateOrCopyAction()) {
				setHdfObject(Hdf5File.openFile(getFilePath(), Hdf5File.READ_WRITE_ACCESS));
				success = getHdfObject() != null;
			}
			return success && super.doAction(inputTable, flowVariables, saveColumnProperties);
			
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		return false;
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		try {
			setHdfObject(Hdf5File.createFile(getFilePath()));
			return getHdfObject() != null;
			
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
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
}

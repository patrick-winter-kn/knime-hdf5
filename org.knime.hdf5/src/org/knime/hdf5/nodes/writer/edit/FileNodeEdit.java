package org.knime.hdf5.nodes.writer.edit;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;

public class FileNodeEdit extends GroupNodeEdit {
	
	private final String m_filePath;
	
	public FileNodeEdit(Hdf5File file) {
		this(file.getFilePath());
		m_editAction = EditAction.NO_ACTION;
		setHdfObject(file);
	}
	
	public FileNodeEdit(String filePath) {
		super(null, filePath.substring(filePath.lastIndexOf(File.separator) + 1));
		m_editAction = EditAction.CREATE;
		m_filePath = filePath;
	}
	
	public String getFilePath() {
		return m_filePath;
	}
	
	public static FileNodeEdit useFileSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		FileNodeEdit edit = new FileNodeEdit(settings.getString(SettingsKey.FILE_PATH.getKey()));
        edit.loadSettings(settings);
        return edit;
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
        settings.addString(SettingsKey.FILE_PATH.getKey(), m_filePath);
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		/* nothing to do here, setEditAsRoot() is used instead */
	}
	
	public void setEditAsRoot(DefaultTreeModel treeModel) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		treeModel.setRoot(node);
		m_treeNode = node;
		/*
		for (GroupNodeEdit edit : getGroupNodeEdits()) {
	        edit.addEditToNode(node);
		}
		
		for (DataSetNodeEdit edit : getDataSetNodeEdits()) {
	        edit.addEditToNode(node);
		}
		
		for (AttributeNodeEdit edit : getAttributeNodeEdits()) {
	        edit.addEditToNode(node);
		}*/
	}
	
	@Override
	protected boolean getValidation() {
		// TODO check if file can be created correctly with its properties
		return m_filePath.endsWith(".h5") || m_filePath.endsWith(".hdf5");
	}
	
	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof FileNodeEdit && !edit.equals(this) && ((FileNodeEdit) edit).getFilePath().equals(getFilePath()) && edit.getName().equals(getName());
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

	@Override
	public boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		try {
			setHdfObject(Hdf5File.openFile(getFilePath(), Hdf5File.READ_WRITE_ACCESS));
			return getHdfObject() != null && super.doAction(inputTable, flowVariables, saveColumnProperties);
			
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		return false;
	}
}

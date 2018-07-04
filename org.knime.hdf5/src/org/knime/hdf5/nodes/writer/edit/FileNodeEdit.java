package org.knime.hdf5.nodes.writer.edit;

import java.io.File;
import java.util.Enumeration;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

public class FileNodeEdit extends GroupNodeEdit {
	
	private String m_filePath;
	
	public FileNodeEdit(String filePath) {
		super(filePath.substring(filePath.lastIndexOf(File.separator) + 1));
		m_filePath = filePath;
	}
	
	public String getFilePath() {
		return m_filePath;
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
        settings.addString(SettingsKey.FILE_PATH.getKey(), m_filePath);
	}
	
	@SuppressWarnings("unchecked")
	public static FileNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		FileNodeEdit edit = new FileNodeEdit(settings.getString(SettingsKey.FILE_PATH.getKey()));
		
        NodeSettingsRO groupSettings = settings.getNodeSettings(SettingsKey.GROUPS.getKey());
        Enumeration<NodeSettingsRO> groupEnum = groupSettings.children();
        while (groupEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = groupEnum.nextElement();
        	edit.addGroupNodeEdit(GroupNodeEdit.getEditFromSettings(editSettings));
        }
        
        NodeSettingsRO dataSetSettings = settings.getNodeSettings(SettingsKey.DATA_SETS.getKey());
        Enumeration<NodeSettingsRO> dataSetEnum = dataSetSettings.children();
        while (dataSetEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = dataSetEnum.nextElement();
        	edit.addDataSetNodeEdit(DataSetNodeEdit.getEditFromSettings(editSettings));
        }
        
        NodeSettingsRO attributeSettings = settings.getNodeSettings(SettingsKey.ATTRIBUTES.getKey());
        Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
        while (attributeEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = attributeEnum.nextElement();
        	edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		return edit;
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		
	}
	
	public void setEditAsRoot(DefaultTreeModel treeModel) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		treeModel.setRoot(node);
		m_treeNode = node;
		
		for (GroupNodeEdit edit : getGroupNodeEdits()) {
	        edit.addEditToNode(node);
		}
		
		for (DataSetNodeEdit edit : getDataSetNodeEdits()) {
	        edit.addEditToNode(node);
		}
		
		for (AttributeNodeEdit edit : getAttributeNodeEdits()) {
	        edit.addEditToNode(node);
		}
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
}

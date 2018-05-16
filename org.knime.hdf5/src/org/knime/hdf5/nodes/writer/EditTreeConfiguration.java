package org.knime.hdf5.nodes.writer;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.nodes.writer.edit.AttributeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.DataSetNodeEdit;
import org.knime.hdf5.nodes.writer.edit.GroupNodeEdit;

public class EditTreeConfiguration {
	
	private final String m_configRootName;

	private final List<GroupNodeEdit> m_groupEdits = new ArrayList<>();
	
	private final List<DataSetNodeEdit> m_dataSetEdits = new ArrayList<>();
	
	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	EditTreeConfiguration(String configRootName) {
		m_configRootName = configRootName;
	}
	
	public GroupNodeEdit[] getGroupNodeEdits() {
		return m_groupEdits.toArray(new GroupNodeEdit[] {});
	}
	
	public DataSetNodeEdit[] getDataSetNodeEdits() {
		return m_dataSetEdits.toArray(new DataSetNodeEdit[] {});
	}
	
	public AttributeNodeEdit[] getAttributeNodeEdits() {
		return m_attributeEdits.toArray(new AttributeNodeEdit[] {});
	}

	public void addGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.add(edit);
	}
	
	public void addDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.add(edit);
	}
	
	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
	}
	
	public void removeGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.remove(edit);
	}
	
	public void removeDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.remove(edit);
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
	}
	
	void saveConfiguration(NodeSettingsWO settings) {
        NodeSettingsWO groupSettings = settings.addNodeSettings(m_configRootName + "_Groups");
        NodeSettingsWO dataSetSettings = settings.addNodeSettings(m_configRootName + "_DataSets");
        NodeSettingsWO attributeSettings = settings.addNodeSettings(m_configRootName + "_Attributes");
        
        for (GroupNodeEdit edit : m_groupEdits) {
	        NodeSettingsWO editSettings = groupSettings.addNodeSettings(edit.getPathFromFile() + edit.getName() + "/");
	        editSettings.addString("pathFromFile", edit.getPathFromFile());
			edit.saveSettings(editSettings);
		}
		
		for (DataSetNodeEdit edit : m_dataSetEdits) {
	        NodeSettingsWO editSettings = dataSetSettings.addNodeSettings(edit.getPathFromFile() + edit.getName() + "/");
	        editSettings.addString("pathFromFile", edit.getPathFromFile());
			edit.saveSettings(editSettings);
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        NodeSettingsWO editSettings = attributeSettings.addNodeSettings(edit.getPathFromFile() + edit.getName() + "/");
	        editSettings.addString("pathFromFile", edit.getPathFromFile());
			edit.saveSettings(editSettings);
		}
	}
	
	@SuppressWarnings("unchecked")
	void loadConfiguration(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_groupEdits.clear();
        NodeSettingsRO groupSettings = settings.getNodeSettings(m_configRootName + "_Groups");
        Enumeration<NodeSettingsRO> groupEnum = groupSettings.children();
        while (groupEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = groupEnum.nextElement();
        	addGroupNodeEdit(GroupNodeEdit.loadSettings(editSettings));
        }
        
        m_dataSetEdits.clear();
        NodeSettingsRO dataSetSettings = settings.getNodeSettings(m_configRootName + "_DataSets");
        Enumeration<NodeSettingsRO> dataSetEnum = dataSetSettings.children();
        while (dataSetEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = dataSetEnum.nextElement();
        	addDataSetNodeEdit(DataSetNodeEdit.loadSettings(editSettings));
        }
        
        m_attributeEdits.clear();
        NodeSettingsRO attributeSettings = settings.getNodeSettings(m_configRootName + "_Attributes");
        Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
        while (attributeEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = attributeEnum.nextElement();
        	addAttributeNodeEdit(AttributeNodeEdit.loadSettings(editSettings));
        }
    }
}

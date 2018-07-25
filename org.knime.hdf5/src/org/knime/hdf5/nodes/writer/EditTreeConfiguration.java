package org.knime.hdf5.nodes.writer;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;

public class EditTreeConfiguration {
	
	private final String m_configRootName;

	private FileNodeEdit m_fileEdit;
	
	EditTreeConfiguration(String configRootName) {
		m_configRootName = configRootName;
	}

	public FileNodeEdit getFileNodeEdit() {
		return m_fileEdit;
	}

	public void setFileNodeEdit(FileNodeEdit edit) {
		m_fileEdit = edit;
		edit.validate();
	}
	
	void saveConfiguration(NodeSettingsWO settings) {
        if (m_fileEdit != null) {
            NodeSettingsWO fileSettings = settings.addNodeSettings(m_configRootName + "_File");
	        m_fileEdit.saveSettings(fileSettings);
        }
	}
	
	void loadConfiguration(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_fileEdit = null;
		if (settings.containsKey(m_configRootName + "_File")) {
	        NodeSettingsRO fileSettings = settings.getNodeSettings(m_configRootName + "_File");
	        setFileNodeEdit(FileNodeEdit.useFileSettings(fileSettings));
		}
    }
}

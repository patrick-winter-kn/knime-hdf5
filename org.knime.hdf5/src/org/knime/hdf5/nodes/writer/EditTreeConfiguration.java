package org.knime.hdf5.nodes.writer;

import java.io.IOException;

import javax.swing.JTree;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5File;
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

	public void setFileNodeEdit(FileNodeEdit fileEdit) {
		m_fileEdit = fileEdit;
	}
	
	void saveConfiguration(NodeSettingsWO settings) {
        if (m_fileEdit != null) {
            NodeSettingsWO fileSettings = settings.addNodeSettings(m_configRootName + "_File");
	        m_fileEdit.saveSettingsTo(fileSettings);
        }
	}
	
	void loadConfiguration(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_fileEdit = null;
		if (settings.containsKey(m_configRootName + "_File")) {
	        NodeSettingsRO fileSettings = settings.getNodeSettings(m_configRootName + "_File");
	        setFileNodeEdit(FileNodeEdit.useFileSettings(fileSettings));
		}
    }
	
	void initConfigOfFile(String filePath, boolean overwriteFile, boolean keepConfig, JTree tree) throws IOException {
		Hdf5File file = null;
		
		try {
			FileNodeEdit oldFileEdit = m_fileEdit;
			if (!overwriteFile && Hdf5File.existsHdfFile(filePath)) {
				file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
				m_fileEdit = new FileNodeEdit(file);
			} else {
				m_fileEdit = new FileNodeEdit(filePath, overwriteFile);
			}
			
			boolean treeAvailable = tree != null;
			if (treeAvailable) {
				m_fileEdit.setEditAsRootOfTree(tree);
			}
			
			if (!m_fileEdit.getEditAction().isCreateOrCopyAction()) {
				m_fileEdit.loadChildrenOfHdfObject();
			}
			if (keepConfig && oldFileEdit != null) {
				m_fileEdit.integrate(oldFileEdit);
			}
			
			if (treeAvailable) {
				m_fileEdit.reloadTreeWithEditVisible(true);
			}
			
		} finally {
			if (file != null) {
				file.close();
			}
		}
	}
	
	void updateFilePathOfConfig(boolean rename) throws IOException {
		initConfigOfFile(getFilePathToUpdate(rename), m_fileEdit.isOverwriteHdfFile(), true, null);
	}
	
	String getFilePathToUpdate(boolean rename) throws IOException {
		String filePath = m_fileEdit.getFilePath();
		
		if (rename) {
			filePath = Hdf5File.getUniqueFilePath(filePath);
		}
		
		return filePath;
	}
}

package org.knime.hdf5.nodes.writer;

import java.io.IOException;

import javax.swing.JTree;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.edit.EditOverwritePolicy;
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

	void checkConfiguration() throws InvalidSettingsException {
		EditTreeConfiguration checkConfig = new EditTreeConfiguration(m_configRootName + "_check");
		
		try {
			checkConfig.setFileNodeEdit(m_fileEdit.copyWithoutNoActionEdits());
			HDF5WriterNodeModel.checkForErrors(checkConfig);
			
		} catch (IOException ioe) {
			throw new InvalidSettingsException(ioe.getMessage(), ioe);
		}
	}
	
	void saveConfiguration(NodeSettingsWO settings) {
        if (m_fileEdit != null) {
            NodeSettingsWO fileSettings = settings.addNodeSettings(m_configRootName + "_File");
	        m_fileEdit.saveSettingsTo(fileSettings);
        }
	}
	
	void loadConfiguration(final NodeSettingsRO settings, final String oldFilePath, final EditOverwritePolicy policy) throws InvalidSettingsException {
		FileNodeEdit oldFileEdit = m_fileEdit;
		m_fileEdit = null;
		
		String filePath = null;
        if (oldFileEdit != null) {
        	filePath = oldFileEdit.getFilePath();
        } else {
        	filePath = oldFilePath;
        }
        // if filePath is still null, use filePath from settings
		
		if (settings.containsKey(m_configRootName + "_File")) {
	        NodeSettingsRO fileSettings = settings.getNodeSettings(m_configRootName + "_File");
	        m_fileEdit = FileNodeEdit.useFileSettings(fileSettings, filePath, policy);
		}
    }
	
	void updateConfiguration(final String urlPath, final EditOverwritePolicy policy) throws IOException {
		FileNodeEdit oldFileEdit = m_fileEdit;
		m_fileEdit = null;
		
		String oldFilePath = oldFileEdit != null ? oldFileEdit.getFilePath() : null;
		String filePath = urlPath.trim().isEmpty() ? oldFilePath : HDF5WriterNodeModel.getFilePathFromUrlPath(urlPath, false);
		
		if (filePath != null) {
			if (policy == EditOverwritePolicy.RENAME) {
				filePath = Hdf5File.getUniqueFilePath(filePath);
			}
			
			boolean overwriteFile = policy == EditOverwritePolicy.OVERWRITE;
			if (!overwriteFile && Hdf5File.existsHdf5File(filePath)) {
				Hdf5File file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
				m_fileEdit = new FileNodeEdit(file);
				file.close();
			} else {
				m_fileEdit = new FileNodeEdit(filePath, overwriteFile);
			}
			
			m_fileEdit.integrate(oldFileEdit);
		}
	}
	
	void initConfigOfFile(String filePath, boolean overwriteFile, boolean keepConfig, JTree tree) throws IOException {
		Hdf5File file = null;
		
		try {
			FileNodeEdit oldFileEdit = m_fileEdit;
			if (!overwriteFile && Hdf5File.existsHdf5File(filePath)) {
				file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
				m_fileEdit = new FileNodeEdit(file);
			} else {
				m_fileEdit = new FileNodeEdit(filePath, overwriteFile);
			}
			
			if (tree != null) {
				m_fileEdit.setEditAsRootOfTree(tree);
				if (!m_fileEdit.getEditAction().isCreateOrCopyAction()) {
					m_fileEdit.loadChildrenOfHdfObject();
				}
				
				if (keepConfig && oldFileEdit != null) {
					m_fileEdit.integrateAndValidate(oldFileEdit);
				}
				
				m_fileEdit.reloadTreeWithEditVisible(true);
				
			} else {
				if (keepConfig && oldFileEdit != null) {
					m_fileEdit.integrate(oldFileEdit);
				}
			}
		} finally {
			if (file != null) {
				file.close();
			}
		}
	}
}

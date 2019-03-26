package org.knime.hdf5.nodes.writer;

import java.io.IOException;

import javax.swing.JTree;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.edit.EditOverwritePolicy;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;

/**
 * The configuration how to modify/create an hdf file.
 */
public class EditTreeConfiguration {
	
	private final String m_configRootName;

	private FileNodeEdit m_fileEdit;
	
	EditTreeConfiguration(String configRootName) {
		m_configRootName = configRootName;
	}

	/**
	 * @return the root file edit
	 */
	public FileNodeEdit getFileNodeEdit() {
		return m_fileEdit;
	}

	/**
	 * Sets the root of this config to the new file edit.
	 * 
	 * @param fileEdit the new file edit
	 */
	void setFileNodeEdit(FileNodeEdit fileEdit) {
		m_fileEdit = fileEdit;
	}
	
	/**
	 * Initializes the config with the {@code filePath} and {@code overwriteFile}.
	 * <br>
	 * If {@code keepConfig} is {@code true} and a config already exists, the
	 * config within the file edit will be kept. Otherwise, the config of the
	 * file edit of {@code filePath} will be used.
	 * <br>
	 * <br>
	 * Also initializes the {@code tree} with this config.
	 * 
	 * @param filePath the new file path for this config
	 * @param overwriteFile if the new file edit should be in OVERWRITE policy
	 * @param keepConfig if the old config within the file edit should be kept
	 * @param tree the {@linkplain JTree} to be initialized with the new config
	 * @throws IOException if an hdf object of the hdf file could not be
	 * 	opened/closed
	 */
	void initConfigOfFile(String filePath, boolean overwriteFile, boolean keepConfig, JTree tree) throws IOException {
		Hdf5File file = null;
		
		try {
			FileNodeEdit oldFileEdit = m_fileEdit;
			
			// create a new file edit for the changed file path
			if (!overwriteFile && Hdf5File.existsHdf5File(filePath)) {
				file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
				m_fileEdit = new FileNodeEdit(file);
			} else {
				m_fileEdit = new FileNodeEdit(filePath, overwriteFile);
			}
			
			// integrate the old config and update the tree
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
	
	/**
	 * Updates the file path of the configuration with the {@code urlPath} and
	 * {@code policy}.
	 * 
	 * @param urlPath the url path
	 * @param policy the overwrite policy (may only be INTEGRATE,
	 * 	OVERWRITE or RENAME)
	 * @throws IOException if the file of the url path does not exist or could
	 * 	not be opened/closed
	 * @throws InvalidSettingsException if the settings of the fileEdit are invalid
	 */
	void updateConfiguration(final String urlPath, final EditOverwritePolicy policy) throws IOException, InvalidSettingsException {
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

	/**
	 * Checks this configuration for errors.
	 * 
	 * @throws InvalidSettingsException if the config is invalid
	 */
	void checkConfiguration() throws InvalidSettingsException {
		EditTreeConfiguration checkConfig = new EditTreeConfiguration(m_configRootName + "_check");
		
		try {
			checkConfig.setFileNodeEdit(m_fileEdit.copyWithoutNoActionEdits());
			HDF5WriterNodeModel.checkForErrors(checkConfig);
			
		} catch (InvalidSettingsException ise) {
			throw ise;
			
		} catch (Exception e) {
			throw new InvalidSettingsException("Could not check configuration: " + e.getMessage(), e);
		}
	}
	
	/**
	 * Save the settings of this configuration.
	 * 
	 * @param settings the settings where this configuration is saved
	 */
	void saveConfiguration(NodeSettingsWO settings) {
        if (m_fileEdit != null) {
            NodeSettingsWO fileSettings = settings.addNodeSettings(m_configRootName + "_File");
	        m_fileEdit.saveSettingsTo(fileSettings);
        }
	}
	
	/**
	 * Loads this configuration from the settings. The {@code filePath} will
	 * be used if the file edit of this config is {@code null}. Otherwise,
	 * use the file path of the config. If both is null, the file path from
	 * the settings will be used.
	 * 
	 * @param settings the settings to be loaded
	 * @param filePath the file path to use if the config does not have a
	 * 	file edit so far
	 * @param policy the overwrite policy to use (may only be INTEGRATE,
	 * 	OVERWRITE or RENAME)
	 * @throws InvalidSettingsException if the settings of the fileEdit are invalid
	 */
	void loadConfiguration(final NodeSettingsRO settings, final String filePath, final EditOverwritePolicy policy) throws InvalidSettingsException {
		FileNodeEdit oldFileEdit = m_fileEdit;
		m_fileEdit = null;
		
		String newFilePath = null;
        if (oldFileEdit != null) {
        	newFilePath = oldFileEdit.getFilePath();
        } else {
        	newFilePath = filePath;
        }
        
        // if filePath is still null, use filePath from settings
		if (settings.containsKey(m_configRootName + "_File")) {
	        NodeSettingsRO fileSettings = settings.getNodeSettings(m_configRootName + "_File");
	        m_fileEdit = FileNodeEdit.loadSettingsFrom(fileSettings, newFilePath, policy);
		}
    }
}

package org.knime.hdf5.nodes.writer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.FileUtil;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.edit.EditOverwritePolicy;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;

public class HDF5WriterNodeModel extends NodeModel {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelString m_fileOverwritePolicySettings;
	
	private SettingsModelBoolean m_saveColumnPropertiesSettings;
	
	private EditTreeConfiguration m_editTreeConfig;
	
	protected HDF5WriterNodeModel() {
		super(new PortType[] { BufferedDataTable.TYPE_OPTIONAL }, new PortType[] {});
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		m_fileOverwritePolicySettings = SettingsFactory.createFileOverwritePolicySettings();
		m_saveColumnPropertiesSettings = SettingsFactory.createSaveColumnPropertiesSettings();
		m_editTreeConfig = SettingsFactory.createEditTreeConfiguration();
	}
	
	protected HDF5WriterNodeModel(NodeCreationContext context) {
		this();
		m_filePathSettings.setStringValue(context.getUrl().getPath());
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		checkForErrors(m_editTreeConfig, inData[0]);

		boolean success = false;
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		try {
			success = fileEdit.doAction(inData[0], getAvailableFlowVariables(), m_saveColumnPropertiesSettings.getBooleanValue(), exec);
			
		} finally {
			// TODO change after testing
			NodeLogger.getLogger(getClass()).warn("Success: " + success);
			NodeLogger.getLogger(getClass()).warn("States of all edits during fail:\n" + fileEdit.getSummaryOfEditStates(false));
			
			if (success) {
				try {
					fileEdit.deleteAllBackups();
					
				} catch (Exception e) {
					NodeLogger.getLogger(getClass()).error("Deletion of backups failed: " + e.getMessage(), e);
				}
			} else {
				boolean rollbackSuccess = false;
				try {
					if (!success) {
						rollbackSuccess = fileEdit.doRollbackAction();
					}
				} catch (Exception e) {
					NodeLogger.getLogger(getClass()).error("Rollback failed: " + e.getMessage(), e);
				
				} finally {
					NodeLogger.getLogger(getClass()).warn("Success of rollback: " + rollbackSuccess);
					NodeLogger.getLogger(getClass()).warn("States of all edits after rollback:\n" + fileEdit.getSummaryOfEditStates(true));
				}
			}
			
			if (fileEdit.getHdfObject() != null) {
				((Hdf5File) fileEdit.getHdfObject()).close();
			}
		}
		
		return null;
	}
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		checkForErrors(m_editTreeConfig, null);
		return null;
    }
	
	/**
	 * Checks if there are errors in the configuration {@code editTreeConfig}.
	 * If the table {@code inputTable} is not null, it also considers the OverwritePolicy of {@code editTreeConfig}.
	 * 
	 * @param editTreeConfig
	 * @param inputTable
	 * @throws InvalidSettingsException
	 */
	private static void checkForErrors(EditTreeConfiguration editTreeConfig, BufferedDataTable inputTable) throws InvalidSettingsException {
		FileNodeEdit fileEdit = editTreeConfig.getFileNodeEdit();
		if (fileEdit == null) {
			throw new InvalidSettingsException("No file selected");
		}
		
		Hdf5File file = null;
		try {
			FileNodeEdit oldFileEdit = null;
			if (fileEdit.getEditAction().isCreateOrCopyAction()) {
				oldFileEdit = new FileNodeEdit(fileEdit.getFilePath(), fileEdit.isOverwriteHdfFile());
				
			} else {
				file = Hdf5File.openFile(fileEdit.getFilePath(), Hdf5File.READ_ONLY_ACCESS);
				oldFileEdit = new FileNodeEdit(file);
				oldFileEdit.loadChildrenOfHdfObject();
			}
			
			boolean valid = inputTable == null ? oldFileEdit.integrateAndValidate(fileEdit) : oldFileEdit.doLastValidationBeforeExecution(fileEdit, inputTable);
			if (!valid) {
				// TODO change after testing
				System.out.println(inputTable == null ? "1." : "2.");
				NodeLogger.getLogger(HDF5WriterNodeModel.class).warn/*throw new InvalidSettingsException*/("The configuration for file \"" + oldFileEdit.getFilePath()
						+ "\" is not valid:\n" + oldFileEdit.getInvalidCauseMessages(fileEdit));
			}
		} catch (IOException ioe) {
			throw new InvalidSettingsException("Could not check configuration: " + ioe.getMessage(), ioe.getCause());
			
		} finally {
			if (file != null) {
				file.close();
			}
		}
	}
	
	static String getFilePathFromUrlPath(String urlPath, boolean mustExist) throws IOException {
		if (urlPath == null || urlPath.trim().isEmpty()) {
			throw new IOException("No file selected");
		}
        
        try {
        	Path filePath = FileUtil.resolveToPath(FileUtil.toURL(urlPath));
        	if (mustExist || filePath.toFile().exists()) {
            	CheckUtils.checkSourceFile(filePath.toString());
            } else {
            	CheckUtils.checkDestinationDirectory(filePath.getParent().toString());
            }
            
            return filePath.toString();
            
        } catch (InvalidSettingsException | InvalidPathException | IOException | URISyntaxException | NullPointerException isipiousnpe) {
        	throw new IOException("Incorrect file path/url: " + isipiousnpe.getMessage(), isipiousnpe);
        }
	}

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		m_filePathSettings.saveSettingsTo(settings);
		m_fileOverwritePolicySettings.saveSettingsTo(settings);
		m_saveColumnPropertiesSettings.saveSettingsTo(settings);
		m_editTreeConfig.saveConfiguration(settings);
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		SettingsModelString filePathSettings = SettingsFactory.createFilePathSettings();
		filePathSettings.validateSettings(settings);
		filePathSettings.loadSettingsFrom(settings);

		SettingsModelString fileOverWritePolicySettings = SettingsFactory.createFileOverwritePolicySettings();
		fileOverWritePolicySettings.validateSettings(settings);
		fileOverWritePolicySettings.loadSettingsFrom(settings);
		EditOverwritePolicy policy = EditOverwritePolicy.get(fileOverWritePolicySettings.getStringValue());
		
		SettingsModelBoolean saveColumnPropertiesSettings = SettingsFactory.createSaveColumnPropertiesSettings();
		saveColumnPropertiesSettings.validateSettings(settings);
		saveColumnPropertiesSettings.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings, null, policy);
		checkForErrors(editTreeConfig, null);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		m_fileOverwritePolicySettings.loadSettingsFrom(settings);
		m_saveColumnPropertiesSettings.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings, null, EditOverwritePolicy.get(m_fileOverwritePolicySettings.getStringValue()));
		m_editTreeConfig = editTreeConfig;
	}

	@Override
	protected void reset() {
		try {
			m_editTreeConfig.updateConfiguration(m_filePathSettings.getStringValue(),
					EditOverwritePolicy.get(m_fileOverwritePolicySettings.getStringValue()));
		} catch (IOException ioe) {
			NodeLogger.getLogger("HDF5 Files").error("Reset failed: " + ioe.getMessage(), ioe);
		}
	}
}

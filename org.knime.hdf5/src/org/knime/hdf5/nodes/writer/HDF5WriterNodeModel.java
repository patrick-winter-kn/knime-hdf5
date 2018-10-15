package org.knime.hdf5.nodes.writer;

import java.io.File;
import java.io.IOException;

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
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;

public class HDF5WriterNodeModel extends NodeModel {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelBoolean m_forceCreationOfNewFile;
	
	private SettingsModelBoolean m_saveColumnProperties;
	
	private EditTreeConfiguration m_editTreeConfig;
	
	protected HDF5WriterNodeModel() {
		super(new PortType[] { BufferedDataTable.TYPE_OPTIONAL }, new PortType[] {});
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		m_forceCreationOfNewFile = SettingsFactory.createForceCreationOfNewFileSettings();
		m_saveColumnProperties = SettingsFactory.createSaveColumnPropertiesSettings();
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
			success = fileEdit.doAction(inData[0], getAvailableFlowVariables(), m_saveColumnProperties.getBooleanValue(), exec);
			
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
		
		String filePath = fileEdit.getFilePath();
		if (!fileEdit.getEditAction().isCreateOrCopyAction()) {
			Hdf5File file = null;
			try {
				file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
				FileNodeEdit oldFileEdit = new FileNodeEdit(file);
				oldFileEdit.loadChildrenOfHdfObject();
				boolean valid = inputTable == null ? oldFileEdit.integrate(fileEdit) : oldFileEdit.doLastValidationBeforeExecution(fileEdit, inputTable);
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
		} else if (Hdf5File.existsHdfFile(filePath)) {
			throw new InvalidSettingsException("The selected file \"" + filePath + "\" does already exist");
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
		m_forceCreationOfNewFile.saveSettingsTo(settings);
		m_saveColumnProperties.saveSettingsTo(settings);
		m_editTreeConfig.saveConfiguration(settings);
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		SettingsModelString filePathSettings = SettingsFactory.createFilePathSettings();
		filePathSettings.validateSettings(settings);
		filePathSettings.loadSettingsFrom(settings);

		SettingsModelBoolean forceCreationOfNewFile = SettingsFactory.createForceCreationOfNewFileSettings();
		forceCreationOfNewFile.validateSettings(settings);
		forceCreationOfNewFile.loadSettingsFrom(settings);
		
		SettingsModelBoolean saveColumnProperties = SettingsFactory.createSaveColumnPropertiesSettings();
		saveColumnProperties.validateSettings(settings);
		saveColumnProperties.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings);
		checkForErrors(editTreeConfig, null);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		m_forceCreationOfNewFile.loadSettingsFrom(settings);
		m_saveColumnProperties.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings);
		m_editTreeConfig = editTreeConfig;
	}

	@Override
	protected void reset() {}
}

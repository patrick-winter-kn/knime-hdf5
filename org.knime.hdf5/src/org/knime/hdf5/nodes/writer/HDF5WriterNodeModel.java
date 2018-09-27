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
	
	private SettingsModelBoolean m_structureMustMatch;
	
	private SettingsModelBoolean m_saveColumnProperties;
	
	private EditTreeConfiguration m_editTreeConfig;
	
	protected HDF5WriterNodeModel() {
		super(new PortType[] { BufferedDataTable.TYPE_OPTIONAL }, new PortType[] {});
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		m_structureMustMatch = SettingsFactory.createStructureMustMatchSettings();
		m_saveColumnProperties = SettingsFactory.createSaveColumnPropertiesSettings();
		m_editTreeConfig = SettingsFactory.createEditTreeConfiguration();
	}
	
	protected HDF5WriterNodeModel(NodeCreationContext context) {
		this();
		m_filePathSettings.setStringValue(context.getUrl().getPath());
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		checkForErrors(m_filePathSettings, m_editTreeConfig, inData[0]);

		// TODO find a possibility to estimate exec.setProgress()
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		try {
			fileEdit.doAction(inData[0], getAvailableFlowVariables(), m_saveColumnProperties.getBooleanValue());
		} catch (Exception e) {
			// just for testing if there are exceptions here
			NodeLogger.getLogger(getClass()).error(e.getMessage(), e);
		} finally {
			if (fileEdit.getHdfObject() != null) {
				((Hdf5File) fileEdit.getHdfObject()).close();
			}
		}
		
		return null;
	}
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		checkForErrors(m_filePathSettings, m_editTreeConfig, null);
		return null;
    }
	
	private static void checkForErrors(SettingsModelString filePathSettings,
			EditTreeConfiguration editTreeConfig, BufferedDataTable inputTable) throws InvalidSettingsException {
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
				oldFileEdit.integrate(fileEdit, inputTable, true);
				
			} catch (IOException ioe) {
				throw new InvalidSettingsException(ioe.getMessage(), ioe.getCause());
				
			} finally {
				if (file != null) {
					file.close();
				}
			}
		} else if (Hdf5File.existsHdfFile(filePath)) {
			throw new InvalidSettingsException("The selected file \"" + filePath + "\" does already exist");
		}
		
		if (!fileEdit.isValid()) {
			throw new InvalidSettingsException("The settings for file \"" + fileEdit.getFilePath()
					+ "\" are not valid:\n" + fileEdit.getInvalidCauseMessages());
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
		m_structureMustMatch.saveSettingsTo(settings);
		m_saveColumnProperties.saveSettingsTo(settings);
		m_editTreeConfig.saveConfiguration(settings);
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		SettingsModelString filePathSettings = SettingsFactory.createFilePathSettings();
		filePathSettings.validateSettings(settings);
		filePathSettings.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings);
		
		checkForErrors(filePathSettings, editTreeConfig, null);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		m_structureMustMatch.loadSettingsFrom(settings);
		m_saveColumnProperties.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings);
		m_editTreeConfig = editTreeConfig;
	}

	@Override
	protected void reset() {}
}

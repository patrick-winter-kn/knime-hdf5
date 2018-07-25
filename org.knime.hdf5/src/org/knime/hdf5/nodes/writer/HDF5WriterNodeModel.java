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
		m_saveColumnProperties = SettingsFactory.createStructureMustMatchSettings();
		m_editTreeConfig = SettingsFactory.createEditTreeConfiguration();
	}
	
	protected HDF5WriterNodeModel(NodeCreationContext context) {
		this();
		m_filePathSettings.setStringValue(context.getUrl().getPath());
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		checkForErrors(m_filePathSettings, m_editTreeConfig);

		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		try {
			fileEdit.doAction(inData[0], getAvailableFlowVariables(), m_saveColumnProperties.getBooleanValue());
		} finally {
			if (fileEdit.getHdfObject() != null) {
				((Hdf5File) fileEdit.getHdfObject()).close();
			}
		}
		
		
		
		/*
		String filePath = m_filePathSettings.getStringValue();
		
		if (Hdf5File.existsFile(filePath)) {
			Hdf5File file = null;
			try {
				file = Hdf5File.openFile(filePath, Hdf5File.READ_WRITE_ACCESS);
				
				// TODO find a possibility to estimate exec.setProgress()
				for (AttributeNodeEdit attributeEdit : m_editTreeConfig.getAttributeNodeEdits()) {
					String pathFromFile = attributeEdit.getOutputPathFromFileWithoutEndSlash();
					Hdf5TreeElement treeElement = null;
					try {
						treeElement = file.getGroupByPath(pathFromFile);
					} catch (IOException ioe) {
						treeElement = file.getDataSetByPath(pathFromFile);
					}
					createAttributeFromEdit(treeElement, attributeEdit);
				}
				
				for (DataSetNodeEdit dataSetEdit : m_editTreeConfig.getDataSetNodeEdits()) {
					String pathFromFile = dataSetEdit.getOutputPathFromFileWithoutEndSlash();
					Hdf5Group group = file.getGroupByPath(pathFromFile);
					createDataSetFromEdit(inData[0], exec, group, dataSetEdit);
				}

				for (GroupNodeEdit groupEdit : m_editTreeConfig.getGroupNodeEdits()) {
					String pathFromFile = groupEdit.getOutputPathFromFileWithoutEndSlash();
					Hdf5Group group = file.getGroupByPath(pathFromFile);
					createGroupFromEdit(inData[0], exec, group, groupEdit);
				}	
			} finally {
				if (file != null) {
					file.close();
				}
			}
		} else if (m_editTreeConfig.getFileNodeEdit() != null) {
			createFileFromEdit(inData[0], exec, m_editTreeConfig.getFileNodeEdit());
		}
		*/
		return null;
	}
	/*
	private boolean createFileFromEdit(BufferedDataTable inputTable, ExecutionContext exec, FileNodeEdit edit)
			throws IOException, CanceledExecutionException, HDF5LibraryException, NullPointerException {
		boolean success = true;
		Hdf5File file = null;
		
		try {
			file = Hdf5File.createFile(edit.getFilePath());
	
			for (AttributeNodeEdit attributeEdit : edit.getAttributeNodeEdits()) {
				exec.checkCanceled();
				success &= createAttributeFromEdit(file, attributeEdit);
			}
			
			for (DataSetNodeEdit dataSetEdit : edit.getDataSetNodeEdits()) {
				exec.checkCanceled();
				success &= createDataSetFromEdit(inputTable, exec, file, dataSetEdit);
			}
			
			for (GroupNodeEdit groupEdit : edit.getGroupNodeEdits()) {
				exec.checkCanceled();
				success &= createGroupFromEdit(inputTable, exec, file, groupEdit);
			}
		} finally {
			if (file != null) {
				file.close();
			}
		}
		
		return success;
	}
	
	private boolean createGroupFromEdit(BufferedDataTable inputTable, ExecutionContext exec, Hdf5Group parent, GroupNodeEdit edit)
			throws IOException, CanceledExecutionException, HDF5LibraryException, NullPointerException {
		Hdf5Group group = parent.createGroup(edit.getName());
		boolean success = group != null;

		for (AttributeNodeEdit attributeEdit : edit.getAttributeNodeEdits()) {
			exec.checkCanceled();
			success &= createAttributeFromEdit(group, attributeEdit);
		}
		
		for (DataSetNodeEdit dataSetEdit : edit.getDataSetNodeEdits()) {
			exec.checkCanceled();
			success &= createDataSetFromEdit(inputTable, exec, group, dataSetEdit);
		}
		
		for (GroupNodeEdit groupEdit : edit.getGroupNodeEdits()) {
			exec.checkCanceled();
			success &= createGroupFromEdit(inputTable, exec, group, groupEdit);
		}
		
		return success;
	}
	
	private boolean createDataSetFromEdit(BufferedDataTable inputTable, ExecutionContext exec, Hdf5Group parent, DataSetNodeEdit edit)
			throws IOException, CanceledExecutionException, HDF5LibraryException, NullPointerException {
		boolean success = true;
		
		if (edit.getHdfType() == HdfDataType.STRING) {
			int stringLength = 0;
			
			CloseableRowIterator iter = inputTable.iterator();
			while (iter.hasNext()) {
				exec.checkCanceled();
				
				DataRow row = iter.next();
				for (int i = 0; i < row.getNumCells(); i++) {
					DataCell cell = row.getCell(i);
					int newStringLength = cell.toString().length();
					stringLength = newStringLength > stringLength ? newStringLength : stringLength;
				}
			}

			if (!edit.isFixed()) {
				edit.setStringLength(stringLength);
				
			} else if (stringLength > edit.getStringLength()) {
				NodeLogger.getLogger(getClass()).warn("Fixed string length has been changed from " + edit.getStringLength() + " to " + stringLength);
				edit.setStringLength(stringLength);
			}
		}
		
		Hdf5DataSet<?> dataSet = parent.createDataSetFromEdit(inputTable.size(), edit);
		
		for (AttributeNodeEdit attributeEdit : edit.getAttributeNodeEdits()) {
			exec.checkCanceled();
			success &= createAttributeFromEdit(dataSet, attributeEdit);
		}
		
		DataTableSpec tableSpec = inputTable.getDataTableSpec();
		int[] specIndices = new int[edit.getColumnSpecs().length];
		for (int i = 0; i < specIndices.length; i++) {
			specIndices[i] = tableSpec.findColumnIndex(edit.getColumnSpecs()[i].getName());
		}
		
		CloseableRowIterator iter = inputTable.iterator();
		int rowId = 0;
		while (iter.hasNext()) {
			exec.checkCanceled();
			
			DataRow row = iter.next();
			success &= dataSet.writeRowToDataSet(row, specIndices, rowId);
			
			rowId++;
		}
		
		if (m_saveColumnProperties.getBooleanValue()) {
			String[] columnNames = new String[specIndices.length];
			String[] columnTypes = new String[specIndices.length];
			for (int i = 0; i < columnNames.length; i++) {
				DataColumnSpec spec = tableSpec.getColumnSpec(specIndices[i]);
				columnNames[i] = spec.getName();
				columnTypes[i] = spec.getType().getName();
			}

			try {
				success &= dataSet.createAndWriteAttribute(SettingsFactory.COLUMN_PROPERTY_NAMES, columnNames);
				success &= dataSet.createAndWriteAttribute(SettingsFactory.COLUMN_PROPERTY_TYPES, columnTypes);
			} catch (IOException ioe) {
				NodeLogger.getLogger("HDF5 Files").warn("Property attributes of dataSet \"" + dataSet.getPathFromFileWithName() + "\" could not be written completely");
			}
		}
		
		return success;
	}
	
	private boolean createAttributeFromEdit(Hdf5TreeElement parent, AttributeNodeEdit edit) throws IOException, HDF5LibraryException, NullPointerException {
		return edit.doAction(getAvailableFlowVariables().get(edit.getInputPathFromFileWithName()));
	}
	*/
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		checkForErrors(m_filePathSettings, m_editTreeConfig);
		return null;
    }
	
	private static void checkForErrors(SettingsModelString filePathSettings, EditTreeConfiguration editTreeConfig) throws InvalidSettingsException {
		if (filePathSettings.getStringValue().trim().isEmpty()) {
			throw new InvalidSettingsException("No file selected");
		}
		
		// TODO find causes for invalid things
		FileNodeEdit fileEdit = editTreeConfig.getFileNodeEdit();
		if (!fileEdit.isValid()) {
			throw new InvalidSettingsException("The settings for file \"" + fileEdit.getFilePath() + "\" are not valid");
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
		
		checkForErrors(filePathSettings, editTreeConfig);
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

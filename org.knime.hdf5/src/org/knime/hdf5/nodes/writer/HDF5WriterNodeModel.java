package org.knime.hdf5.nodes.writer;

import java.io.File;
import java.io.IOException;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.nodes.writer.edit.AttributeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.DataSetNodeEdit;
import org.knime.hdf5.nodes.writer.edit.GroupNodeEdit;

public class HDF5WriterNodeModel extends NodeModel {

	private SettingsModelString m_filePathSettings;
	
	private EditTreeConfiguration m_editTreeConfig;
	
	protected HDF5WriterNodeModel() {
		super(1, 0);
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		m_editTreeConfig = SettingsFactory.createEditTreeConfiguration();
	}
	
	protected HDF5WriterNodeModel(NodeCreationContext context) {
		this();
		m_filePathSettings.setStringValue(context.getUrl().getPath());
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		checkForErrors(m_filePathSettings);
		
		OverwritePolicy filePolicy = OverwritePolicy.OVERWRITE;
		Hdf5File file = null;
		
		try {
			file = getFile(filePolicy);
			
			for (GroupNodeEdit edit : m_editTreeConfig.getGroupNodeEdits()) {
				String pathFromFile = edit.getPathFromFileWithoutEndSlash();
				Hdf5Group group = file.getGroupByPath(pathFromFile);
				createGroupFromEdit(inData[0], exec, group, edit);
				// TODO delete settings after successful creation
			}
			
			for (DataSetNodeEdit edit : m_editTreeConfig.getDataSetNodeEdits()) {
				String pathFromFile = edit.getPathFromFileWithoutEndSlash();
				Hdf5Group group = file.getGroupByPath(pathFromFile);
				createDataSetFromEdit(inData[0], exec, group, edit);
				// TODO delete settings after successful creation
			}
			
			for (AttributeNodeEdit edit : m_editTreeConfig.getAttributeNodeEdits()) {
				String pathFromFile = edit.getPathFromFileWithoutEndSlash();
				Hdf5TreeElement treeElement = null;
				try {
					treeElement = file.getGroupByPath(pathFromFile);
				} catch (IOException ioe) {
					treeElement = file.getDataSetByPath(pathFromFile);
				}
				createAttributeFromEdit(treeElement, edit);
				// TODO delete settings after successful creation
			}
			
		} finally {
			file.close();
		}
		
		return null;
	}

	private Hdf5File getFile(OverwritePolicy policy) throws IOException {
		String filePath = m_filePathSettings.getStringValue();
		
		try {
			return Hdf5File.createFile(filePath);
			
		} catch (IOException ioe) {
			if (policy == OverwritePolicy.ABORT) {
				throw new IOException("Abort: " + ioe.getMessage());
			} else {
				return Hdf5File.openFile(filePath, Hdf5File.READ_WRITE_ACCESS);
			}
		}
	}
	
	private void createGroupFromEdit(BufferedDataTable inputTable, ExecutionContext exec, Hdf5Group parent, GroupNodeEdit edit)
			throws IOException, CanceledExecutionException {
		Hdf5Group group = parent.createGroup(edit.getName());
		
		for (GroupNodeEdit groupEdit : edit.getGroupNodeEdits()) {
	        createGroupFromEdit(inputTable, exec, group, groupEdit);
		}
		
		for (DataSetNodeEdit dataSetEdit : edit.getDataSetNodeEdits()) {
	        createDataSetFromEdit(inputTable, exec, group, dataSetEdit);
		}
		
		for (AttributeNodeEdit attributeEdit : edit.getAttributeNodeEdits()) {
	        createAttributeFromEdit(group, attributeEdit);
		}
	}
	
	private void createDataSetFromEdit(BufferedDataTable inputTable, ExecutionContext exec, Hdf5Group parent, DataSetNodeEdit edit)
			throws CanceledExecutionException, UnsupportedDataTypeException {
		Hdf5DataSet<?> dataSet = parent.createDataSetFromSpec(edit.getName(), inputTable.size(), edit.getColumnSpecs(), true);

		DataTableSpec tableSpec = inputTable.getDataTableSpec();
		int[] specIndex = new int[edit.getColumnSpecs().length];
		for (int i = 0; i < specIndex.length; i++) {
			specIndex[i] = tableSpec.findColumnIndex(edit.getColumnSpecs()[i].getName());
		}
		
		CloseableRowIterator iter = inputTable.iterator();
		int rowId = 0;
		while (iter.hasNext()) {
			exec.checkCanceled();
			// exec.setProgress((double) rowId / (inputTable.size() * dataSets.keySet().size()));
			
			DataRow row = iter.next();
			dataSet.writeRowToDataSet(row, specIndex, rowId);
			
			rowId++;
		}
	}
	
	private void createAttributeFromEdit(Hdf5TreeElement parent, AttributeNodeEdit edit) throws IOException {
		parent.createAndWriteAttributeFromFlowVariable(getAvailableFlowVariables().get(edit.getName()));
	}
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		checkForErrors(m_filePathSettings);
		return null;
    }
	
	private static void checkForErrors(SettingsModelString filePathSettings) throws InvalidSettingsException {
		String filePath = filePathSettings.getStringValue();
		if (filePath.trim().isEmpty()) {
			throw new InvalidSettingsException("No file selected");
		}
		if (!new File(filePath).exists()) {
			throw new InvalidSettingsException("The selected file \"" + filePath + "\" does not exist");
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
		m_editTreeConfig.saveConfiguration(settings);
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		SettingsModelString filePathSettings = SettingsFactory.createFilePathSettings();
		filePathSettings.validateSettings(settings);
		filePathSettings.loadSettingsFrom(settings);
		
		checkForErrors(filePathSettings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfiguration(settings);
		m_editTreeConfig = editTreeConfig;
	}

	@Override
	protected void reset() {}
}

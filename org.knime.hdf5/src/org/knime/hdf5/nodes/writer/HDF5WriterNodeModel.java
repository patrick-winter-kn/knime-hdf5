package org.knime.hdf5.nodes.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
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
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;

import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

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
			file = createFile(filePolicy);
			
			Map<String, List<DataColumnSpec>> dataSets = new HashMap<>();
			List<TreeNodeEdit> attributeEdits = new ArrayList<>();
			for (TreeNodeEdit edit : m_editTreeConfig.getEdits()) {
				switch (edit.getNodeValueClassId()) {
				case TreeNodeEdit.HDF5_GROUP:
					// TODO
					break;
				case TreeNodeEdit.HDF5_DATASET:
					dataSets.put(edit.getPathFromFile() + edit.getName(), new ArrayList<>());
					break;
				case TreeNodeEdit.DATA_COLUMN_SPEC:
					dataSets.get(edit.getPathFromFile().substring(0, edit.getPathFromFile().length() - 1)).add(inData[0].getDataTableSpec().getColumnSpec(edit.getName()));
					break;
				case TreeNodeEdit.FLOW_VARIABLE:
					attributeEdits.add(edit);
				}
			}
			
			for (String pathFromFileWithName : dataSets.keySet()) {
				int index = pathFromFileWithName.lastIndexOf("/");
				String pathFromFile = index >= 0 ? pathFromFileWithName.substring(0, index) : "";
				Hdf5Group group = file.getGroupByPath(pathFromFile);
				String name = pathFromFileWithName.substring(index + 1);
				
				List<DataColumnSpec> specs = dataSets.get(pathFromFileWithName);
				Hdf5DataSet<?> dataSet = group.createDataSetFromSpec(name, inData[0].size(), specs.toArray(new DataColumnSpec[] {}), true);

				DataTableSpec tableSpec = inData[0].getDataTableSpec();
				int[] specIndex = new int[specs.size()];
				for (int i = 0; i < specIndex.length; i++) {
					specIndex[i] = tableSpec.findColumnIndex(specs.get(i).getName());
				}
				
				CloseableRowIterator iter = inData[0].iterator();
				int rowId = 0;
				while (iter.hasNext()) {
					exec.checkCanceled();
					exec.setProgress((double) rowId / (inData[0].size() * dataSets.keySet().size()));
					
					DataRow row = iter.next();
					dataSet.writeRowToDataSet(row, specIndex, rowId);
					
					rowId++;
				}
			}
			
			for (TreeNodeEdit edit : attributeEdits) {
				Hdf5TreeElement treeElement = null;
				String pathFromFile = edit.getPathFromFile();
				String pathFromFileWithoutEndSlash = !pathFromFile.isEmpty() ? pathFromFile.substring(0, pathFromFile.length() - 1) : "";
				try {
					treeElement = file.getGroupByPath(pathFromFileWithoutEndSlash);
				} catch (IOException ioe) {
					treeElement = file.getDataSetByPath(pathFromFileWithoutEndSlash);
				}
				treeElement.createAndWriteAttributeFromFlowVariable(getAvailableFlowVariables().get(edit.getName()));
			}
			
		} finally {
			file.close();
		}
		
		return null;
	}
	
	private Hdf5File createFile(OverwritePolicy policy) throws IOException {
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
		editTreeConfig.loadConfigurationInModel(settings);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfigurationInModel(settings);
		m_editTreeConfig = editTreeConfig;
	}

	@Override
	protected void reset() {}
}

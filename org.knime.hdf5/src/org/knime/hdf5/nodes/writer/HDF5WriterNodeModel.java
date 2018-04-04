package org.knime.hdf5.nodes.writer;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;

public class HDF5WriterNodeModel extends NodeModel {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelString m_groupPathSettings;
	
	private SettingsModelString m_groupNameSettings;
	
	protected HDF5WriterNodeModel() {
		super(1, 0);
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		m_groupPathSettings = SettingsFactory.createGroupPathSettings();
		m_groupNameSettings = SettingsFactory.createGroupNameSettings();
	}
	
	protected HDF5WriterNodeModel(NodeCreationContext context) {
		this();
		m_filePathSettings.setStringValue(context.getUrl().getPath());
	}

	private static String removeBeginAndEndSlashes(String path) {
		int begin = path.startsWith("/") ? 1 : 0;
		int end = path.length() - (path.endsWith("/") ?  1 : 0);
		
		return path.substring(begin, end);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		String filePath = m_filePathSettings.getStringValue();
		String groupPath = removeBeginAndEndSlashes(m_groupPathSettings.getStringValue());
		String groupName = m_groupNameSettings.getStringValue();

		Hdf5File file = null;
		try {
			file = Hdf5File.openFile(filePath, Hdf5File.READ_WRITE_ACCESS);
		} catch (IOException ioe) {
			throw new InvalidSettingsException(ioe.getMessage(), ioe);
		}
		
		try {
			Hdf5Group parentGroup = file.getGroupByPath(groupPath);
			Hdf5Group group = null;
			try {
				group = parentGroup.createGroup(groupName);
				
			} catch (IOException ioe) {
				group = parentGroup.getGroup(groupName);
			}
			
			List<Hdf5DataSet<?>> dataSets = group.createDataSetsFromSpec(groupName, inData[0].size(), inData[0].getDataTableSpec());
			
			CloseableRowIterator iter = inData[0].iterator();
			int rowId = 0;
			while (iter.hasNext()) {
				exec.checkCanceled();
				exec.setProgress((double) rowId / inData[0].size());
				
				DataRow row = iter.next();
				int specIndex = 0;
				for (int i = 0; i < dataSets.size(); i++) {
					Hdf5DataSet<?> dataSet = dataSets.get(i);
					dataSet.writeRowToDataSet(row, specIndex, rowId);
					specIndex += dataSet.numberOfValuesFrom(1);
				}
				
				rowId++;
			}
			
			peekFlowVariables(group);
			
		} finally {
			file.close();
		}
		
		return null;
	}
	
	private void peekFlowVariables(Hdf5Group group) throws IOException {
		Iterator<FlowVariable> iter = getAvailableFlowVariables().values().iterator();
		while (iter.hasNext()) {
			group.createAndWriteAttributeFromFlowVariable(iter.next());
		}
	}
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		return null;
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
		m_groupPathSettings.saveSettingsTo(settings);
		m_groupNameSettings.saveSettingsTo(settings);
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		SettingsModelString filePathSettings = SettingsFactory.createFilePathSettings();
		filePathSettings.validateSettings(settings);
		filePathSettings.loadSettingsFrom(settings);
		
		SettingsModelString groupPathSettings = SettingsFactory.createGroupPathSettings();
		groupPathSettings.validateSettings(settings);
		groupPathSettings.loadSettingsFrom(settings);
		
		SettingsModelString groupNameSettings = SettingsFactory.createGroupNameSettings();
		groupNameSettings.validateSettings(settings);
		groupNameSettings.loadSettingsFrom(settings);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		m_groupPathSettings.loadSettingsFrom(settings);
		m_groupNameSettings.loadSettingsFrom(settings);
	}

	@Override
	protected void reset() {}
}

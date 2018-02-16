package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class HDF5ReaderNodeModel extends NodeModel {

	private SettingsModelString m_filePathSettings;

	private SettingsModelBoolean m_failIfRowSizeDiffersSettings;

	private DataColumnSpecFilterConfiguration m_dataSetFilterConfig;

	private DataColumnSpecFilterConfiguration m_attributeFilterConfig;

	protected HDF5ReaderNodeModel() {
		super(0, 1);
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		m_failIfRowSizeDiffersSettings = SettingsFactory.createFailIfRowSizeDiffersSettings();
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		checkForErrors(m_filePathSettings, m_failIfRowSizeDiffersSettings, m_dataSetFilterConfig);
		Hdf5File file = null;
		BufferedDataContainer outContainer = null;

		try {
			file = Hdf5File.openFile(m_filePathSettings.getStringValue());
			outContainer = exec.createDataContainer(createOutSpec());
			String[] dataSetPaths = m_dataSetFilterConfig.applyTo(file.createSpecOfDataSets()).getIncludes();

			Hdf5DataSet<?>[] dataSets = new Hdf5DataSet<?>[dataSetPaths.length];
			long maxRows = 0;
			for (int i = 0; i < dataSetPaths.length; i++) {
				dataSets[i] = file.getDataSetByPath(dataSetPaths[i]);
				dataSets[i].open();

				long rowSize = dataSets[i].getDimensions()[0];
				maxRows = rowSize > maxRows ? rowSize : maxRows;
			}

			for (long i = 0; i < maxRows; i++) {
				exec.checkCanceled();
				exec.setProgress((double) i / maxRows);

				List<DataCell> row = new LinkedList<>();
				for (Hdf5DataSet<?> dataSet : dataSets) {
					dataSet.extendRow(row, i);
				}

				outContainer.addRowToTable(new DefaultRow("Row" + i, row));
			}

			pushFlowVariables(file);

		} finally {
			file.close();
			outContainer.close();
		}

		return new BufferedDataTable[] { outContainer.getTable() };
	}

	private void pushFlowVariables(Hdf5File file) {
		String[] attributePaths = m_attributeFilterConfig.applyTo(file.createSpecOfAttributes()).getIncludes();
		if (attributePaths != null) {
			for (String attrPath: attributePaths) {
				Hdf5Attribute<?> attr = file.getAttributeByPath(attrPath);
				
				if (attr != null) {
					int attrNum = attr.getValue().length;
					
					if (attrNum == 1) {
						if (attr.getType().isKnimeType(Hdf5KnimeDataType.INTEGER)) {
							pushFlowVariableInt(attrPath, (Integer) attr.getValue()[0]);
							
						} else if (attr.getType().isKnimeType(Hdf5KnimeDataType.DOUBLE)) {
							pushFlowVariableDouble(attrPath, (Double) attr.getValue()[0]);
							
						} else if (attr.getType().isKnimeType(Hdf5KnimeDataType.STRING)) {
							pushFlowVariableString(attrPath, "" + attr.getValue()[0]);
						}
						
					} else {
						pushFlowVariableString(attrPath, Arrays.toString(attr.getValue()) + " ("
								+ attr.getType().getKnimeType().getArrayType() + ")");
					}
				}
			}
		}
	}

	private DataTableSpec createOutSpec() throws InvalidSettingsException {
		String filePath = m_filePathSettings.getStringValue();
		List<DataColumnSpec> colSpecList = new ArrayList<>();

		if (filePath != null) {
			Hdf5File file = null;
			try {
				file = Hdf5File.openFile(filePath);
			} catch (IOException e) {
				throw new InvalidSettingsException(e.getMessage(), e);
			}
			String[] dataSetPaths = m_dataSetFilterConfig.applyTo(file.createSpecOfDataSets()).getIncludes();
			file = Hdf5File.createFile(filePath);
			for (String dsPath : dataSetPaths) {
				Hdf5DataSet<?> dataSet = file.getDataSetByPath(dsPath);
				Hdf5KnimeDataType dataType = dataSet.getType().getKnimeType();
				DataType type = dataType.getColumnType();

				if (dataSet.getDimensions().length > 1) {
					long[] colDims = new long[dataSet.getDimensions().length - 1];
					Arrays.fill(colDims, 0);

					do {
						colSpecList
								.add(new DataColumnSpecCreator(dsPath + Arrays.toString(colDims), type).createSpec());
					} while (dataSet.nextColumnDims(colDims));

				} else {
					colSpecList.add(new DataColumnSpecCreator(dsPath, type).createSpec());
				}
			}
		}

		return new DataTableSpec(colSpecList.toArray(new DataColumnSpec[] {}));
	}

	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		checkForErrors(m_filePathSettings, m_failIfRowSizeDiffersSettings, m_dataSetFilterConfig);
		return new DataTableSpec[] { createOutSpec() };
	}
	
	private static void checkForErrors(SettingsModelString filePathSettings, 
			SettingsModelBoolean failIfRowSizeDiffersSettings, 
			DataColumnSpecFilterConfiguration dataSetFilterConfig) throws InvalidSettingsException {
		String filePath = filePathSettings.getStringValue();
		if (filePath.trim().isEmpty()) {
			throw new InvalidSettingsException("No file selected");
		}
		if (!new File(filePath).exists()) {
			throw new InvalidSettingsException("The selected file \"" + filePath + "\" does not exist");
		}
		Hdf5File file = null;
		try {
			file = Hdf5File.openFile(filePath);
		} catch (Exception e) {
			throw new InvalidSettingsException(e.getMessage(), e);
		}
		if (failIfRowSizeDiffersSettings.getBooleanValue()) {
			String[] dataSetPaths = dataSetFilterConfig.applyTo(file.createSpecOfDataSets()).getIncludes();
			Set<Long> rowSizes = new TreeSet<>();
			for (String dataSetPath : dataSetPaths) {
				Hdf5DataSet<?> dataSet = file.getDataSetByPath(dataSetPath);
				rowSizes.add(dataSet.getDimensions()[0]);
			}
			if (rowSizes.size() > 1) {
				throw new InvalidSettingsException("Found unequal row sizes " + rowSizes);
			}
		}
	}

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
	}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		m_filePathSettings.saveSettingsTo(settings);
		m_failIfRowSizeDiffersSettings.saveSettingsTo(settings);
		m_dataSetFilterConfig.saveConfiguration(settings);
		m_attributeFilterConfig.saveConfiguration(settings);
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		SettingsModelString filePathSettings = SettingsFactory.createFilePathSettings();
		filePathSettings.validateSettings(settings);
		filePathSettings.loadSettingsFrom(settings);
		SettingsModelBoolean failIfRowSizeDiffersSettings = SettingsFactory.createFailIfRowSizeDiffersSettings();
		failIfRowSizeDiffersSettings.validateSettings(settings);
		failIfRowSizeDiffersSettings.loadSettingsFrom(settings);
		DataColumnSpecFilterConfiguration dataSetFilterConfig = SettingsFactory.createDataSetFilterConfiguration();
		dataSetFilterConfig.loadConfigurationInModel(settings);
		checkForErrors(filePathSettings, failIfRowSizeDiffersSettings, dataSetFilterConfig);
		
		DataColumnSpecFilterConfiguration attributeFilterConfig = SettingsFactory.createAttributeFilterConfiguration();
		attributeFilterConfig.loadConfigurationInModel(settings);
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_filePathSettings.loadSettingsFrom(settings);
		m_failIfRowSizeDiffersSettings.loadSettingsFrom(settings);
		DataColumnSpecFilterConfiguration dataSetFilterConfig = SettingsFactory.createDataSetFilterConfiguration();
		dataSetFilterConfig.loadConfigurationInModel(settings);
		m_dataSetFilterConfig = dataSetFilterConfig;
		DataColumnSpecFilterConfiguration attributeFilterConfig = SettingsFactory.createAttributeFilterConfiguration();
		attributeFilterConfig.loadConfigurationInModel(settings);
		m_attributeFilterConfig = attributeFilterConfig;
	}

	@Override
	protected void reset() {
	}
}

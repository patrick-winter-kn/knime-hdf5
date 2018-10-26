package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.activation.UnsupportedDataTypeException;

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
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.util.FileUtil;
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
		m_dataSetFilterConfig = SettingsFactory.createDataSetFilterConfiguration();
		m_attributeFilterConfig = SettingsFactory.createAttributeFilterConfiguration();
	}
	
	protected HDF5ReaderNodeModel(NodeCreationContext context) {
		this();
		m_filePathSettings.setStringValue(context.getUrl().getPath());
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		checkForErrors(m_filePathSettings, m_failIfRowSizeDiffersSettings, m_dataSetFilterConfig);
		Hdf5File file = null;
		BufferedDataContainer outContainer = null;

		try {
	        file = Hdf5File.openFile(getFilePathFromUrlPath(m_filePathSettings.getStringValue(), true), Hdf5File.READ_ONLY_ACCESS);
			outContainer = exec.createDataContainer(createOutSpec());
			String[] dataSetPaths = m_dataSetFilterConfig.applyTo(file.createSpecOfDataSets()).getIncludes();

			Hdf5DataSet<?>[] dataSets = new Hdf5DataSet<?>[dataSetPaths.length];
			long maxRows = 0;
			for (int i = 0; i < dataSetPaths.length; i++) {
				dataSets[i] = file.getDataSetByPath(dataSetPaths[i]);

				long rowCount = dataSets[i].numberOfRows();
				maxRows = rowCount > maxRows ? rowCount : maxRows;
			}

			for (long i = 0; i < maxRows; i++) {
				exec.checkCanceled();
				exec.setProgress((double) i / maxRows);

				List<DataCell> row = new ArrayList<>();
				for (Hdf5DataSet<?> dataSet : dataSets) {
					dataSet.extendRow(row, i);
				}
				
				if (!row.isEmpty()) {
					outContainer.addRowToTable(new DefaultRow("Row" + i, row));
				}
			}

			pushFlowVariables(file);

		} finally {
			file.close();
			outContainer.close();
		}
		
		return new BufferedDataTable[] { outContainer.getTable() };
	}

	private void pushFlowVariables(Hdf5File file) throws IOException {
		String[] attributePaths = m_attributeFilterConfig.applyTo(file.createSpecOfAttributes()).getIncludes();
		if (attributePaths != null) {
			for (String attrPath: attributePaths) {
				Hdf5Attribute<?> attr = file.getAttributeByPath(attrPath);
				
				if (attr != null) {
					if (attr.getValue() == null) {
						attr.read();
					}
					
					if (attr.getDimension() == 1) {
						if (attr.getType().isKnimeType(Hdf5KnimeDataType.INTEGER)) {
							pushFlowVariableInt(attrPath, (Integer) attr.getValue()[0]);
							
						} else if (attr.getType().isKnimeType(Hdf5KnimeDataType.DOUBLE)) {
							pushFlowVariableDouble(attrPath, (Double) attr.getValue()[0]);
							
						} else if (attr.getType().isKnimeType(Hdf5KnimeDataType.STRING)) {
							pushFlowVariableString(attrPath, "" + attr.getValue()[0]);
						}
					} else {
						pushFlowVariableString(attrPath, Arrays.toString(attr.getValue()) + " ("
								+ attr.getType().getKnimeType().toString() + ")");
					}
				}
			}
		}
	}

	private DataTableSpec createOutSpec() throws InvalidSettingsException {
		List<DataColumnSpec> colSpecList = new ArrayList<>();
        
		Hdf5File file = null;
		try {
			file = Hdf5File.openFile(getFilePathFromUrlPath(m_filePathSettings.getStringValue(), true), Hdf5File.READ_ONLY_ACCESS);
		} catch (IOException ioe) {
			throw new InvalidSettingsException(ioe.getMessage(), ioe);
		}
		
		try {
			String[] dataSetPaths = m_dataSetFilterConfig.applyTo(file.createSpecOfDataSets()).getIncludes();
		
			for (String dsPath : dataSetPaths) {
				Hdf5DataSet<?> dataSet = null;
				try {
					dataSet = file.getDataSetByPath(dsPath);
				} catch (IOException ioe) {
					throw new InvalidSettingsException(ioe.getMessage(), ioe);
				}
				
				Hdf5KnimeDataType dataType = dataSet.getType().getKnimeType();
				
				try {
					DataType type = dataType.getColumnDataType();
	
					if (dataSet.getDimensions().length > 1) {
						long[] colDims = new long[dataSet.getDimensions().length - 1];
						Arrays.fill(colDims, 0);
	
						do {
							colSpecList
									.add(new DataColumnSpecCreator(dsPath + Arrays.toString(colDims), type).createSpec());
						} while (dataSet.nextColumnDims(colDims));
	
					} else {
						// also do this for dataSet.getDimensions().length == 0 which means that the dataSet is scalar
						colSpecList.add(new DataColumnSpecCreator(dsPath, type).createSpec());
					}
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").warn("Unknown dataType of columns in \"" + dsPath + "\"");
				}
			}
		} finally {
			file.close();
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
		Hdf5File file = null;
		try {
			file = Hdf5File.openFile(getFilePathFromUrlPath(filePathSettings.getStringValue(), true), Hdf5File.READ_ONLY_ACCESS);
		} catch (IOException ioe) {
			throw new InvalidSettingsException("Could not check configuration: " + ioe.getMessage(), ioe);
		}
		
		try {
			if (failIfRowSizeDiffersSettings.getBooleanValue()) {
				String[] dataSetPaths = dataSetFilterConfig.applyTo(file.createSpecOfDataSets()).getIncludes();
				Map<Long, List<String>> rowSizes = new TreeMap<>();
				for (String dataSetPath : dataSetPaths) {
					Hdf5DataSet<?> dataSet = null;
					try {
						dataSet = file.getDataSetByPath(dataSetPath);
					} catch (IOException ioe) {
						throw new InvalidSettingsException("Could not check configuration: " + ioe.getMessage(), ioe);
					}
					
					long rowCount = dataSet.numberOfRows();
					if (!rowSizes.containsKey(rowCount)) {
						List<String> paths = new ArrayList<>();
						rowSizes.put(rowCount, paths);
					}
					rowSizes.get(rowCount).add(dataSetPath);
				}
				if (rowSizes.size() > 1) {
					String message = "Found unequal row sizes:";
					for (long rowCount : rowSizes.keySet()) {
						message += "\n" + rowCount + " row" + (rowCount != 1 ? "s" : "") + ":";
						for (String dataSetPath : rowSizes.get(rowCount)) {
							message += "\n- " + dataSetPath;
						}
					}
					throw new InvalidSettingsException(message);
				}
			}
		} finally {
			file.close();
		}
	}
	
	static String getFilePathFromUrlPath(String urlPath, boolean mustExist) throws InvalidSettingsException {
		if (urlPath.trim().isEmpty()) {
			throw new InvalidSettingsException("No file selected");
		}
        
        try {
        	String filePath = FileUtil.resolveToPath(FileUtil.toURL(urlPath)).toString();
        	if (mustExist || new File(filePath).exists()) {
            	CheckUtils.checkSourceFile(urlPath);
            } else {
            	CheckUtils.checkDestinationDirectory(urlPath);
            }
            
            return filePath;
            
        } catch (InvalidPathException | IOException | URISyntaxException | NullPointerException ipiousnpe) {
        	throw new InvalidSettingsException("Incorrect file path/url: " + ipiousnpe.getMessage(), ipiousnpe);
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

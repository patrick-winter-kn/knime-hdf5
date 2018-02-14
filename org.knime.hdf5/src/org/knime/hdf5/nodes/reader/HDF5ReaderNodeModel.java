package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
import org.knime.core.node.NodeLogger;
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

	private String m_filePath;
	
	private String[] m_dsPaths;
	
	private String[] m_attrPaths;
	
    private SettingsModelString m_fcSource;
    
    private SettingsModelBoolean m_firsdSource;
	
	protected HDF5ReaderNodeModel() {
		super(0, 1);
		m_fcSource = SettingsFactory.createFcSourceSettings();
		m_firsdSource = SettingsFactory.createFirsdSourceSettings();
	}

    /** A new configuration to store the settings of the dataSet filter. Also enables the type filter.
     * @return ...
     */
    static final DataColumnSpecFilterConfiguration createDsFilterPanelConfiguration() {
        return new DataColumnSpecFilterConfiguration(SettingsFactory.DS_CONF_KEY);
    }

    /** A new configuration to store the settings of the attribute filter. Also enables the type filter.
     * @return ...
     */
    static final DataColumnSpecFilterConfiguration createAttrFilterPanelConfiguration() {
        return new DataColumnSpecFilterConfiguration(SettingsFactory.ATTR_CONF_KEY);
    }

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		if (m_filePath == null || m_dsPaths == null || m_dsPaths.length == 0) {
			NodeLogger.getLogger("HDF5 Reader").error("No dataSet to use", new NullPointerException());
		}

		Hdf5File file = null;
		BufferedDataContainer outContainer = null;

		try {
			file = Hdf5File.createFile(m_filePath);
			outContainer = exec.createDataContainer(createOutSpec());
			
			Hdf5DataSet<?>[] dataSets = new Hdf5DataSet<?>[m_dsPaths.length];
			long maxRows = 0;
			for (int i = 0; i < m_dsPaths.length; i++) {
				dataSets[i] = file.getDataSetByPath(m_dsPaths[i]);
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
		
		return new BufferedDataTable[]{outContainer.getTable()};
	}
	
	private void pushFlowVariables(Hdf5File file) {
		if (m_attrPaths != null) {
			for (String attrPath: m_attrPaths) {
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
	
	private DataTableSpec createOutSpec() {
		List<DataColumnSpec> colSpecList = new LinkedList<>();
		
		if (m_filePath != null && m_dsPaths != null) {
			Hdf5File file = Hdf5File.createFile(m_filePath);
			for (String dsPath: m_dsPaths) {
				Hdf5DataSet<?> dataSet = file.getDataSetByPath(dsPath);
				Hdf5KnimeDataType dataType = dataSet.getType().getKnimeType();
				DataType type = dataType.getColumnType();
				
				if (dataSet.getDimensions().length > 1) {
					long[] colDims = new long[dataSet.getDimensions().length - 1];
					Arrays.fill(colDims, 0);
					
					do {
						colSpecList.add(new DataColumnSpecCreator(dsPath + Arrays.toString(colDims), type).createSpec());
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
		return new DataTableSpec[]{createOutSpec()};
    }

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		m_fcSource.validateSettings(settings);
		m_firsdSource.validateSettings(settings);
		
		if (settings.containsKey("allRowSizesEqual")) {
			m_firsdSource.loadSettingsFrom(settings);
			if (m_firsdSource.getBooleanValue() && !settings.getBoolean("allRowSizesEqual")) {
				throw new InvalidSettingsException("Fail because rowSize differs");
			}
			
		} else {
			throw new InvalidSettingsException("Settings unreadable");
		}
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_fcSource.loadSettingsFrom(settings);
		m_filePath = m_fcSource.getStringValue();
		Hdf5File file = Hdf5File.createFile(m_filePath);

        DataColumnSpecFilterConfiguration dsConf = createDsFilterPanelConfiguration();
        dsConf.loadConfigurationInModel(settings);
		m_dsPaths = dsConf.applyTo(file.createSpecOfDataSets()).getIncludes();
		
        DataColumnSpecFilterConfiguration attrConf = createAttrFilterPanelConfiguration();
        attrConf.loadConfigurationInModel(settings);
		m_attrPaths = attrConf.applyTo(file.createSpecOfAttributes()).getIncludes();
	}

	@Override
	protected void reset() {}
}

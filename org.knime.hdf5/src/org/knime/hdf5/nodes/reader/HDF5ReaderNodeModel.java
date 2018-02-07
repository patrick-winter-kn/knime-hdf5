package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
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
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class HDF5ReaderNodeModel extends NodeModel {

	private String m_filePath;
	
	private String[] m_dsPaths;
	
	private int m_maxRows;
	
	private String[] m_attrPaths;
	
	
	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		if (m_filePath == null || m_dsPaths == null || m_dsPaths.length == 0) {
			NodeLogger.getLogger("HDF5 Files").error("No dataSet to use", new NullPointerException());
		}

		Hdf5File file = null;
		BufferedDataContainer outContainer = null;

		try {
			file = Hdf5File.createFile(m_filePath);
			outContainer = exec.createDataContainer(createOutSpec());
			
			for (DataRow row: dataSetsToRows(file)) {
				outContainer.addRowToTable(row);
			}
			
			pushFlowVariables(file);

		} catch (Exception e) {
		} finally {
			file.close();
	        outContainer.close();
		}
		
		return new BufferedDataTable[]{outContainer.getTable()};
	}
	
	private DataRow[] dataSetsToRows(Hdf5File file) {
		DataRow[] rows = new DataRow[m_maxRows];

		for (String dsPath: m_dsPaths) {
			Hdf5DataSet<?> dataSet = file.getDataSetByPath(dsPath);
			dataSet.open();
			dataSet.extendRows(rows);
		}
		
		return rows;
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
				
				int rowNum = (int) dataSet.getDimensions()[0];
				m_maxRows = rowNum > m_maxRows ? rowNum : m_maxRows;
				
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
		if (settings.containsKey("allRowSizesEqual") && !settings.getBoolean("allRowSizesEqual")) {
			throw new InvalidSettingsException("contains MissingCells (not all columns have equal row number)");
		}
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey("filePath") && settings.containsKey("dataSetsIncluded") && settings.containsKey("attributesIncluded")) {
			m_filePath = settings.getString("filePath");
			m_dsPaths = DataTableSpec.load(settings.getConfig("dataSetsIncluded")).getColumnNames();
			m_maxRows = 0;
			m_attrPaths = DataTableSpec.load(settings.getConfig("attributesIncluded")).getColumnNames();
		}
	}

	@Override
	protected void reset() {}
}

package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.JoinedRow;
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
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5DataType;
import org.knime.hdf5.lib.Hdf5File;

public class HDF5ReaderNodeModel extends NodeModel {

    private Hdf5DataSet<?>[] m_dataSets;
    
	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		if (m_dataSets.length == 0) {
			NodeLogger.getLogger("HDF5 Files").error("No dataSet to use", new NullPointerException());
			throw new NullPointerException();
		}

		Hdf5File file = Hdf5File.createFile(m_dataSets[0].getFilePath());
		BufferedDataContainer outContainer = exec.createDataContainer(createOutSpec());
		
		int maxRows = 0;
		for (Hdf5DataSet<?> dataSet: m_dataSets) {
			dataSet.open();
			System.out.println(dataSet.getPathFromFile() + "/" + dataSet.getName());
			
			int rowNum = (int) dataSet.getDimensions()[0];
			maxRows = rowNum > maxRows ? rowNum : maxRows;
		}
		DataRow[] rows = new DataRow[maxRows];
		
		System.out.println("Length: "+ m_dataSets.length + ", " + Arrays.toString(m_dataSets));
		
		for (int i = 0; i < m_dataSets.length; i++) {
			if (m_dataSets[i].getType() == Hdf5DataType.INTEGER) {
				Hdf5DataSet<Integer> dataSet = (Hdf5DataSet<Integer>) m_dataSets[i];
				
				long[] dimensions = dataSet.getDimensions();
				pushFlowVariables(dataSet);
				
				Integer[] dataRead = dataSet.read(new Integer[(int) dataSet.numberOfValues()]);

				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int rc = 0; rc < rowNum * colNum; rc++) {
					System.out.println("Read Value " + i + "|" + rc + ": " + dataRead[rc]);
				}

				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : 0));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (m_dataSets[i].getType() == Hdf5DataType.LONG) {
				Hdf5DataSet<Long> dataSet = (Hdf5DataSet<Long>) m_dataSets[i];
			
				long[] dimensions = dataSet.getDimensions();
				pushFlowVariables(dataSet);
				
				Long[] dataRead = dataSet.read(new Long[(int) dataSet.numberOfValues()]);
	
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int rc = 0; rc < rowNum * colNum; rc++) {
					System.out.println("Read Value " + i + "|" + rc + ": " + dataRead[rc]);
				}
	
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : 0L));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (m_dataSets[i].getType() == Hdf5DataType.DOUBLE) {
				Hdf5DataSet<Double> dataSet = (Hdf5DataSet<Double>) m_dataSets[i];
			
				long[] dimensions = dataSet.getDimensions();
				pushFlowVariables(dataSet);
				
				Double[] dataRead = dataSet.read(new Double[(int) dataSet.numberOfValues()]);
	
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int rc = 0; rc < rowNum * colNum; rc++) {
					System.out.println("Read Value " + i + "|" + rc + ": " + dataRead[rc]);
				}
	
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : 0.0));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (m_dataSets[i].getType() == Hdf5DataType.STRING) {
				Hdf5DataSet<String> dataSet = (Hdf5DataSet<String>) m_dataSets[i];
			
				long[] dimensions = dataSet.getDimensions();
				pushFlowVariables(dataSet);
				
				String[] dataRead = dataSet.read(new String[(int) dataSet.numberOfValues()]);
	
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int rc = 0; rc < rowNum * colNum; rc++) {
					System.out.println("Read Value " + i + "|" + rc + ": " + dataRead[rc]);
				}
	
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : ""));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("DataType not available", new NullPointerException());
			}
		}

		for (DataRow row: rows) {
			outContainer.addRowToTable(row);
		}
		
		file.close();
        outContainer.close();
		
		return new BufferedDataTable[]{outContainer.getTable()};
	}
	
	private void pushFlowVariables(Hdf5DataSet<?> dataSet) {
		List<String> attrs = dataSet.loadAttributeNames();
		Iterator<String> iter = attrs.iterator();
		while (iter.hasNext()) {
			String name = iter.next();
			Hdf5Attribute<?> attr = dataSet.getAttribute(name);
			if (attr.getType() == Hdf5DataType.INTEGER) {
				for (int i = 0; i < attr.getValue().length; i++) {
					pushFlowVariableInt(attr.getName() + i, (Integer) attr.getValue()[i]);
				}
			} else if (attr.getType() == Hdf5DataType.DOUBLE) {
				for (int i = 0; i < attr.getValue().length; i++) {
					pushFlowVariableDouble(attr.getName() + i, (Double) attr.getValue()[i]);
				}
			} else if (attr.getType() == Hdf5DataType.STRING) {
				pushFlowVariableString(attr.getName(), (String) attr.getValue()[0]);
			}
		}
	}
	
	private DataTableSpec createOutSpec() {
		List<DataColumnSpec> colSpecList = new LinkedList<>();

		m_dataSets = HDF5ReaderNodeDialog.dsMap.keySet().toArray(new Hdf5DataSet<?>[] {});
		
		System.out.println("Datasets: " + Arrays.toString(m_dataSets));
		
		for (Hdf5DataSet<?> dataSet: m_dataSets) {
			Hdf5DataType dataType = dataSet.getType();
			DataType type = dataType.getColumnType();
			
			Iterator<String> iterIncl = HDF5ReaderNodeDialog.dsMap.get(dataSet).iterator();
			while (iterIncl.hasNext()) {
				colSpecList.add(new DataColumnSpecCreator(iterIncl.next(), type).createSpec());
			}
		}
		
		DataColumnSpec[] colSpecs = colSpecList.toArray(new DataColumnSpec[] {});
		System.out.println("Columns: " + Arrays.toString(colSpecs));
        return new DataTableSpec(colSpecs);
    }
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		// TODO here for Exceptions you know before using execute()
		return new DataTableSpec[]{createOutSpec()};
    }
	
    /** A new configuration to store the settings. Also enables the type filter.
     * @return ...
     */
    static final DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration("column-filter");
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
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void reset() {}
}

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
    
	private List<Hdf5DataSet<?>> dsList = new LinkedList<>();
	
	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		Hdf5DataSet<?>[] dataSets = dsList.toArray(new Hdf5DataSet<?>[dsList.size()]);
		
		if (dataSets.length == 0) {
			NodeLogger.getLogger("HDF5 Files").error("No dataSet to use", new NullPointerException());
			throw new NullPointerException();
		}
		
		Hdf5File file = Hdf5File.createFile(dataSets[0].getFilePath());
		BufferedDataContainer outContainer = exec.createDataContainer(createOutSpec());
		
		int maxRows = 0;
		for (Hdf5DataSet<?> dataSet: dataSets) {
			dataSet.open();
			NodeLogger.getLogger("HDF5 Files").info("Read dataSet " + dataSet.getPathFromFile() + "/" + dataSet.getName());
			
			int rowNum = (int) dataSet.getDimensions()[0];
			maxRows = rowNum > maxRows ? rowNum : maxRows;
		}
		DataRow[] rows = new DataRow[maxRows];
		
		for (int i = 0; i < dataSets.length; i++) {
			if (dataSets[i].getType() == Hdf5DataType.INTEGER) {
				Hdf5DataSet<Integer> dataSet = (Hdf5DataSet<Integer>) dataSets[i];
				pushFlowVariables(dataSet);
				Integer[] dataRead = dataSet.read(new Integer[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);

				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						//System.out.println("Read Integer in " + dataSet.getName() + ": (" + colIds[c] + ", " + r + ") = " + (r < rowNum ? dataRead[r * colNum + colIds[c]] : 0));
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : 0));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (dataSets[i].getType() == Hdf5DataType.LONG) {
				Hdf5DataSet<Long> dataSet = (Hdf5DataSet<Long>) dataSets[i];
				pushFlowVariables(dataSet);
				Long[] dataRead = dataSet.read(new Long[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);

				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						//System.out.println("Read Long in " + dataSet.getName() + ": (" + colIds[c] + ", " + r + ") = " + (r < rowNum ? dataRead[r * colNum + colIds[c]] : 0L));
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : 0L));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (dataSets[i].getType() == Hdf5DataType.DOUBLE) {
				Hdf5DataSet<Double> dataSet = (Hdf5DataSet<Double>) dataSets[i];
				pushFlowVariables(dataSet);
				Double[] dataRead = dataSet.read(new Double[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						//System.out.println("Read Double in " + dataSet.getName() + ": (" + colIds[c] + ", " + r + ") = " + (r < rowNum ? dataRead[r * colNum + colIds[c]] : 0.0));
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum ? dataRead[r * colNum + c] : 0.0));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (dataSets[i].getType() == Hdf5DataType.STRING) {
				Hdf5DataSet<String> dataSet = (Hdf5DataSet<String>) dataSets[i];
				pushFlowVariables(dataSet);
				String[] dataRead = dataSet.read(new String[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						//System.out.println("Read String in " + dataSet.getName() + ": (" + colIds[c] + ", " + r + ") = " + (r < rowNum ? dataRead[r * colNum + colIds[c]] : ""));
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
			if (attr != null) {
				int attrNum = attr.getValue().length;
				String path = dataSet.getPathWithoutFileName() + "/" + attr.getName();
				
				if (attr.getType() == Hdf5DataType.INTEGER) {
					for (int i = attrNum - 1; i >= 0; i--) {
						// TODO maybe only add dataSet name to attr name if the attr name would exist more than once
						pushFlowVariableInt(path + (attrNum == 1 ? "" : i), (Integer) attr.getValue()[i]);
					}
					
				} else if (attr.getType() == Hdf5DataType.DOUBLE) {
					for (int i = attrNum - 1; i >= 0; i--) {
						pushFlowVariableDouble(path + (attrNum == 1 ? "" : i), (Double) attr.getValue()[i]);
					}
					
				} else if (attr.getType() == Hdf5DataType.STRING) {
					pushFlowVariableString(path, (String) attr.getValue()[0]);
				}
			}
		}
	}
	
	private DataTableSpec createOutSpec() {
		List<DataColumnSpec> colSpecList = new LinkedList<>();

		Iterator<Hdf5DataSet<?>> iterDS = dsList.iterator();
		
		while (iterDS.hasNext()) {
			Hdf5DataSet<?> dataSet = iterDS.next();
			Hdf5DataType dataType = dataSet.getType();
			DataType type = dataType.getColumnType();
			String path = dataSet.getPathWithoutFileName();
			// TODO later only for 2-dimensional dataSets
			int colNum = (int) dataSet.numberOfValuesRange(1, dataSet.getDimensions().length);
			
			for (int i = 0; i < colNum; i++) {
				colSpecList.add(new DataColumnSpecCreator(path + i, type).createSpec());
			}
		}
		
		DataColumnSpec[] colSpecs = colSpecList.toArray(new DataColumnSpec[] {});
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
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		dsList.clear();
		if (settings.containsKey("filePath") && settings.containsKey("dataSets")) {
			String filePath = settings.getString("filePath");
			String[] dsPaths = settings.getStringArray("dataSets");
			
			System.out.println(filePath + ": " + Arrays.toString(dsPaths));
		
			Hdf5File file = Hdf5File.createFile(filePath);
			for (String path: dsPaths) {
				dsList.add(file.getDataSetByPath(path));
			}
		}
	}

	@Override
	protected void reset() {}
}

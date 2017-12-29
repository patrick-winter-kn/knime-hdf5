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
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class HDF5ReaderNodeModel extends NodeModel {
    
	private List<Hdf5DataSet<?>> m_dsList = new LinkedList<>();
	
	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		Hdf5DataSet<?>[] dataSets = m_dsList.toArray(new Hdf5DataSet<?>[m_dsList.size()]);
		
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
			if (dataSets[i].getType().isKnimeType(Hdf5KnimeDataType.INTEGER)) {
				Hdf5DataSet<Integer> dataSet = (Hdf5DataSet<Integer>) dataSets[i];
				pushFlowVariables(dataSet);
				Integer[] dataRead = dataSet.read(new Integer[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);

				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum, r < rowNum ? dataRead[r * colNum + c] : 0));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (dataSets[i].getType().isKnimeType(Hdf5KnimeDataType.LONG)) {
				Hdf5DataSet<Long> dataSet = (Hdf5DataSet<Long>) dataSets[i];
				pushFlowVariables(dataSet);
				Long[] dataRead = dataSet.read(new Long[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);

				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum, r < rowNum ? dataRead[r * colNum + c] : 0L));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (dataSets[i].getType().isKnimeType(Hdf5KnimeDataType.DOUBLE)) {
				Hdf5DataSet<Double> dataSet = (Hdf5DataSet<Double>) dataSets[i];
				pushFlowVariables(dataSet);
				Double[] dataRead = dataSet.read(new Double[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum, r < rowNum ? dataRead[r * colNum + c] : 0.0));
						rows[r] = (i == 0 && c == 0) ? row : new JoinedRow(rows[r], row);
					}
				}
			} else if (dataSets[i].getType().isKnimeType(Hdf5KnimeDataType.STRING)) {
				Hdf5DataSet<String> dataSet = (Hdf5DataSet<String>) dataSets[i];
				pushFlowVariables(dataSet);
				String[] dataRead = dataSet.read(new String[(int) dataSet.numberOfValues()]);

				long[] dimensions = dataSet.getDimensions();
				int rowNum = (int) dimensions[0];
				int colNum = (int) dataSet.numberOfValuesRange(1, dimensions.length);
				
				for (int c = 0; c < colNum; c++) {
					for (int r = 0; r < maxRows; r++) {
						DefaultRow row = new DefaultRow("Row" + r, dataSet.getDataCell(r < rowNum, r < rowNum ? dataRead[r * colNum + c] : ""));
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
		Iterator<String> iter = dataSet.loadAttributeNames().iterator();
		
		while (iter.hasNext()) {
			String name = iter.next();
			Hdf5Attribute<?> attr = dataSet.getAttribute(name);
			
			if (attr != null) {
				int attrNum = attr.getValue().length;
				String path = dataSet.getPathFromFile() + dataSet.getName() + "/" + attr.getName();
				
				if (attrNum == 1) {
					if (attr.getType().isKnimeType(Hdf5KnimeDataType.INTEGER)) {
						// TODO maybe only add dataSet name to attr name if the attr name would exist more than once
						pushFlowVariableInt(path, (Integer) attr.getValue()[0]);
						
					} else if (attr.getType().isKnimeType(Hdf5KnimeDataType.DOUBLE)) {
						pushFlowVariableDouble(path, (Double) attr.getValue()[0]);
						
					} else if (attr.getType().isKnimeType(Hdf5KnimeDataType.STRING)) {
						pushFlowVariableString(path, "" + attr.getValue()[0]);
					}
				} else {
					pushFlowVariableString(path, Arrays.toString(attr.getValue()) + " ("
							+ attr.getType().getKnimeType().toString() + ")");
				}
			}
		}
	}
	
	private DataTableSpec createOutSpec() {
		List<DataColumnSpec> colSpecList = new LinkedList<>();

		Iterator<Hdf5DataSet<?>> iterDS = m_dsList.iterator();
		
		while (iterDS.hasNext()) {
			Hdf5DataSet<?> dataSet = iterDS.next();
			Hdf5KnimeDataType dataType = dataSet.getType().getKnimeType();
			DataType type = dataType.getColumnType();
			String pathWithName = dataSet.getPathFromFile() + dataSet.getName();
			// TODO later only for 2-dimensional dataSets
			int colNum = (int) dataSet.numberOfValuesRange(1, dataSet.getDimensions().length);
			
			for (int i = 0; i < colNum; i++) {
				colSpecList.add(new DataColumnSpecCreator(pathWithName + i, type).createSpec());
			}
		}
		
		DataColumnSpec[] colSpecs = colSpecList.toArray(new DataColumnSpec[] {});
		DataTableSpec tableSpec = new DataTableSpec(colSpecs);
        return tableSpec;
    }
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		// TODO here for Exceptions you know before using execute()
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
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_dsList.clear();
		if (settings.containsKey("filePath") && settings.containsKey("dataSets")) {
			String filePath = settings.getString("filePath");
			String[] dsPaths = settings.getStringArray("dataSets");
			
			System.out.println(filePath + ": " + Arrays.toString(dsPaths));
		
			Hdf5File file = Hdf5File.createFile(filePath);
			for (String path: dsPaths) {
				m_dsList.add(file.getDataSetByPath(path));
			}
		}
	}

	@Override
	protected void reset() {}
}

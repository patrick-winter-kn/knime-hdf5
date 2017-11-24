package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.JoinedRow;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5DataType;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;

public class HDF5ReaderNodeModel extends NodeModel {

    static String fname = "Data" + File.separator + "dataTypes.h5";
    public static Hdf5File file;
    
    // TODO File separator '/' everywhere (opp system) the same?
    static String dspath = "/String/";
    private Hdf5Group group;
    
    static String dsname = "withoutAttr";
    private Hdf5DataType dataType;
    
    private long[] dimensions = {4, 4};
    
    Hdf5DataSet<?>[] dataSets;
    
	protected HDF5ReaderNodeModel() {
		super(0, 1);
		dataSets = HDF5ReaderNodeDialog.dsMap.keySet().toArray(new Hdf5DataSet<?>[] {});
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {

		if (dataSets.length == 0) {
			throw new NullPointerException("No dataSet to use");
		}
		
		file = Hdf5File.createFile(dataSets[0].getFilePath());
		dataSets[0].open();
		
		// TODO make one table out of the dataSet array
		
		DataTableSpec outSpec = createOutSpec();
		BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
		
		BufferedDataTable[] data = null;
		if (dataType == Hdf5DataType.INTEGER) {
			Hdf5DataSet<Integer> dataSet = (Hdf5DataSet<Integer>) group.getDataSet(dsname);
			dimensions = dataSet.getDimensions();
			for (int i = 0; i < dimensions.length; i++) {
				System.out.println("dimensions "+ i + ": " + dimensions[i]);
			}
			pushFlowVariables(dataSet);
			
			Integer[] dataRead = dataSet.read(new Integer[(int) dataSet.numberOfValues()]);
			file.close();
			
			for (int i = 0; i < dataSet.numberOfValues(); i++) {
				System.out.println("Read Integer " + i + ": " + dataRead[i]);
			}
	
			for (int i = 0; i < dimensions[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new IntCell(dataRead[(int) (dimensions[1] * i)]));
				for (int j = 1; j < dimensions[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new IntCell(dataRead[(int) dimensions[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		} else if (dataType == Hdf5DataType.LONG) {
			Hdf5DataSet<Long> dataSet = (Hdf5DataSet<Long>) group.getDataSet(dsname);
			dimensions = dataSet.getDimensions();
			pushFlowVariables(dataSet);
			
			Long[] dataRead = dataSet.read(new Long[(int) dataSet.numberOfValues()]);
			file.close();
			
			for (int i = 0; i < dataSet.numberOfValues(); i++) {
				System.out.println("Read Long " + i + ": " + dataRead[i]);
			}
	
			for (int i = 0; i < dimensions[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new LongCell(dataRead[(int) (dimensions[1] * i)]));
				for (int j = 1; j < dimensions[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new LongCell(dataRead[(int) dimensions[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		} else if (dataType == Hdf5DataType.DOUBLE) {
			Hdf5DataSet<Double> dataSet = (Hdf5DataSet<Double>) group.getDataSet(dsname);
			dimensions = dataSet.getDimensions();
			pushFlowVariables(dataSet);
			
			Double[] dataRead = dataSet.read(new Double[(int) dataSet.numberOfValues()]);
			file.close();
			
			for (int i = 0; i < dataSet.numberOfValues(); i++) {
				System.out.println("Read Double " + i + ": " + dataRead[i]);
			}
	
			for (int i = 0; i < dimensions[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new DoubleCell(dataRead[(int) (dimensions[1] * i)]));
				for (int j = 1; j < dimensions[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new DoubleCell(dataRead[(int) dimensions[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		} else if (dataType == Hdf5DataType.STRING) {
			Hdf5DataSet<String> dataSet = (Hdf5DataSet<String>) group.getDataSet(dsname);
			dimensions = dataSet.getDimensions();
			pushFlowVariables(dataSet);
			
			String[] dataRead = dataSet.read(new String[(int) dataSet.numberOfValues()]);
			file.close();

			for (int i = 0; i < dimensions[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new StringCell(dataRead[(int) (dimensions[1] * i)]));
				for (int j = 1; j < dimensions[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new StringCell(dataRead[(int) dimensions[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		}
		
		return data;
	}
	
	private void pushFlowVariables(Hdf5DataSet<?> dataSet) {
		List<String> attrs = dataSet.loadAttributeNames();
		Iterator<String> iter = attrs.iterator();
		while (iter.hasNext()) {
			String name = iter.next();
			Hdf5Attribute<?> attr = dataSet.getAttribute(name);
			System.out.println("Attr " + attr.getName() + ":");
			for (Object val: attr.getValue()) {
				System.out.print(val + " ");
			}
			System.out.print("\n");
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
		
		for (Hdf5DataSet<?> dataSet: dataSets) {
			Hdf5DataType dataType = dataSet.getType();
			DataType type = dataType.getColumnType();
			
			Iterator<String> iterIncl = HDF5ReaderNodeDialog.dsMap.get(dataSet).iterator();
			while (iterIncl.hasNext()) {
				colSpecList.add(new DataColumnSpecCreator(iterIncl.next(), type).createSpec());
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
    static final  DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
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

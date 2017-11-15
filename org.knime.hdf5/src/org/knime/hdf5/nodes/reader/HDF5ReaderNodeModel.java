package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
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
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
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
    
	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {

		NodeLogger.getLogger("HDF5 Files").info("Complete path of the dataSet: " + fname + dspath + dsname);
		

		file = Hdf5File.createFile(fname);
		/* TODO also do it for deeper group structure using
		 * 
		 * 	String[] pathTokens = dspath.split("/");
			Hdf5Group[] groups = new Hdf5Group[Math.max(pathTokens.length, 1)];
			groups[0] = file;
			for (int i = 1; i < groups.length; i++) {
				System.out.println(pathTokens[i]);
				groups[i] = groups[i-1].getGroup(pathTokens[i]);
			}
		 */
		group = file.getGroup(dspath.substring(1, dspath.length() - 1));
		
		DataTableSpec outSpec = createOutSpec();
		BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
		
		BufferedDataTable[] data = null;
		if (dataType == Hdf5DataType.INTEGER) {
			// TODO for 0 give a constant in Hdf5DataType
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
		// TODO will result in a failure when once already called with 1 and later again with 4
		DataColumnSpec[] colSpecs = new DataColumnSpec[(int) dimensions[1]];
		for (int i = 0; i < colSpecs.length; i++) {
			switch (dataType.getTypeId()) {
			case 0:
				colSpecs[i] = new DataColumnSpecCreator("Column " + i, IntCell.TYPE).createSpec();
				break;
			case 1:
				colSpecs[i] = new DataColumnSpecCreator("Column " + i, LongCell.TYPE).createSpec();
				break;
			case 2:
				colSpecs[i] = new DataColumnSpecCreator("Column " + i, DoubleCell.TYPE).createSpec();
				break;
			case 3:
				colSpecs[i] = new DataColumnSpecCreator("Column " + i, StringCell.TYPE).createSpec();
				break;
	        }
		}
        return new DataTableSpec(colSpecs);
    }
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		// TODO here for Exceptions you know before using execute()
		file = Hdf5File.createFile(fname);
		group = file.getGroup(dspath.substring(1, dspath.length() - 1));
		if (group.existsDataSet(dsname)) {
			dataType = group.findDataSetType(dsname);
		} else {
			if (dspath.contains("Int")) {
				dataType = Hdf5DataType.INTEGER;
			} else if (dspath.contains("Long")) {
				dataType = Hdf5DataType.LONG;
			} else if (dspath.contains("Double")) {
				dataType = Hdf5DataType.DOUBLE;
			} else {
				dataType = Hdf5DataType.STRING;
			}	
		}
		file.close();
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
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void reset() {}
}

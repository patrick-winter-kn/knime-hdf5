package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
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
import org.knime.hdf5.lib.Hdf5OverwritePolicy;
import org.knime.hdf5.lib.Hdf5TreeElement;

public class HDF5ReaderNodeModel extends NodeModel {

    String fname = "Data" + File.separator + "dataTypes.h5";
    private Hdf5File file;
    
    // TODO File separator '/' everywhere (opp system) the same?
    String dspath = "/String/";
    private Hdf5Group group;
    
    String dsname = "withoutAttr";
    private Hdf5DataType dataType;
    
    //stringLength = 7, +1 for the null terminator
    //private static long[] dims2D = { 2, 2, 2, 7+1 };
    private long[] dims2D = { 3, 4, 3+1 };
    
    private boolean first = true;

    private Hdf5OverwritePolicy groupOP = Hdf5OverwritePolicy.ABORT;
    private Hdf5OverwritePolicy dataSetOP = Hdf5OverwritePolicy.KEEP_BOTH;
    
    
	protected HDF5ReaderNodeModel() {
		super(0, 1);
		file = Hdf5File.createFile(fname);
		group = file.createGroup(dspath.substring(1, dspath.length() - 1), groupOP);
		if (group.existsDataSet(dsname)) {
			dataType = group.findDataSetType(dsname);
		} else {
			if (dspath.contains("Integer")) {
				dataType = Hdf5DataType.INTEGER;
			} else if (dspath.contains("Long")) {
				dataType = Hdf5DataType.LONG;
			} else if (dspath.contains("Double")) {
				dataType = Hdf5DataType.DOUBLE;
			} else {
				dataType = Hdf5DataType.STRING;
			}	
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		
		NodeLogger.getLogger("HDF5 Files").info("Complete path of the dataSet: " + fname + dspath + dsname);
		
		if (!first) {
			file = Hdf5File.createFile(fname);
			group = file.createGroup(dspath.substring(1, dspath.length() - 1), groupOP);
			if (group.existsDataSet(dsname)) {
				dataType = group.findDataSetType(dsname);
			} else {
				if (dspath.contains("Integer")) {
					dataType = Hdf5DataType.INTEGER;
				} else if (dspath.contains("Long")) {
					dataType = Hdf5DataType.LONG;
				} else if (dspath.contains("Double")) {
					dataType = Hdf5DataType.DOUBLE;
				} else {
					dataType = Hdf5DataType.STRING;
				}	
			}
		} else {
			first = false;
		}
		
		//createIntFile();
		//createStringFile();
		//discoverFile();
		
		BufferedDataTable[] data = null;
		if (dataType == Hdf5DataType.INTEGER) {
			Hdf5DataSet<Integer> dataset = (Hdf5DataSet<Integer>) group.createDataSet(dsname,
					new long[]{3, 4}, dataType, dataSetOP);
			Integer[] dataRead = dataset.read(new Integer[(int) dataset.numberOfValues()]);
			file.close();
			
			for (int i = 0; i < 3*4; i++) {
				System.out.println("Read Integer " + i + ": " + dataRead[i]);
			}
			DataTableSpec outSpec = createOutSpec();
			BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
	
			for (int i = 0; i < dims2D[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new IntCell(dataRead[(int) (dims2D[1] * i)]));
				for (int j = 1; j < dims2D[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new IntCell(dataRead[(int) dims2D[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		} else if (dataType == Hdf5DataType.LONG) {
			Hdf5DataSet<Long> dataset = (Hdf5DataSet<Long>) group.createDataSet(dsname,
					new long[]{3, 4}, dataType, dataSetOP);
			Long[] dataRead = dataset.read(new Long[(int) dataset.numberOfValues()]);
			file.close();
			
			for (int i = 0; i < 3*4; i++) {
				System.out.println("Read Long " + i + ": " + dataRead[i]);
			}
			DataTableSpec outSpec = createOutSpec();
			BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
	
			for (int i = 0; i < dims2D[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new LongCell(dataRead[(int) (dims2D[1] * i)]));
				for (int j = 1; j < dims2D[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new LongCell(dataRead[(int) dims2D[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		} else if (dataType == Hdf5DataType.DOUBLE) {
			Hdf5DataSet<Double> dataset = (Hdf5DataSet<Double>) group.createDataSet(dsname,
					new long[]{3, 4}, dataType, dataSetOP);
			Double[] dataRead = dataset.read(new Double[(int) dataset.numberOfValues()]);
			file.close();
			
			for (int i = 0; i < 3*4; i++) {
				System.out.println("Read Double " + i + ": " + dataRead[i]);
			}
			DataTableSpec outSpec = createOutSpec();
			BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
	
			for (int i = 0; i < dims2D[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new DoubleCell(dataRead[(int) (dims2D[1] * i)]));
				for (int j = 1; j < dims2D[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new DoubleCell(dataRead[(int) dims2D[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		} else if (dataType == Hdf5DataType.STRING) {
			Hdf5DataSet<Byte> dataset = (Hdf5DataSet<Byte>) group.createDataSet(dsname,
					new long[]{3, 4, 4}, dataType, dataSetOP);
			Byte[] dread = dataset.read(new Byte[(int) dataset.numberOfValues()]);
			file.close();
			
		    String[] dataRead = new String[3*4];
			for (int i = 0; i < 3*4; i++) {
				dataRead[i] = String.copyValueOf(new char[]{(char) (byte) dread[4*i], (char) (byte) dread[4*i + 1], (char) (byte) dread[4*i + 2]});
				System.out.println("Read String " + i + ": " + dataRead[i]);
			}
			DataTableSpec outSpec = createOutSpec();
			BufferedDataContainer outContainer = exec.createDataContainer(outSpec);

			for (int i = 0; i < dims2D[0]; i++) {
				DefaultRow row = null;
				JoinedRow jrow = null;
				row = new DefaultRow("Row " + i, new StringCell(dataRead[(int) (dims2D[1] * i)]));
				for (int j = 1; j < dims2D[1]; j++) {
					DefaultRow newRow = new DefaultRow("Row " + i, new StringCell(dataRead[(int) dims2D[1] * i + j]));
					jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
				}
				outContainer.addRowToTable(jrow);
			}
	        outContainer.close();
	        data = new BufferedDataTable[]{outContainer.getTable()};
		}
		
		return data;
	}
	
	@SuppressWarnings("unchecked")
	public void createIntFile() {
		Hdf5File file = Hdf5File.createFile(fname);
		Hdf5Group group = file.createGroup("group", Hdf5OverwritePolicy.ABORT);
		Hdf5DataSet<Integer> dataSet = (Hdf5DataSet<Integer>) group.createDataSet(dsname, dims2D,
				Hdf5DataType.INTEGER, Hdf5OverwritePolicy.ABORT);
		
		Integer[] dataRead = new Integer[7 * 4];
		for (int i = 0; i < dataRead.length; i++) {
			dataRead[i] = 2 * i;
		}		
		dataSet.write(dataRead);
		
		file.close();
		
		//HDF5ReaderNodeModel.dataRead = dataRead;
	}
	
	@SuppressWarnings("unchecked")
	private void createStringFile() {
		StringBuffer[][][] str_data = { { {new StringBuffer("A"), new StringBuffer("BB")},
				{new StringBuffer("CCC"), new StringBuffer("DDDD")} } , 
				{ {new StringBuffer("EEEEE"), new StringBuffer("FFFFFF")},
					{new StringBuffer("GGGGGGG"), new StringBuffer("HHHHHHHH")} } };

		Hdf5File file = Hdf5File.createFile(fname);
		String[] pathTokens = dspath.split("/");
		Hdf5Group[] groups = new Hdf5Group[Math.max(pathTokens.length, 1)];
		groups[0] = file;
		for (int i = 1; i < groups.length; i++) {
			System.out.println(pathTokens[i]);
			groups[i] = groups[i-1].createGroup(pathTokens[i], Hdf5OverwritePolicy.ABORT);
		}
		Hdf5DataSet<Byte> dataSet = (Hdf5DataSet<Byte>) groups[groups.length-1].createDataSet(dsname, dims2D,
				Hdf5DataType.STRING, Hdf5OverwritePolicy.ABORT);
		
		long SDIM = dataSet.getStringLength() + 1;
		Byte[] dstr = new Byte[(int) dataSet.numberOfValues()];

		// Write the data to the dataset.
		for (int i = 0; i < dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1); i++) {
			// TODO write it with dimensions from Hdf5DataSet
			StringBuffer buf = str_data[(i / (int) dims2D[2]) / (int) dims2D[1]][(i / (int) dims2D[2]) % (int) dims2D[1]][i % (int) dims2D[2]];
			for (int j = 0; j < SDIM; j++) {
				if (j < buf.length()) {
					dstr[i * (int) SDIM + j] = (Byte) (byte) buf.charAt(j);
				} else {
					dstr[i * (int) SDIM + j] = 0;
				}
			}
		}
		dataSet.write(dstr);

		// Allocate space for data.
		Byte[] dataReadByte = new Byte[(int) dataSet.numberOfValues()];

		// Read data.
		dataReadByte = dataSet.read(dataReadByte);
		
		// just for testing
		for (int i = -1; i < Math.min(dataReadByte.length, 20); i++) {
			System.out.println(i == -1 ? ("Length: " + dataReadByte.length) : (i + ": " + dataReadByte[i]));
		}
		
		String[] dataRead = new String[(int) dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1)];
		char[] tempbuf = new char[(int) SDIM];
		for (int i = 0; i < dataReadByte.length; i++) {
			tempbuf[i % (int)SDIM] = (char) (byte) dataReadByte[i];
			if ((i+1) % (int)SDIM == 0) {
				dataRead[i / (int)SDIM] = String.copyValueOf(tempbuf);
			}
		}
		
		// Output the data to the screen.
		for (int i = 0; i < (int) dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1); i++) {
			System.out.println(dsname + "[" + i + "]: " + dataRead[i]);
		}
        
		addDoubleAttribute(file);
		if (groups.length >= 2) {
			addStringAttribute(groups[1]);
		}
		addStringAttribute(dataSet);
		
		file.close();
		
		//HDF5ReaderNodeModel.dataRead = dataRead;
    }
	
	private void discoverFile() {
		Hdf5File file = Hdf5File.createFile(fname);
		discoverGroup(file, 0);
		file.close();
	}
	
	private void discoverGroup(Hdf5Group group, int depth) {
		String space = "";
		for (int i = 0; i < depth; i++) {
			space = "\t" + space;
		}
		System.out.println(space + ((depth == 0) ? "File: " : "Group: ") + group.getName() + ", Path: " + group.getPathFromFile());
		
		List<String> groupNames = group.loadGroupNames();
		if (groupNames != null) {
			for (String name: groupNames) {
				discoverGroup(group.createGroup(name, Hdf5OverwritePolicy.ABORT), depth + 1);
			}
		}
		
		List<String> dataSetNames = group.loadDataSetNames();
		if (dataSetNames != null) {
			for (String name: dataSetNames) {
				System.out.println(space + "\tDataset: " + name + ", Path: " + group.getPathFromFile() + name);
				if (dspath.equals(group.getPathFromFile()) && dsname.equals(name)) {
					// TODO there could be an error if the actual dataset isn't with string values
					@SuppressWarnings("unchecked")
					Hdf5DataSet<Byte> ds = (Hdf5DataSet<Byte>) group.getDataSet(name, new long[2]);
					Byte[] dataReadByte = ds.read(new Byte[(int) ds.numberOfValues()]);
					
					System.out.println();
					
					// just for testing
					for (int i = -1; i < Math.min(dataReadByte.length, 20); i++) {
						System.out.println(i == -1 ? ("Length: " + dataReadByte.length) : (i + ": " + dataReadByte[i]));
					}
					
					String[] dataRead = new String[(int) ds.numberOfValuesRange(0, ds.getDimensions().length - 1)];
					long SDIM = ds.getStringLength() + 1;
					char[] tempbuf = new char[(int) SDIM];
					for (int i = 0; i < dataReadByte.length; i++) {
						tempbuf[i % (int)SDIM] = (char) (byte) dataReadByte[i];
						if ((i+1) % (int)SDIM == 0) {
							dataRead[i / (int)SDIM] = String.copyValueOf(tempbuf);
						}
					}
					
					// Output the data to the screen.
					for (int i = 0; i < (int) ds.numberOfValuesRange(0, ds.getDimensions().length - 1); i++) {
						System.out.println(dsname + "[" + i + "]: " + dataRead[i]);
					}
					
			        System.out.println("\n\nStringCell 0 1 1: " + ds.getStringCell(dataReadByte, 0, 1, 1));
			        System.out.println("\n\nCell 0 1 1 3: " + ds.getCell(dataReadByte, 0, 1, 1, 3));
			        
					//HDF5ReaderNodeModel.dataRead = dataRead;
				}
			}
		}
	}
	
	public static void addDoubleAttribute(Hdf5TreeElement treeElement) {

        String attrname  = "double2";
        Double[] attrValue = { 3.345, 23.243, 54.354 };

        Hdf5Attribute<Double> attribute = new Hdf5Attribute<>(attrname, attrValue);
        treeElement.addAttribute(attribute);
        
        long DIM0 = attribute.getDimension();
        Double[] attrData = attribute.read(new Double[(int) DIM0]);
        
        // print out attribute value
        System.out.println("\n\n" + attrname);
        System.out.println("SDIM Attribute: " + DIM0);
        for (Double d: attrData) {
            System.out.println("Zahl: " + d);
        }

        attribute.close();
	}

	public static void addStringAttribute(Hdf5TreeElement treeElement) {
        String attrname  = "numbers3";
        long stringLength = 8;
		StringBuffer str_data = new StringBuffer("ABCDEFGHIJ");

		Byte[] attrValue = new Byte[(int) stringLength];

		// Write the data to the dataset.
		for (int i = 0; i < stringLength; i++) {
			if (i < str_data.length()) {
				attrValue[i] = (Byte) (byte) str_data.charAt(i);
			} else {
				attrValue[i] = 0;
			}
		}
		
        Hdf5Attribute<Byte> attribute = new Hdf5Attribute<>(attrname, attrValue);
        treeElement.addAttribute(attribute);

		// Read data.
		String dataRead;
		Byte[] dataReadByte = attribute.read(new Byte[(int) stringLength]);		
		char[] tempbuf = new char[(int) stringLength];
		for (int i = 0; i < dataReadByte.length; i++) {
			tempbuf[i] = (char) (byte) dataReadByte[i];
		}
		dataRead = String.copyValueOf(tempbuf);
		
 		System.out.println(attrname + ": " + dataRead + "\n");
        
        attribute.close();
	}
	
	private DataTableSpec createOutSpec() {
		DataColumnSpec[] colSpecs = new DataColumnSpec[(int) dims2D[1]];
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
        DataTableSpec spec = new DataTableSpec(colSpecs);
        return spec;
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
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void reset() {}
}

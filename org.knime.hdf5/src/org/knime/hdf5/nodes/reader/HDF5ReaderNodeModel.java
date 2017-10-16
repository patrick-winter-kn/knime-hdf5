package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.data.DataColumnProperties;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.JoinedRow;
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
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5DataSet.Hdf5DataType;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;

/* what's possible so far:
 * HDF5 - Java:		1acd
 * 					1b		(maybe add another class Hdf5Path to access the groups along this path)
 * 					2ab
 * KNIME - Java:	
 * 	
 * what's possible so far, but can be done differently/better:
 * HDF5 - Java:		2c		(TODO automatically recognize the datatype of the dataset)
 * 					2d 		(TODO not possible to see a list of all attributes in a treeElement)
 * KNIME - Java:	1c		(TODO not possible to write datasets with more than 2 dimensions or variable datatypes)
 * 
 * what's not possible so far:
 * HDF5 - Java:		3abcd	(maybe also not necessary)
 * KNIME - Java:	1ab		(not necessary)
 * 					1d		(TODO still missing)
 * 					2abcd	(TODO the whole thing with reading KNIME datasets in Java is still missing)
 * 					3abcd	(maybe also not necessary)
 * 
 * abbreviations: (A = (HDF5|KNIME))
 * 1 - write something in A with methods from Java
 * 2 - read something in Java which is already available in A
 * 3 - delete something in A with methods from Java
 * 
 * something:
 * a - file
 * b - a group which isn't a file
 * c - dataset
 * d - attribute
 */

public class HDF5ReaderNodeModel extends NodeModel {

    static String fname  = "Data" + File.separator + "example002.h5";
    // TODO File separator '/' everywhere (opp system) the same?
    static String dspath  = "/tests/stringTests/second/";
    static String dsname  = "stringDS";
    //stringLength = 7, +1 for the null terminator
    private static long[] dims2D = { 2, 2, 2, 7+1 };
    //private static long[] dims2D = { 7, 4 };
    private static String[] dataRead;
    //private static Integer[] dataRead;

	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		
		System.out.println("Complete path: " + fname + dspath + dsname);
		
		//createIntFile();
		createStringFile();
		//discoverFile();
		
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
		return new BufferedDataTable[]{outContainer.getTable()};
	}
	
	public static void createIntFile() {
		Hdf5File file = Hdf5File.createInstance(fname);
		Hdf5Group group = Hdf5Group.createInstance(file, "group");
		Hdf5DataSet<Integer> dataSet = (Hdf5DataSet<Integer>) Hdf5DataSet.createInstance(group, dsname, dims2D, Hdf5DataType.INTEGER);
		
		Integer[] dataRead = new Integer[7 * 4];
		for (int i = 0; i < dataRead.length; i++) {
			dataRead[i] = 2 * i;
		}		
		dataSet.write(dataRead);
		
		file.closeAll();
		
		//HDF5ReaderNodeModel.dataRead = dataRead;
	}
	
	private static void createStringFile() {
		StringBuffer[][][] str_data = { { {new StringBuffer("A"), new StringBuffer("BB")},
				{new StringBuffer("CCC"), new StringBuffer("DDDD")} } , 
				{ {new StringBuffer("EEEEE"), new StringBuffer("FFFFFF")},
					{new StringBuffer("GGGGGGG"), new StringBuffer("HHHHHHHH")} } };

		Hdf5File file = Hdf5File.createInstance(fname);
		String[] pathTokens = dspath.split("/");
		Hdf5Group[] groups = new Hdf5Group[Math.max(pathTokens.length, 1)];
		groups[0] = file;
		for (int i = 1; i < groups.length; i++) {
			System.out.println(pathTokens[i]);
			Hdf5Group group = Hdf5Group.createInstance(groups[i-1], pathTokens[i]);
			groups[i] = group;
		}
		
		Hdf5DataSet<Byte> dataSet = (Hdf5DataSet<Byte>) Hdf5DataSet.createInstance(groups[groups.length-1], dsname, dims2D, Hdf5DataType.STRING);
		
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
		

		System.out.println();
		
		for (Byte b: dataReadByte) {
			System.out.println(b);
		}
		
		String[] dataRead = new String[(int) dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1)];
		char[] tempbuf = new char[(int) SDIM];
		for (int i = 0; i < dataReadByte.length; i++) {
			tempbuf[i%(int) SDIM] = (char) (byte) dataReadByte[i];
			if ((i+1) % (int) SDIM == 0) {
				dataRead[i/(int) SDIM] = String.copyValueOf(tempbuf);
			}
		}
		
		// Output the data to the screen.
		for (int i = 0; i < (int) dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1); i++) {
			System.out.println(dsname + " [" + i + "]: " + dataRead[i]);
		}
		
        System.out.println("\n\nStringCell 0 1 1: " + dataSet.getStringCell(dataReadByte, 0, 1, 1));
        System.out.println("\n\nCell 0 1 1 3: " + dataSet.getCell(dataReadByte, 0, 1, 1, 3));
        
		addDoubleAttribute(file);
		if (groups.length >= 2) {
			addStringAttribute(groups[1]);
		}
		addStringAttribute(dataSet);
		
		file.closeAll();
		
		HDF5ReaderNodeModel.dataRead = dataRead;
    }
	
	private static void discoverFile() {
		Hdf5File file = Hdf5File.createInstance(fname);
		discoverGroup(file, 0);
		file.closeAll();
	}
	
	private static void discoverGroup(Hdf5Group group, int depth) {
		String space = "";
		for (int i = 0; i < depth; i++) {
			space = "\t" + space;
		}
		System.out.println(space + ((depth == 0) ? "File: " : "Group: ") + group.getName() + ", Path: " + group.getPathFromFile());
		
		Hdf5Group[] groups = group.loadGroups();
		if (groups != null) {
			for (Hdf5Group grp: groups) {
				discoverGroup(grp, depth + 1);
			}
		}
		
		Hdf5DataSet<Byte>[] dataSets = (Hdf5DataSet<Byte>[]) group.loadDataSets(Hdf5DataType.STRING);
		if (dataSets != null) {
			for (Hdf5DataSet<Byte> ds: dataSets) {
				System.out.println(space + "\tDataset: " + ds.getName() + ", Path: " + ds.getPathFromFile());
				if (dspath.equals(group.getPathFromFile()) && dsname.equals(ds.getName())) {
					Byte[] dataReadByte = ds.read(new Byte[(int) ds.numberOfValues()]);
					
					System.out.println();
					
					for (Byte b: dataReadByte) {
						System.out.println(b);
					}
					
					String[] dataRead = new String[(int) ds.numberOfValuesRange(0, ds.getDimensions().length - 1)];
					long SDIM = ds.getStringLength() + 1;
					char[] tempbuf = new char[(int) SDIM];
					for (int i = 0; i < dataReadByte.length; i++) {
						tempbuf[i%(int) SDIM] = (char) (byte) dataReadByte[i];
						if ((i+1) % (int) SDIM == 0) {
							dataRead[i/(int) SDIM] = String.copyValueOf(tempbuf);
						}
					}
					
					// Output the data to the screen.
					for (int i = 0; i < (int) ds.numberOfValuesRange(0, ds.getDimensions().length - 1); i++) {
						System.out.println(dsname + " [" + i + "]: " + dataRead[i]);
					}
					
			        System.out.println("\n\nStringCell 0 1 1: " + ds.getStringCell(dataReadByte, 0, 1, 1));
			        System.out.println("\n\nCell 0 1 1 3: " + ds.getCell(dataReadByte, 0, 1, 1, 3));
			        
					HDF5ReaderNodeModel.dataRead = dataRead;
				}
			}
		}
	}
	
	public static void addDoubleAttribute(Hdf5TreeElement treeElement) {

        String attrname  = "double2";
        Double[] attrValue = { 3.345, 23.243, 54.354 };

        Hdf5Attribute<Double> attribute = new Hdf5Attribute<>(attrname, attrValue);
        long DIM0 = attribute.getDimension();
        
        System.out.println("\n\naddToTreeElement: " + attribute.writeToTreeElement(treeElement));
        System.out.println("ds: " + attribute.getDataspace_id() + "\na: " + attribute.getAttribute_id());

        Double[] attrData = attribute.read(new Double[(int) DIM0]);
        
        // print out attribute value
        System.out.println("\n\n" + attrname);
        System.out.println("SDIM Attribute: " + DIM0);
        for (Double d: attrData) {
            System.out.println("Zahl : " + d);
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
		
        System.out.println("\n\naddToTreeElement: " + attribute.writeToTreeElement(treeElement));

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
        Map<String, String> map = new HashMap<>();
        map.put("Platz_01", "BVB");
        map.put("Platz_02", "Bayern");
        map.put("Platz_03", "Hoffenheim");
        DataColumnProperties pro = new DataColumnProperties(map);
		for (int i = 0; i < colSpecs.length; i++) {
	        colSpecs[i] = new DataColumnSpecCreator("Column " + i, StringCell.TYPE).createSpec();
		}
        DataTableSpec spec = new DataTableSpec(colSpecs);
        return spec;
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
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {}

	@Override
	protected void reset() {}
}

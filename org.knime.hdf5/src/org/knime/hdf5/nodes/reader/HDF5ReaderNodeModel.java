package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

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
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

/* what's possible so far:
 * HDF5 - Java:		1acd
 * 					1b		(maybe add another class Hdf5Path to access the groups along this path)
 * KNIME - Java:	
 * 	
 * what's possible so far, but can be done differently/better:
 * HDF5 - Java:		2abcd 	(TODO not possible to see a list of all groups in the file
 * 							 where you don't know the name)
 * KNIME - Java:	1c		(TODO not possible to write datasets with more than 2 dimensions)
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
	
    private static String fname  = "Data" + File.separator + "example_new2.h5";
    // TODO File separator '/' everywhere (opp system) the same?
    private static String dspath  = "tests/groupTests/first";
    private static String dsname  = "stringTest";
    private static long[] dims2D = { 2, 2, 2, 8 }; //stringLength = 7 (= 8 - 1), 1 for the null terminator

	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		
		String[] dataRead = createFile();
        
		DataTableSpec outSpec = createOutSpec();
		BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
		
		System.out.println("\n");
		for (int i = 0; i < dims2D[0]; i++) {
			DefaultRow row = null;
			JoinedRow jrow = null;
			System.out.println(i + ": " + dataRead.length);
			row = new DefaultRow("Row " + i, new StringCell(dataRead[(int) (dims2D[1] * i)]));
			for (int j = 1; j < dims2D[1]; j++) {
				DefaultRow newRow = new DefaultRow("Row " + i, new StringCell(dataRead[(int) dims2D[1] * i + j]));
				jrow = (j == 1) ? new JoinedRow(row, newRow) : new JoinedRow(jrow, newRow);
			}
			outContainer.addRowToTable(jrow);
		}
		System.out.println(File.separator);
        outContainer.close();
		return new BufferedDataTable[]{outContainer.getTable()};
	}
	
	enum H5G_object {
		H5G_UNKNOWN(-1), // Unknown object type
		H5G_GROUP(0), // Object is a group
		H5G_DATASET(1), // Object is a dataset
		H5G_TYPE(2), // Object is a named data type
		H5G_LINK(3), // Object is a symbolic link
		H5G_UDLINK(4), // Object is a user-defined link
		H5G_RESERVED_5(5), // Reserved for future use
		H5G_RESERVED_6(6), // Reserved for future use
		H5G_RESERVED_7(7); // Reserved for future use
		private static final Map<Integer, H5G_object> lookup = new HashMap<Integer, H5G_object>();

		static {
			for (H5G_object s : EnumSet.allOf(H5G_object.class))
				lookup.put(s.getCode(), s);
		}

		private int code;

		H5G_object(int layout_type) {
			this.code = layout_type;
		}

		public int getCode() {
			return this.code;
		}

		public static H5G_object get(int code) {
			return lookup.get(code);
		}
	}
	
	/**
     * create the file and add groups ans dataset into the file, which is the
     * same as javaExample.H5DatasetCreate
     * 
     * @see HDF5DatasetCreate.H5DatasetCreate
     * @throws Exception
     */
	
    private static String[] createFile() {
    	/*
    	String FILENAME = "Data" + File.separator + "file01.h5";
    	String DATASETNAME = "/";

    	long file_id = -1;

		// Open a file using default properties.
		try {
			file_id = H5.H5Fopen(FILENAME, HDF5Constants.H5F_ACC_RDONLY,
					HDF5Constants.H5P_DEFAULT);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		// Begin iteration.
		System.out.println("Objects in root group:");
		
		try {
			if (file_id >= 0) {
				long count = H5.H5Gn_members(file_id, DATASETNAME);
				String[] oname = new String[(int) count];
				int[] otype = new int[(int) count];
				long[] orefs = new long[(int) count];
				H5.H5Gget_obj_info_all(file_id, DATASETNAME, oname, otype, new int[otype.length], orefs, HDF5Constants.H5_INDEX_NAME);

				// Get type of the object and display its name and type.
				for (int indx = 0; indx < otype.length; indx++) {
					switch (H5G_object.get(otype[indx])) {
					case H5G_GROUP:
						System.out.println("  Group: " + oname[indx]);
						break;
					case H5G_DATASET:
						System.out.println("  Dataset: " + oname[indx]);
						break;
					case H5G_TYPE:
						System.out.println("  Datatype: " + oname[indx]);
						break;
					default:
						System.out.println("  Unknown: " + oname[indx]);
					}
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		// Close the file.
		try {
			if (file_id >= 0)
				H5.H5Fclose(file_id);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
    	*/
    	
    	
    	
    	
    	
    	
    	
		StringBuffer[][][] str_data = { { {new StringBuffer("1"), new StringBuffer("22")},
				{new StringBuffer("333"), new StringBuffer("4444")} } , 
				{ {new StringBuffer("55555"), new StringBuffer("666666")},
					{new StringBuffer("7777777"), new StringBuffer("88888888")} } };

		Hdf5File file = new Hdf5File(fname);
		String[] pathTokens = dspath.split("/");
		Hdf5Group[] groups = new Hdf5Group[pathTokens.length + 1];
		groups[0] = file;
		for (int i = 1; i < groups.length; i++) {
			Hdf5Group group = new Hdf5Group(pathTokens[i-1]);
			group.addToGroupInFile(groups[i-1], file);
			groups[i] = group;
		}
		
		Hdf5DataSet<Byte> dataSet = new Hdf5DataSet<>(dsname, dims2D, new Byte((byte) 0));
		System.out.println("Added to file: " + dataSet.addToGroupInFile(groups[groups.length-1], file));
		
		long SDIM = dataSet.getStringLength() + 1;
		Byte[] dstr = new Byte[(int) dataSet.numberOfValues()];

		// Write the data to the dataset.
		for (int i = 0; i < dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1); i++) {
			// TODO das mit dimensions aus Datasets schreiben
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
        System.out.println("\n\nCell 0 1 3: " + dataSet.getCell(dataReadByte, 0, 1, 3));
		addAttribute(groups[3]);
		addAttribute2(groups[2]);
		
		dataSet.close();
		/*for (Hdf5Group group: groups) {
			group.close();
		}*/
		file.closeAllBelow();
		file.close();
		
		return dataRead;
        
        
        
/*
        // set the data values
        String[] dataIn = new String[(int) (dims2D[0] * dims2D[1])];
        for (int i = 0; i < dims2D[0]; i++) {
            for (int j = 0; j < dims2D[1]; j++) {
                dataIn[i * (int) dims2D[1] + j] = "i: " + i + ", j: " + j;
            }
        }

        // print out the data values before writing into file
        System.out.println("\n\nData Values");
        for (int i = 0; i < dims2D[0] * dims2D[1]; i++) {
            if(i % dims2D[1] == 0) {
            	System.out.print("\n" + dataIn[i]);
            } else {
            	System.out.print(", " + dataIn[i]);
            }
        }
        
        System.out.println("\n\nwrite Dataset: " + dataSet.write(dataIn));
        
        String[] dataRead = new String[(int) dataSet.numberOfValues()];
        dataRead = dataSet.read(dataRead);

        // print out the data values
        System.out.println("\n\nRead Data Values");
        for (int i = 0; i < dims2D[0] * dims2D[1]; i++) {
            if(i % dims2D[1] == 0) {
            	System.out.print("\n" + dataRead[i]);
            } else {
            	System.out.print(", " + dataRead[i]);
            }
        }

        System.out.println("\n\nCell 2 1: " + dataSet.getCell(dataRead, 2, 1));
        
		addAttribute(dataSet);
		
        dataSet.close();
        file.closeAll();
        
        return dataRead;*/
    }
	
	public static void addAttribute(Hdf5TreeElement treeElement) {

        String attrname  = "double2";
        Double[] attrValue = { 3.345, 23.243, 54.354 }; // attribute value

        Hdf5Attribute<Double> attribute = new Hdf5Attribute<>(attrname, attrValue);
        long DIM0 = attribute.getDimension(); // 1D of size two
        
        System.out.println("\n\naddToTreeElement: " + attribute.writeToTreeElement(treeElement));
        System.out.println("ds: " + attribute.getDataspace_id() + "\na: " + attribute.getAttribute_id());

        Double[] attrData = attribute.read(new Double[(int) DIM0]);
        
        // print out attribute value
        System.out.println("\n\n" + attrname);
        System.out.println("SDIM: " + DIM0);
        for (Double d: attrData) {
            System.out.println("Zahl : " + d);
        }

        attribute.close();
        
	}

	public static void addAttribute2(Hdf5TreeElement treeElement) {
        String attrname  = "numbers3";
        long stringLength = 8; // 1D of size two
		StringBuffer str_data = new StringBuffer("12345678910");

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
		
 		// Output the data to the screen.
 		System.out.println(attrname + ": " + dataRead + "\n");
        
        attribute.close();
	}
	
	private DataTableSpec createOutSpec() {
		DataColumnSpec[] colSpecs = new DataColumnSpec[(int) dims2D[1]];
		for (int i = 0; i < colSpecs.length; i++) {
	        colSpecs[i] = new DataColumnSpecCreator("Column " + i, StringCell.TYPE).createSpec();
		}
        return new DataTableSpec(colSpecs);
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

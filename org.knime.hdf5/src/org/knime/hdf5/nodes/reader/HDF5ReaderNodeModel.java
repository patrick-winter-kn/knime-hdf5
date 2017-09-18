package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
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

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

// TODO first: look to line 210
// TODO delete things you can create, try everything out again
// TODO later for GUI: change the thing how you go into a group,
//		load all subGroups/attributes to show them
public class HDF5ReaderNodeModel extends NodeModel {
	
    private static String fname  = "Data" + File.separator + "example.h5";
    // TODO File separator '/' everywhere (opp system) the same?
    private static String dspath  = "tests/stringTests/first";
    private static String dsname  = "stringTest";
    private static long[] dims2D = { 2, 2 };

	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		// create the file and add groups and dataset into the file
		
		String[] dataRead = CreateDataset();
		
		//String[] dataRead = createFile();
        
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
	
	/**
     * create the file and add groups ans dataset into the file, which is the
     * same as javaExample.H5DatasetCreate
     * 
     * @see HDF5DatasetCreate.H5DatasetCreate
     * @throws Exception
     */
	
    private static String[] createFile() {
        Hdf5File file = new Hdf5File(fname);
        Hdf5DataSet<String> dataSet = new Hdf5DataSet<>(dsname, dims2D, new String(""));
        System.out.println("\n\naddToFile: " + dataSet.addToFile(file, dspath));

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
        
        return dataRead;
    }
	
	public static void addAttribute(Hdf5DataSet<?> dataSet) {
        String attrname  = "data range5";
        Byte[] attrValue = { 's', 'd' }; // attribute value
        long[] attrDims = { 2 }; // 1D of size two

        Hdf5Attribute<Byte> attribute = new Hdf5Attribute<>(attrname, attrValue, attrDims);
        
        System.out.println("\n\naddToDataSet: " + attribute.writeToTreeElement(dataSet));
        System.out.println("ds: " + attribute.getDataspace_id() + "\na: " + attribute.getAttribute_id());

        Byte[] attrData = attribute.read(new Byte[(int) attribute.numberOfValues()]);
        
        // print out attribute value
        System.out.println("\n\n" + attrname);
        System.out.println(attrData[0] + "  " + attrData[1]);

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
	
	
	
	
	
	
	
	
	
	
	
	
	// Test
	
	private static String[] CreateDataset() {
		String FILENAME = "Data" + File.separator + "h5ex_t_string.h5";
		String DATASETNAME = "DS2";
		int DIM0 = 2;
		int DIM1 = 2;
		int SDIM = 8;
		long[] dims = { DIM0, DIM1 };
		byte[][][] dset_data = new byte[DIM0][DIM1][SDIM];
		String[] dstr = new String[DIM0 * DIM1 * SDIM];
		StringBuffer[][] str_data = { {new StringBuffer("Parting"),
				new StringBuffer("is such")}, {new StringBuffer("sweet"),
				new StringBuffer("sorrow.")} };

		System.out.println("\n\nInt: " + HDF5Constants.H5T_NATIVE_INT + "\nLong: " + HDF5Constants.H5T_NATIVE_INT64 + "\nDouble: " + HDF5Constants.H5T_NATIVE_DOUBLE);
		
		Hdf5File file = new Hdf5File(FILENAME);
		// TODO make it with Character instead of String
		Hdf5DataSet<String> dataSet = new Hdf5DataSet<>(DATASETNAME, dims, new String(""));
		System.out.println("Added to file: " + dataSet.addToFile(file, ""));

		//dataspace_id = dataSet.getDataspace_id();
		
		// Write the data to the dataset.
		for (int indx = 0; indx < DIM0; indx++) {
			for (int kndx = 0; kndx < DIM1; kndx++) {
				for (int jndx = 0; jndx < SDIM; jndx++) {
					if (jndx < str_data[indx][kndx].length()) {
						dset_data[indx][kndx][jndx] = (byte) str_data[indx][kndx].charAt(jndx);
						dstr[(indx*DIM1 + kndx) * SDIM + jndx] = "" + str_data[indx][kndx].charAt(jndx);
					} else {
						dset_data[indx][kndx][jndx] = 0;
						dstr[(indx*DIM1 + kndx) * SDIM + jndx] = "0";
					}
				}
			}
		}
		dataSet.write(dstr);

/*
		// Terminate access to the data space.
		try {
			if (dataspace_id >= 0)
				H5.H5Sclose(dataspace_id);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
*/

		String[] dataRead;

		// Allocate space for data.
		//byte[][][] dset_dataRead = new byte[(int) dims[0]][(int) dims[1]][(int) dataSet.getStringLength()];
		//StringBuffer[][] str_dataRead = new StringBuffer[(int) dims[0]][(int) dims[1]];
		dataRead = new String[(int) dims[0] * (int) dims[1]];

		// Read data.
		dataRead = dataSet.read(dataRead);

		// Output the data to the screen.
		for (int indx = 0; indx < dims[0]; indx++) {
			for (int kndx = 0; kndx < (int) dims[1]; kndx++) {
				System.out.println(DATASETNAME + " [" + indx + "][" + kndx + "]: " + str_data[indx][kndx]);
			}
		}
		System.out.println();
		
		for (String s: dataRead) {
			System.out.println(s);
		}

		dataSet.close();
		file.closeAll();

		return dataRead;
	}
}

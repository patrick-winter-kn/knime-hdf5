package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.JoinedRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5File;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

public class HDF5ReaderNodeModel extends NodeModel {
	
    private static String fname  = "Data" + File.separator + "example2.h5";
    private static String dsname  = "intTest"; //File separator überall gleich?
    private static long[] dims2D = { 3, 3 };

	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		// create the file and add groups and dataset into the file
        createFile();
        int[][] dataRead = useFile();
        
		DataTableSpec outSpec = createOutSpec();
		BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
		/*for (int i = 0; i < 100; i++) {
			outContainer.addRowToTable(new DefaultRow("Row " + i, new IntCell(i+1)));
		}*/
		System.out.println("\n");
		for (int i = 0; i < dataRead.length; i++) {
			DefaultRow row = null;
			JoinedRow jrow = null;
			if (dataRead[i].length > 0) {
				row = new DefaultRow("Row " + i, new IntCell(dataRead[i][0]));
			}
			System.out.println(i + ": " + dataRead[i].length);
			for (int j = 1; j < dataRead[i].length; j++) {
				DefaultRow newRow = new DefaultRow("Row " + i, new IntCell(dataRead[i][j]));
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
    private static void createFile() throws Exception {
        int file_id = -1;
        int dataspace_id = -1;
        int dataset_id = -1;
        
        Hdf5File file = new Hdf5File(fname);
        file_id = file.getFile_id();

        // Create the data space for the dataset.
        try {
            dataspace_id = H5.H5Screate_simple(2, dims2D, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Create the dataset.
        try {
            if ((file_id >= 0) && (dataspace_id >= 0))
                dataset_id = H5.H5Dcreate(file_id, dsname,
                        HDF5Constants.H5T_STD_I32LE, dataspace_id,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("\nCreate: " + file_id + " : " + dataset_id);
        
        // Terminate access to the data space.
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // set the data values
        int[] dataIn = new int[3 * 3];
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                dataIn[i * 3 + j] = i * 3 + j;
            }
        }

        // print out the data values before writing into file
        System.out.println("\n\nData Values");
        for (int i = 0; i < 3 * 3; i++) {
            System.out.print("\n" + dataIn[i]);
        }
        
        // Write the data to the dataset.
        try {
            if (dataset_id >= 0)
                H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT,
                        HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, dataIn);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // End access to the dataset and release resources used by it.
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
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
    }

	private static int[][] useFile() throws Exception {
	    int file_id = -1;
        int dataset_id = -1;
        
		Hdf5File file = new Hdf5File(fname);
		file_id = file.getFile_id();
		
        // Open dataset using the default properties.
        try {
            if (file_id >= 0)
                dataset_id = H5.H5Dopen(file_id, dsname, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println("\nUse: " + file_id + " : " + dataset_id);

        // Allocate array of pointers to two-dimensional arrays (the
        // elements of the dataset.
        int[][] dataRead = new int[(int) dims2D[0]][(int) (dims2D[1])];

        try {
            if (dataset_id >= 0)
                H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_INT,
                        HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, dataRead);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // print out the data values
        System.out.println("\n\nOriginal Data Values");
        for (int i = 0; i < dims2D[0]; i++) {
            System.out.print("\n" + dataRead[i][0]);
            for (int j = 1; j < dims2D[1]; j++) {
                System.out.print(", " + dataRead[i][j]);
            }
        }

        // Close the dataset.
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
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
        
        return dataRead;
	}
	
	private DataTableSpec createOutSpec() {
		DataColumnSpec[] colSpecs = new DataColumnSpec[(int) dims2D[1]];
		for (int i = 0; i < colSpecs.length; i++) {
	        colSpecs[i] = new DataColumnSpecCreator("Column " + i, IntCell.TYPE).createSpec();
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

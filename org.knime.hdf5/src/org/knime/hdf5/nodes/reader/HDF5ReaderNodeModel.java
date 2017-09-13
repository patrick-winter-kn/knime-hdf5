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
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

public class HDF5ReaderNodeModel extends NodeModel {
	
    private static String fname  = "Data" + File.separator + "example.h5";
    // TODO File separator '/' everywhere (opp system) the same?
    private static String dspath  = "tests/intTests/third";
    private static String dsname  = "intTest";
    private static long[] dims2D = { 7, 4 };

	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		// create the file and add groups and dataset into the file
		
		Integer[] dataRead = createFile();
        //Integer[] dataRead = useFile();
        
		DataTableSpec outSpec = createOutSpec();
		BufferedDataContainer outContainer = exec.createDataContainer(outSpec);
		/*for (int i = 0; i < 100; i++) {
			outContainer.addRowToTable(new DefaultRow("Row " + i, new IntCell(i+1)));
		}*/
		System.out.println("\n");
		for (int i = 0; i < dims2D[0]; i++) {
			DefaultRow row = null;
			JoinedRow jrow = null;
			System.out.println(i + ": " + dataRead.length);
			row = new DefaultRow("Row " + i, new IntCell(dataRead[(int) (dims2D[1] * i)]));
			for (int j = 1; j < dims2D[1]; j++) {
				DefaultRow newRow = new DefaultRow("Row " + i, new IntCell(dataRead[(int) dims2D[1] * i + j]));
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
	
    private static Integer[] createFile() {
        Hdf5File file = new Hdf5File(fname);
        // TODO create Groups
        Hdf5DataSet<Integer> dataSet = new Hdf5DataSet<>(dsname, dims2D);
        System.out.println("\n\naddToFile: " + dataSet.addToFile(file, dspath));

        // set the data values
        Integer[] dataIn = new Integer[(int) (dims2D[0] * dims2D[1])];
        for (int i = 0; i < dims2D[0]; i++) {
            for (int j = 0; j < dims2D[1]; j++) {
                dataIn[i * (int) dims2D[1] + j] = (int) (i*dims2D[1] + j);
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
        
        Integer[] dataRead = new Integer[(int) dataSet.numberOfValues()];
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

		addAttribute(dataSet);
		
        dataSet.close();
        file.closeAll();
        
        return dataRead;
    }

	private static Integer[] useFile() {
		Hdf5File file = new Hdf5File(fname);
        Hdf5DataSet<Integer> dataSet = new Hdf5DataSet<>(dsname, dims2D);
        dataSet.addToFile(file, dspath);

        // Allocate array of pointers to two-dimensional arrays (the
        // elements of the dataset.
        Integer[] dataRead = new Integer[(int) dataSet.numberOfValues()];
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

        dataSet.close();
        file.close();
        
        return dataRead;
	}
	
	public static void addAttribute(Hdf5DataSet<?> dataSet) {
        String attrname  = "data range";
        Integer[] attrValue = { 0, 10000 }; // attribute value
        long[] attrDims = { 2 }; // 1D of size two

        Hdf5Attribute<Integer[]> attribute = new Hdf5Attribute<>(attrname, attrValue, attrDims);
        
        System.out.println("\n\naddToDataSet: " + attribute.writeToDataSet(dataSet));
        
        System.out.println("ds: " + attribute.getDataspace_id() + "\na: " + attribute.getAttribute_id());
/*
        attribute.close();

        System.out.println("addToDataSet: " + attribute.writeToDataSet(dataSet));
        
        System.out.println("ds: " + attribute.getDataspace_id() + "\na: " + attribute.getAttribute_id());
*/
        try {
            if (attribute.getDataspace_id() >= 0)
                H5.H5Sget_simple_extent_dims(attribute.getDataspace_id(), attrDims, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Allocate array of pointers to two-dimensional arrays (the
        // elements of the dataset.
        int[] attrData = new int[(int) attrDims[0]];

        // Read data.
        try {
            if (attribute.getAttribute_id() >= 0)
                H5.H5Aread(attribute.getAttribute_id(), HDF5Constants.H5T_NATIVE_INT, attrData);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // print out attribute value
        System.out.println("\n\n" + attrname);
        System.out.println(attrData[0] + "  " + attrData[1]);

        attribute.close();
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

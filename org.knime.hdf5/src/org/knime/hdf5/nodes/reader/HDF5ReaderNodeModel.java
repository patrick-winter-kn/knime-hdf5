package org.knime.hdf5.nodes.reader;

import java.io.File;
import java.io.IOException;

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

// TODO more Dimensions also for KNIME
// TODO delete things you can create, try everything out again
// TODO later for GUI: change the thing how you go into a group,
//		load all subGroups/attributes to show them
public class HDF5ReaderNodeModel extends NodeModel {
	
    private static String fname  = "Data" + File.separator + "example.h5";
    // TODO File separator '/' everywhere (opp system) the same?
    private static String dspath  = "tests/stringTests/second";
    private static String dsname  = "stringTest";
    private static long[] dims2D = { 2, 2, 2, 8 }; //stringLength = 7 (= 8 - 1), 1 for the null terminator

	protected HDF5ReaderNodeModel() {
		super(0, 1);
	}

	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		// create the file and add groups and dataset into the file
		
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
	
	/**
     * create the file and add groups ans dataset into the file, which is the
     * same as javaExample.H5DatasetCreate
     * 
     * @see HDF5DatasetCreate.H5DatasetCreate
     * @throws Exception
     */
	
    private static String[] createFile() {
		StringBuffer[][][] str_data = { { {new StringBuffer("1"), new StringBuffer("22")},
				{new StringBuffer("333"), new StringBuffer("4444")} } , 
				{ {new StringBuffer("55555"), new StringBuffer("666666")},
					{new StringBuffer("7777777"), new StringBuffer("88888888")} } };

		Hdf5File file = new Hdf5File(fname);
		Hdf5DataSet<Byte> dataSet = new Hdf5DataSet<>(dsname, dims2D, new Byte((byte) 0));
		System.out.println("Added to file: " + dataSet.addToFile(file, dspath));
		
		long SDIM = (int) dataSet.getStringLength() + 1;
		Byte[] dstr = new Byte[(int) dataSet.numberOfValues()];

		// Write the data to the dataset.
		for (int i = 0; i < dataSet.numberOfValuesRange(0, dataSet.getDimensions().length - 1); i++) {
			for (int j = 0; j < SDIM; j++) {
				// TODO das mit dimensions aus Datasets schreiben
				StringBuffer buf = str_data[(i / (int) dims2D[2]) / (int) dims2D[1]][(i / (int) dims2D[2]) % (int) dims2D[1]][i % (int) dims2D[2]];
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
		
		// TODO check if indices are too big
        System.out.println("\n\nStringCell 0 1 1: " + dataSet.getStringCell(dataReadByte, 0, 1, 1));
        System.out.println("\n\nCell 0 1 3: " + dataSet.getCell(dataReadByte, 0, 1, 3));
		addAttribute(dataSet);
		
		dataSet.close();
		file.closeAll();
		
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
}

package org.knime.hdf5.nodes.writer;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.IntCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.types.Hdf5DataTypeTemplate;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataTypeTemplate;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class HDF5WriterNodeModel extends NodeModel {

	protected HDF5WriterNodeModel() {
		super(1, 0);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected BufferedDataTable[] execute(BufferedDataTable[] inData, ExecutionContext exec) throws Exception {
		String fname = "C:\\Program Files\\KNIME_SDK\\Data\\writer\\testwriter.h5";
		String gname = "intTests";
		String dname = "int_02";
		long[] dims = new long[]{ 4, 3 };
		
		Hdf5File file = Hdf5File.openFile(fname, Hdf5File.READ_WRITE_ACCESS);
		Hdf5Group group = null;
		try {
			group = file.createGroup(gname);
		} catch (IOException ioe) {
			group = file.getGroup(gname);
		}
		Hdf5DataTypeTemplate dataTypeTempl = new Hdf5DataTypeTemplate(
				new Hdf5HdfDataTypeTemplate(HdfDataType.INTEGER, Hdf5HdfDataType.DEFAULT_STRING_LENGTH), 
				Hdf5KnimeDataType.INTEGER, false, true);
		Hdf5DataSet<Integer> dataSet = null;
		try {
			dataSet = (Hdf5DataSet<Integer>) group.createDataSet(dname, dims, dataTypeTempl);
		} catch (IOException ioe) {
			dataSet = (Hdf5DataSet<Integer>) group.getDataSet(dname);
		}
		
		Integer[] outData = new Integer[(int) (dims[0] * dims[1])];
		
		CloseableRowIterator iter = inData[0].iterator();
		int i = 0;
		while (iter.hasNext()) {
			DataRow row = iter.next();
			for (int j = 0; j < dims[1]; j++) {
				IntCell cell = (IntCell) row.getCell(j);
				outData[i * (int) dims[1] + j] = cell.getIntValue();
			}
			i++;
		}
		
		dataSet.write(outData);
		
		file.close();
		
		return null;
	}
	
	@Override
	protected DataTableSpec[] configure(DataTableSpec[] inSpecs) throws InvalidSettingsException {
		return null;
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

package org.knime.hdf5.lib.types;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataTypeTemplate extends Hdf5DataType {
	
	Hdf5HdfDataTypeTemplate m_hdfTypeTemplate;
	
	public Hdf5DataTypeTemplate(Hdf5HdfDataTypeTemplate hdfTypeTemplate, Hdf5KnimeDataType knimeType, boolean vlen, boolean fromDS) {
		super(hdfTypeTemplate, knimeType, vlen, fromDS);
		m_hdfTypeTemplate = hdfTypeTemplate;
	}

	Hdf5HdfDataTypeTemplate getHdfTypeTemplate() {
		return m_hdfTypeTemplate;
	}
	
	private Hdf5DataType getDataType(long elementId) {
		Hdf5DataType dataType = null;
		
		try {
			int elementTypeId = H5.H5Iget_type(elementId);
			if (elementTypeId != HDF5Constants.H5I_DATASET && elementTypeId != HDF5Constants.H5I_ATTR) {
				throw new IllegalStateException("DataType can only be for a DataSet or Attribute");
			}
			
			dataType = new Hdf5DataType(elementId, this);
		
		} catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("Invalid elementId", hle);
			/* dataType stays null */            
		} 
		
		return dataType;
	}
	
	public Hdf5DataType createDataType(long elementId, long stringLength) {
		Hdf5DataType dataType = null;
		
		dataType = getDataType(elementId);
		dataType.getHdfType().createHdfDataType(elementId, stringLength);
		
		return dataType;
	}
	
	public Hdf5DataType openDataType(long elementId) {
		Hdf5DataType dataType = null;
		
		dataType = getDataType(elementId);
		dataType.getHdfType().openHdfDataType(elementId);
		
		return dataType;
	}
}

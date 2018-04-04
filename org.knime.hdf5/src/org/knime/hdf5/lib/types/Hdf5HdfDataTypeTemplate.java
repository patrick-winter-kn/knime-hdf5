package org.knime.hdf5.lib.types;

public class Hdf5HdfDataTypeTemplate extends Hdf5HdfDataType {

	public Hdf5HdfDataTypeTemplate(HdfDataType type, long stringLength) {
		super(type);
		createHdfDataTypeString(stringLength);
	}
	
	boolean isSimilarTo(Hdf5HdfDataType hdfType) {
		return getType() == hdfType.getType() && getStringLength() == hdfType.getStringLength();
	}
}

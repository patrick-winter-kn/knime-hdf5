package org.knime.hdf5.lib.types;

public class Hdf5HdfDataTypeTemplate extends Hdf5HdfDataType {

	public Hdf5HdfDataTypeTemplate(HdfDataType type, long stringLength) {
		super(type);
	}

	Hdf5HdfDataType getInstance(final long elementId) {
		return getInstance(getType(), elementId);
	}
}

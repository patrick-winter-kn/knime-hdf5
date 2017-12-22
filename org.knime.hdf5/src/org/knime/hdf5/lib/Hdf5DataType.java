package org.knime.hdf5.lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.HDF5Constants;

public enum Hdf5DataType {
	UNKNOWN(-1),		// data type is unknown
	INTEGER(0),			// data type is an Integer
	LONG(1),			// data type is a Long
	DOUBLE(2),			// data type is a Double
	STRING(3);			// data type is a String (by using Byte)
	private static final Map<Integer, Hdf5DataType> lookup = new HashMap<>();

	static {
		for (Hdf5DataType o : EnumSet.allOf(Hdf5DataType.class)) {
			lookup.put(o.getTypeId(), o);
		}
	}

	private final int m_typeId;
	
	private final long[] m_constants = new long[2];

	Hdf5DataType(final int typeId) {
		m_typeId = typeId;
	}

	/**
	 * 
	 * @param type dataType class name ( starts with H5T_ )
	 * @param size size in byte
	 * @param fromDS true if the DataType is from a dataSet
	 */
	public static Hdf5DataType getInstance(String type, int size, boolean fromDS) {
		Hdf5DataType dataType = UNKNOWN;
		
		if (type.equals("H5T_INTEGER") && size <= 4) {
			dataType = INTEGER;
		} else if (fromDS && type.equals("H5T_INTEGER") && size <= 8) {
			dataType = LONG;
		} else if (type.equals("H5T_INTEGER") && size > 4 || type.equals("H5T_FLOAT")) {
			dataType = DOUBLE;
		} else {
			dataType = STRING;
		}
		
		if (type.equals("H5T_INTEGER")) {
			if (size == 1) {
				dataType.m_constants[0] = HDF5Constants.H5T_STD_I8LE;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_INT8;
			} else if (size == 2) {
				dataType.m_constants[0] = HDF5Constants.H5T_STD_I16LE;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_INT16;
			} else if (size == 4) {
				dataType.m_constants[0] = HDF5Constants.H5T_STD_I32LE;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_INT32;
			} else if (size == 8) {
				dataType.m_constants[0] = HDF5Constants.H5T_STD_I64LE;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_INT64;
			}
		} else if (type.equals("H5T_FLOAT")) {
			if (size == 4) {
				dataType.m_constants[0] = HDF5Constants.H5T_IEEE_F32LE;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_FLOAT;
			} else if (size == 8) {
				dataType.m_constants[0] = HDF5Constants.H5T_IEEE_F64LE;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			}
		} else if (type.equals("H5T_CHAR")) {
			if (size == 4) {
				dataType.m_constants[0] = HDF5Constants.H5T_C_S1;
				dataType.m_constants[1] = HDF5Constants.H5T_NATIVE_CHAR;
			}
		}
		
		return dataType;
	}
	
	public static Hdf5DataType get(int typeId) {
		return lookup.get(typeId);
	}

	public static int getTypeIdByArray(Object[] objects) {
		String type = objects.getClass().getComponentType().toString();
		String pack = "class java.lang.";
		if (type.equals(pack + "Integer")) {
			return 0;
		} else if (type.equals(pack + "Long")) {
			return 1;
		} else if (type.equals(pack + "Double")) {
			return 2;
		} else if (type.equals(pack + "String")) {
			return 3;
		} else {
			NodeLogger.getLogger("HDF5 Files").error("Datatype is not supported", new UnsupportedDataTypeException());
			return -1;
		}
	}

	public int getTypeId() {
		return m_typeId;
	}
	
	public long[] getConstants() {
		return m_constants;
	}
	
	public boolean isString() {
		return this == STRING;
	}

	public DataType getColumnType() {
		switch (m_typeId) {
		case 0: 
			return IntCell.TYPE;
		case 1: 
			return LongCell.TYPE;
		case 2: 
			return DoubleCell.TYPE;
		case 3:
			return StringCell.TYPE;
		}
		return null;
	}
}
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

		switch (m_typeId) {
		case 0: 
			m_constants[0] = HDF5Constants.H5T_STD_I32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT32;
			break;
		case 1: 
			m_constants[0] = HDF5Constants.H5T_STD_I64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT64;
			break;
		case 2: 
			m_constants[0] = HDF5Constants.H5T_IEEE_F64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			break;
		case 3:
			// for Hdf5DataSet: m_constants will get values in constructor or updateDimensions()
			// for Hdf5Attribute: m_constants will get values in Hdf5TreeElement.addAttribute() or updateDimension()
			break;
		default:
			NodeLogger.getLogger("HDF5 Files").info("Other datatypes than Integer, Long, Double and String are not supported");
		}
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
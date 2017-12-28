package org.knime.hdf5.lib.types;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;

public class Hdf5DataType {
	
	public static final int DEFAULT = 0;
	
	private final Hdf5HdfDataType m_hdfType;

	private final Hdf5KnimeDataType m_knimeType;
	
	/**
	 * 
	 * @param type dataType class name ( starts with H5T_ )
	 * @param size size in byte
	 * @param fromDS true if the DataType is from a dataSet
	 */
	public Hdf5DataType(String type, int size, boolean unsigned, boolean fromDS) {
		int typeCode = 400;
		
		switch(type) {
		case "H5T_INTEGER":
			typeCode = 100;
			break;
		case "H5T_FLOAT":
			typeCode = 200;
			break;
		case "H5T_CHAR":
			typeCode = 300;
		}
		
		typeCode += 10 * size + (unsigned ? 1 : 0);
		
		switch (typeCode) {
		case 110:
			m_hdfType = Hdf5HdfDataType.BYTE;
			break;
		case 111:
			m_hdfType = Hdf5HdfDataType.UBYTE;
			break;
		case 120:
			m_hdfType = Hdf5HdfDataType.SHORT;
			break;
		case 121:
			m_hdfType = Hdf5HdfDataType.USHORT;
			break;
		case 140:
			m_hdfType = Hdf5HdfDataType.INTEGER;
			break;
		case 141:
			m_hdfType = Hdf5HdfDataType.UINTEGER;
			break;
		case 180:
			m_hdfType = Hdf5HdfDataType.LONG;
			break;
		case 181:
			m_hdfType = Hdf5HdfDataType.ULONG;
			break;
		case 240:
			m_hdfType = Hdf5HdfDataType.FLOAT;
			break;
		case 280:
			m_hdfType = Hdf5HdfDataType.DOUBLE;
			break;
		case 340:
			m_hdfType = Hdf5HdfDataType.CHAR;
			break;
		case 341:
			m_hdfType = Hdf5HdfDataType.UCHAR;
			break;
		default:
			m_hdfType = Hdf5HdfDataType.STRING;
			break;
		}
		
		switch (m_hdfType) {
		case BYTE:
		case UBYTE:
		case SHORT:
		case USHORT:
		case INTEGER:
			m_knimeType = Hdf5KnimeDataType.INTEGER;
			break;
		case UINTEGER:
		case LONG:
			if (fromDS) {
				m_knimeType = Hdf5KnimeDataType.LONG;
				break;
			}
		case ULONG:
		case FLOAT:
		case DOUBLE:
			m_knimeType = Hdf5KnimeDataType.DOUBLE;
			break;
		case CHAR:
		case UCHAR:
		case STRING:
			m_knimeType = Hdf5KnimeDataType.STRING;
			break;
		default:
			m_knimeType = Hdf5KnimeDataType.UNKNOWN;
			break;
		}
	}
	
	public static Hdf5DataType getTypeByArray(Object[] objects) {
		String type = objects.getClass().getComponentType().toString();
		String pack = "class java.lang.";
		if (type.equals(pack + "Integer")) {
			return new Hdf5DataType("H5T_INTEGER", 4, false, false);
		} else if (type.equals(pack + "Long")) {
			return new Hdf5DataType("H5T_INTEGER", 8, false, false);
		} else if (type.equals(pack + "Double")) {
			return new Hdf5DataType("H5T_FLOAT", 8, false, false);
		} else if (type.equals(pack + "String")) {
			return new Hdf5DataType("H5T_STRING", 0, false, false);
		} else {
			NodeLogger.getLogger("HDF5 Files").error("Datatype is not supported", new UnsupportedDataTypeException());
			return null;
		}
	}

	public Hdf5HdfDataType getHdfType() {
		return m_hdfType;
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}
	
	public long[] getConstants() {
		return m_hdfType.getConstants();
	}
	
	public boolean isHdfType(Hdf5HdfDataType hdfType) {
		return m_hdfType == hdfType;
	}

	public boolean isKnimeType(Hdf5KnimeDataType knimeType) {
		return m_knimeType == knimeType;
	}
}

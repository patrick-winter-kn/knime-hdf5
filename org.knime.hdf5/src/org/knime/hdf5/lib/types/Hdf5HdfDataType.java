package org.knime.hdf5.lib.types;

import hdf.hdf5lib.HDF5Constants;

// TODO maybe change back to package visibility
public enum Hdf5HdfDataType {
	UNKNOWN(-1),		// data type is unknown
	BYTE(0),			// data type is a Byte
	UBYTE(1),			// data type is an unsigned Byte
	SHORT(2),			// data type is a Short
	USHORT(3),			// data type is an unsigned Short
	INTEGER(4),			// data type is an Integer
	UINTEGER(5),		// data type is an unsigned Integer
	LONG(6),			// data type is a Long
	ULONG(7),			// data type is an unsigned Long
	FLOAT(8),			// data type is a Float
	DOUBLE(9),			// data type is a Double
	CHAR(10),			// data type is a Char
	UCHAR(11),			// data type is an unsigned Char
	STRING(12);			// data type is a String
	
	private final int m_typeId;
	
	private final long[] m_constants = new long[2];

	Hdf5HdfDataType(final int typeId) {
		m_typeId = typeId;
		
		switch (m_typeId) {
		case 0:
			m_constants[0] = HDF5Constants.H5T_STD_I8LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT8;
			break;
		case 1:
			m_constants[0] = HDF5Constants.H5T_STD_U8LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT8;
			break;
		case 2:
			m_constants[0] = HDF5Constants.H5T_STD_I16LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT16;
			break;
		case 3:
			m_constants[0] = HDF5Constants.H5T_STD_U16LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT16;
			break;
		case 4:
			m_constants[0] = HDF5Constants.H5T_STD_I32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT32;
			break;
		case 5:
			m_constants[0] = HDF5Constants.H5T_STD_U32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT32;
			break;
		case 6:
			m_constants[0] = HDF5Constants.H5T_STD_I64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT64;
			break;
		case 7:
			m_constants[0] = HDF5Constants.H5T_STD_U64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT64;
			break;
		case 8:
			m_constants[0] = HDF5Constants.H5T_IEEE_F32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_FLOAT;
			break;
		case 9:
			m_constants[0] = HDF5Constants.H5T_IEEE_F64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			break;
		case 10:
			m_constants[0] = HDF5Constants.H5T_C_S1;
			m_constants[1] = HDF5Constants.H5T_NATIVE_CHAR;
			break;
		case 11:
			m_constants[0] = HDF5Constants.H5T_C_S1;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UCHAR;
			break;
		case 12:
			// constants will be initialized later in this case
		}
	}
	
	long[] getConstants() {
		return m_constants;
	}
	
	public Object createArray(int length) {
		switch (this) {
		case BYTE:
		case UBYTE:
			return new Byte[length];
		case SHORT:
		case USHORT:
			return new Short[length];
		case INTEGER:
		case UINTEGER:
			return new Integer[length];
		case LONG:
		case ULONG:
			return new Long[length];
		case FLOAT:
			return new Float[length];
		case DOUBLE:
			return new Double[length];
		case CHAR:
		case UCHAR:
			return new Character[length];
		case STRING:
			return new String[length];
		default:
			return null;
		}
	}
}
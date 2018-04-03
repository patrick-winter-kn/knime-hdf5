package org.knime.hdf5.lib.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5HdfDataType {

	public static enum HdfDataType {
		BYTE(110),
		UBYTE(111),
		SHORT(210),
		USHORT(211),
		INTEGER(410),
		UINTEGER(411),
		LONG(810),
		ULONG(811),
		FLOAT(420),
		DOUBLE(820),
		CHAR(30),
		UCHAR(31),
		STRING(41),
		REFERENCE(51);	// dataType is an object reference
		private static final Map<Integer, HdfDataType> LOOKUP = new HashMap<Integer, HdfDataType>();

		static {
			for (HdfDataType s : EnumSet.allOf(HdfDataType.class))
				LOOKUP.put(s.getTypeId(), s);
		}

		private int m_typeId;

		private HdfDataType(int typeId) {
			m_typeId = typeId;
		}
		
		int getTypeId() {
			return m_typeId;
		}

		static HdfDataType get(int typeId) {
			return LOOKUP.get(typeId);
		}
	}
	
	public static final long DEFAULT_STRING_LENGTH = 0L;

	private static final Map<HdfDataType, Hdf5HdfDataType> LOOKUP = new HashMap<>();
	
	private static final Map<Long, Hdf5HdfDataType> LOOKUP_STRING = new HashMap<>();
	
	private final HdfDataType m_type;
	
	private final long[] m_constants = { -1, -1 };
	
	private long m_stringLength;
	
	protected Hdf5HdfDataType(final HdfDataType type) {
		m_type = type;
		if (!(this instanceof Hdf5HdfDataTypeTemplate)) {
			LOOKUP.put(m_type, this);
		}
		
		switch (m_type) {
		case BYTE:
			m_constants[0] = HDF5Constants.H5T_STD_I8LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT8;
			break;
		case UBYTE:
			m_constants[0] = HDF5Constants.H5T_STD_U8LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT8;
			break;
		case SHORT:
			m_constants[0] = HDF5Constants.H5T_STD_I16LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT16;
			break;
		case USHORT:
			m_constants[0] = HDF5Constants.H5T_STD_U16LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT16;
			break;
		case INTEGER:
			m_constants[0] = HDF5Constants.H5T_STD_I32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT32;
			break;
		case UINTEGER:
			m_constants[0] = HDF5Constants.H5T_STD_U32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT32;
			break;
		case LONG:
			m_constants[0] = HDF5Constants.H5T_STD_I64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT64;
			break;
		case ULONG:
			m_constants[0] = HDF5Constants.H5T_STD_U64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT64;
			break;
		case FLOAT:
			m_constants[0] = HDF5Constants.H5T_IEEE_F32LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_FLOAT;
			break;
		case DOUBLE:
			m_constants[0] = HDF5Constants.H5T_IEEE_F64LE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			break;
		case CHAR:
			m_constants[0] = HDF5Constants.H5T_C_S1;
			m_constants[1] = HDF5Constants.H5T_NATIVE_CHAR;
			break;
		case UCHAR:
			m_constants[0] = HDF5Constants.H5T_C_S1;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UCHAR;
			break;
		case STRING:
			/* other constructor used */
			break;
		case REFERENCE:
			m_constants[0] = HDF5Constants.H5T_REFERENCE;
		}
	}
	
	private Hdf5HdfDataType(final long elementId) {
		m_type = HdfDataType.STRING;
		LOOKUP_STRING.put(elementId, this);
	}

	/**
	 * {@code Hdf5HdfDataType} is the representation of the dataType in the .h5 file. <br>
	 * <br>
	 * TODO {@code VLEN} and {@code REFERENCE} are not implemented
	 * 
	 * @param elementId
	 * @param typeId - 
	 * 				This is the code for the {@code Hdf5HdfDataType}
	 * 				and has the following structure: <br>
	 * 				<br>
	 * 				{@code typeId = 100 * a + 10 * b + 1 * c} <br>
	 * 				<br>
	 * 				{@code a} is its size in bytes (only for numbers) <br>
	 * 				{@code b} is its base dataType ({@code b == 1} for int, {@code b == 2} for float, {@code b == 3} for char,
	 * 						{@code b == 4} for String, {@code b == 5} for reference) <br>
	 * 				{@code c} is its sign ({@code c == 0} for signed, {@code c == 1} for unsigned) <br>
	 * @param stringLength
	 * 
	 */
	static synchronized Hdf5HdfDataType getInstance(final HdfDataType type, final long elementId) {
		if (type != HdfDataType.STRING) {
			if (LOOKUP.containsKey(type)) {
				return LOOKUP.get(type);
			}
			return new Hdf5HdfDataType(type);
			
		} else if (LOOKUP_STRING.containsKey(elementId)) {
			return LOOKUP_STRING.get(elementId);
		}
		
		return new Hdf5HdfDataType(elementId);
	}
	
	void createHdfDataType(final long elementId, final long stringLength) {
		if (m_type == HdfDataType.STRING) {
			try {
				// Create file and memory datatypes. For this example we will save
				// the strings as FORTRAN strings, therefore they do not need space
				// for the null terminator in the file.
				long fileTypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
				H5.H5Tset_size(fileTypeId, stringLength);
				
				long memTypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				// (+1) for: Make room for null terminator
				H5.H5Tset_size(memTypeId, stringLength + 1);
				
				m_constants[0] = fileTypeId;
				m_constants[1] = memTypeId;
				m_stringLength = stringLength;
				
			} catch (HDF5LibraryException hle) {
				NodeLogger.getLogger("HDF Files").error("String dataType could not be created", hle);
			}
		}
	}
	
	void openHdfDataType(final long elementId) {
		if (m_type == HdfDataType.STRING) {
			try {
				long fileTypeId = H5.H5Iget_type(elementId) == HDF5Constants.H5I_DATASET ? H5.H5Dget_type(elementId) : H5.H5Aget_type(elementId);
				long stringLength = H5.H5Tget_size(fileTypeId);
	    		
				long memTypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				// (+1) for: Make room for null terminator
				H5.H5Tset_size(memTypeId, stringLength + 1);

				m_constants[0] = fileTypeId;
				m_constants[1] = memTypeId;
				m_stringLength = stringLength;
				
			} catch (HDF5LibraryException hle) {
				NodeLogger.getLogger("HDF Files").error("String dataType could not be opened", hle);
			}
		}
	}
	
	HdfDataType getType() {
		return m_type;
	}
	
	long[] getConstants() {
		return m_constants;
	}

	public long getStringLength() {
		return m_stringLength;
	}

	public Object createArray(int length) throws UnsupportedDataTypeException {
		switch (m_type) {
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
			throw new UnsupportedDataTypeException("Cannot create array of this dataType");
		}
	}
	
	@Override
	public String toString() {
		switch (m_type) {
		case BYTE:
			return "BYTE";
		case UBYTE:
			return "UBYTE";
		case SHORT:
			return "SHORT";
		case USHORT:
			return "USHORT";
		case INTEGER:
			return "INTEGER";
		case UINTEGER:
			return "UINTEGER";
		case LONG:
			return "LONG";
		case ULONG:
			return "ULONG";
		case FLOAT:
			return "FLOAT";
		case DOUBLE:
			return "DOUBLE";
		case CHAR:
			return "CHAR";
		case UCHAR:
			return "UCHAR";
		case STRING:
			return "STRING";
		default:
			return "UNKNOWN";
		}
	}
/*
 * TODO try to reopen with H5T_copy()
	public void closeIfString() throws HDF5LibraryException {
		if (m_type == HdfDataType.STRING) {
			LOOKUP_STRING.remove(m_elementId);
			
	 		// Terminate access to the file and mem type.
			for (int i = 0; i < 2; i++) {
				if (getConstants()[i] >= 0) {
	 				H5.H5Tclose(getConstants()[i]);
	 				getConstants()[i] = -1;
	 			}
			}
		}
	}
*/
}
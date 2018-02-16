package org.knime.hdf5.lib.types;

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

// TODO maybe change back to package visibility if possible
public class Hdf5HdfDataType {

	public static final int BYTE = 110;
	public static final int UBYTE = 111;
	public static final int SHORT = 210;
	public static final int USHORT = 211;
	public static final int INTEGER = 410;
	public static final int UINTEGER = 411;
	public static final int LONG = 810;
	public static final int ULONG = 811;
	public static final int FLOAT = 420;
	public static final int DOUBLE = 820;
	public static final int CHAR = 30;
	public static final int UCHAR = 31;
	public static final int STRING = 0;
	public static final int REFERENCE = 1;	// dataType is an object reference

	private static final Map<Integer, Hdf5HdfDataType> LOOKUP = new HashMap<>();
	private static final Map<Long, Hdf5HdfDataType> LOOKUP_STRING = new HashMap<>();
	
	private final int m_typeId;
	
	private final long[] m_constants = new long[2];
	
	/**
	 * {@code Hdf5HdfDataType} is the representation of the dataType in the .h5 file.
	 * Every dataType can only initialized once, except Strings.
	 * 
	 * TODO {@code VLEN} and {@code REFERENCE} are not implemented
	 * 
	 * @param typeId
	 * 				This is the code for the {@code Hdf5HdfDataType}
	 * 				and has the following structure: <br>
	 * 				<br>
	 * 				{@code typeId = 100 * a + 10 * b + 1 * c} <br>
	 * 				<br>
	 * 				{@code a} is its size (in bytes) <br>
	 * 				{@code b} is its base dataType ({@code b == 1} for int, {@code b == 2} for float, {@code b == 3} for char) <br>
	 * 				{@code c} is its sign ({@code c == 0} for signed, {@code c == 1} for unsigned) <br>
	 * 				<br>
	 * 				{@code typeId == 0} if it isn't a int, float or char
	 * 
	 * @param fileTypeId
	 * @param memTypeId
	 * 
	 */
	private Hdf5HdfDataType(final int typeId, long fileTypeId, long memTypeId) {
		this(typeId);
		
		if (m_typeId == 0) {
			m_constants[0] = fileTypeId;
			m_constants[1] = memTypeId;
		}
	}
	
	private Hdf5HdfDataType(final int typeId) {
		m_typeId = typeId;
		
		if (m_typeId != 0) {
			LOOKUP.put(m_typeId, this);
		}
		
		switch (m_typeId) {
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
			/* 
			 * The other constructor should be used for Strings.
			 */
			break;
		case REFERENCE:
			m_constants[0] = HDF5Constants.H5T_REFERENCE;
		}
	}
	
	static synchronized Hdf5HdfDataType createInstance(final int typeId) {
		if (LOOKUP.containsKey(typeId)) {
			return getInstance(typeId);
		}
		
		return new Hdf5HdfDataType(typeId);
	}
	
	public static synchronized Hdf5HdfDataType createInstanceString(final long elementId, final long stringLength) {
		if (LOOKUP.containsKey(elementId)) {
			return getInstanceString(elementId);
		}
		
		long filetypeId = -1;
		long memtypeId = -1;
		
    	// Create file and memory datatypes. For this example we will save
		// the strings as FORTRAN strings, therefore they do not need space
		// for the null terminator in the file.
		try {
			filetypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
			if (filetypeId >= 0) {
				H5.H5Tset_size(filetypeId, stringLength);
			}
			
			memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
			if (memtypeId >= 0) {
				H5.H5Tset_size(memtypeId, stringLength + 1);
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF Files").error("StringType could not be created", hle);
		}
		
		return new Hdf5HdfDataType(STRING, filetypeId, memtypeId);
	}
	
	public static Hdf5HdfDataType getInstance(final int typeId) {
		return LOOKUP.get(typeId);
	}
	
	public static Hdf5HdfDataType getInstanceString(final long elementId) {
		/*
		long stringLength = -1;
	
		long filetypeId = -1;
		long memtypeId = -1;
		try {
    		// Get the datatype and its size.
    		if (elementId >= 0) {
				filetypeId = m_fromDS ? H5.H5Dget_type(elementId) : H5.H5Aget_type(elementId);
			}
			if (filetypeId >= 0) {
				stringLength = H5.H5Tget_size(filetypeId);
				// (+1) for: Make room for null terminator
			}
    		
    		// Create the memory datatype.
    		memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
			if (memtypeId >= 0) {
				H5.H5Tset_size(memtypeId, stringLength + 1);
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF Files").error("StringType could not be updated", hle);
		}
		
		getConstants()[0] = filetypeId;
		getConstants()[1] = memtypeId;
		
		return stringLength;*/
		return null;
	}
	
	int getTypeId() {
		return m_typeId;
	}
	
	long[] getConstants() {
		return m_constants;
	}

	public Object createArray(int length) {
		switch (m_typeId) {
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
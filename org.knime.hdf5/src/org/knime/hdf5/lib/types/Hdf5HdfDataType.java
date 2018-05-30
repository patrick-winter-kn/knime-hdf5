package org.knime.hdf5.lib.types;

import java.util.HashMap;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
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
		STRING(41),
		REFERENCE(51);	// dataType is an object reference
		private static final Map<Integer, HdfDataType> LOOKUP = new HashMap<>();

		static {
			for (HdfDataType hdfType : HdfDataType.values()) {
				LOOKUP.put(hdfType.getTypeId(), hdfType);
			}
		}

		private int m_typeId;

		private HdfDataType(int typeId) {
			m_typeId = typeId;
		}

		static HdfDataType get(int typeId) {
			return LOOKUP.get(typeId);
		}
		
		public static HdfDataType getHdfDataType(DataType type) throws UnsupportedDataTypeException {
			if (type.equals(IntCell.TYPE)) {	
				return INTEGER;
			} else if (type.equals(LongCell.TYPE)) {	
				return LONG;
			} else if (type.equals(DoubleCell.TYPE)) {	
				return DOUBLE;
			} else if (type.equals(StringCell.TYPE)) {	
				return STRING;
			}
			throw new UnsupportedDataTypeException("Unknown dataType");
		}
		
		int getTypeId() {
			return m_typeId;
		}
	}
	
	public static final long DEFAULT_STRING_LENGTH = 63L;

	private static final Map<HdfDataType, Hdf5HdfDataType> LOOKUP = new HashMap<>();
	
	private final HdfDataType m_type;
	
	private final long[] m_constants = { -1, -1 };
	
	private long m_stringLength;
	
	private Hdf5HdfDataType(final HdfDataType type) {
		m_type = type;
		
		if (m_type != HdfDataType.STRING) {
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
		case STRING:
			break;
		case REFERENCE:
			m_constants[0] = HDF5Constants.H5T_REFERENCE;
		}
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
	public static synchronized Hdf5HdfDataType getInstance(final HdfDataType type) {
		if (type != HdfDataType.STRING && LOOKUP.containsKey(type)) {
			return LOOKUP.get(type);
		}
		
		return new Hdf5HdfDataType(type);
	}
	
	void createHdfDataTypeString(final long stringLength) {
		if (m_type == HdfDataType.STRING) {
			try {
				// Create file and memory dataTypes. For this example we will save
				// the strings as FORTRAN strings, therefore they do not need space
				// for the null terminator in the file.
				long fileTypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
				H5.H5Tset_size(fileTypeId, stringLength);
				H5.H5Tlock(fileTypeId);
				
				long memTypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				// (+1) for: Make room for null terminator
				H5.H5Tset_size(memTypeId, stringLength + 1);
				H5.H5Tlock(memTypeId);
				
				m_constants[0] = fileTypeId;
				m_constants[1] = memTypeId;
				m_stringLength = stringLength;
				
			} catch (HDF5LibraryException hle) {
				NodeLogger.getLogger("HDF Files").error("String dataType could not be created", hle);
			}
		}
	}
	
	void openHdfDataTypeString(final long elementId) {
		if (m_type == HdfDataType.STRING) {
			try {
				long fileTypeId = H5.H5Iget_type(elementId) == HDF5Constants.H5I_DATASET ? H5.H5Dget_type(elementId) : H5.H5Aget_type(elementId);
				long stringLength = H5.H5Tget_size(fileTypeId);
				H5.H5Tlock(fileTypeId);
	    		
				long memTypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				// (+1) for: Make room for null terminator
				H5.H5Tset_size(memTypeId, stringLength + 1);
				H5.H5Tlock(memTypeId);

				m_constants[0] = fileTypeId;
				m_constants[1] = memTypeId;
				m_stringLength = stringLength;
				
			} catch (HDF5LibraryException hle) {
				NodeLogger.getLogger("HDF Files").error("String dataType could not be opened", hle);
			}
		}
	}
	
	public HdfDataType getType() {
		return m_type;
	}
	
	long[] getConstants() {
		return m_constants;
	}

	public long getStringLength() {
		return m_stringLength;
	}
	
	public boolean isSimilarTo(Hdf5HdfDataType hdfType) {
		return getType() == hdfType.getType() && getStringLength() == hdfType.getStringLength();
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
		case STRING:
			return new String[length];
		default:
			throw new UnsupportedDataTypeException("Cannot create array of this dataType");
		}
	}
	
	@Override
	public String toString() {
		return m_type.toString();
	}
}
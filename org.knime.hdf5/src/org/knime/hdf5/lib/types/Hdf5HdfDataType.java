package org.knime.hdf5.lib.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.hdf5.nodes.writer.edit.EditDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5HdfDataType {

	public static enum HdfDataType {
		INT8(110),
		UINT8(111),
		INT16(210),
		UINT16(211),
		INT32(410),
		UINT32(411),
		INT64(810),
		UINT64(811),
		FLOAT32(420),
		FLOAT64(820),
		STRING(2);
		private static final Map<Integer, HdfDataType> LOOKUP = new HashMap<>();
		
		public static final int AUTO_STRING_LENGTH = -1;

		static {
			for (HdfDataType hdfType : HdfDataType.values()) {
				LOOKUP.put(hdfType.getTypeId(), hdfType);
			}
		}

		private int m_typeId;

		private HdfDataType(int typeId) {
			m_typeId = typeId;
		}

		public static HdfDataType get(int typeId) {
			return LOOKUP.get(typeId);
		}
		
		public static HdfDataType getHdfDataType(DataType type) {
			if (type.equals(IntCell.TYPE)) {	
				return INT32;
			} else if (type.equals(LongCell.TYPE)) {	
				return INT64;
			} else if (type.equals(DoubleCell.TYPE)) {	
				return FLOAT64;
			} else {	
				return STRING;
			}
		}
		
		public static HdfDataType getHdfDataType(Type type) {
			switch (type) {
			case INTEGER:
				return INT32;
			case DOUBLE:	
				return FLOAT64;
			default:
				return STRING;
			}
		}
		
		public static List<HdfDataType> getConvertibleTypes(HdfDataType inputType, Object[] values) {
			List<HdfDataType> types = inputType.getAlwaysConvertibleHdfTypes();

			List<HdfDataType> maybeTypes = inputType.getPossiblyConvertibleHdfTypes();
			maybeTypes.removeAll(types);
			for (HdfDataType hdfType : maybeTypes) {
				if (hdfType.areValuesConvertible(values, inputType, null)) {
					types.add(hdfType);
				}
			}
			
			return types;
		}
		
		public int getTypeId() {
			return m_typeId;
		}
		
		public boolean isUnsigned() {
			return m_typeId % 10 == 1;
		}
		
		public HdfDataType getSignedType() {
			return isUnsigned() ? get(m_typeId - 1) : this;
		}
		
		public boolean isNumber() {
			return (m_typeId / 10) % 10 > 0;
		}

		public boolean isFloat() {
			return (m_typeId / 10) % 10 == 2;
		}
		
		public boolean isMaxValueLargerThanInt() {
			return this == UINT32 || this == INT64 || this == UINT64;
		}
		
		public int getSize() {
			return 8 * (m_typeId / 100);
		}
		
		private double getMin() {
			if (!isFloat()) {
				return isUnsigned() ? 0 : -Math.pow(2, getSize()-1);
			} else if (this == FLOAT32) {
				return ((Float) (-Float.MAX_VALUE)).doubleValue();
			} else if (this == FLOAT64) {
				return -Double.MAX_VALUE;
			}
			return -Double.MAX_VALUE;
		}
		
		private double getMax() {
			if (!isFloat()) {
				return Math.pow(2, getSize() - (isUnsigned() ? 0 : 1)) - 1;
			} else if (this == FLOAT32) {
				return  ((Float) Float.MAX_VALUE).doubleValue();
			} else if (this == FLOAT64) {
				return Double.MAX_VALUE;
			}
			return Double.MAX_VALUE;
		}

		public List<HdfDataType> getAlwaysConvertibleHdfTypes() {
			List<HdfDataType> types = new ArrayList<>();
			
			switch (this) {
			case INT8:
				types.add(HdfDataType.INT8);
			case UINT8:
				if (isUnsigned()) {
					types.add(HdfDataType.UINT8);
				}
			case INT16:
				types.add(HdfDataType.INT16);
			case UINT16:
				if (isUnsigned()) {
					types.add(HdfDataType.UINT16);
				}
			case INT32:
				types.add(HdfDataType.INT32);
			case UINT32:
				if (isUnsigned()) {
					types.add(HdfDataType.UINT32);
				}
			case INT64:
				types.add(HdfDataType.INT64);
			case UINT64:
				if (isUnsigned()) {
					types.add(HdfDataType.UINT64);
				}
			case FLOAT32:
				types.add(HdfDataType.FLOAT32);
			case FLOAT64:
				types.add(HdfDataType.FLOAT64);
			case STRING:
				types.add(HdfDataType.STRING);
				break;
			}
			
			return types;
		}
		
		public List<HdfDataType> getPossiblyConvertibleHdfTypes() {
			List<HdfDataType> types = new ArrayList<>();
			
			// TODO check this again
			switch (this) {
			case INT8:
			case UINT8:
			case INT16:
			case UINT16:
			case INT32:
			case UINT32:
			case INT64:
			case UINT64:
				types.add(HdfDataType.INT8);
				types.add(HdfDataType.UINT8);
				types.add(HdfDataType.INT16);
				types.add(HdfDataType.UINT16);
			case FLOAT32:
			case FLOAT64:
				types.add(HdfDataType.INT32);
				types.add(HdfDataType.UINT32);
				types.add(HdfDataType.INT64);
				types.add(HdfDataType.UINT64);
				types.add(HdfDataType.FLOAT32);
				types.add(HdfDataType.FLOAT64);
			case STRING:
				types.add(HdfDataType.STRING);
				break;
			}
			
			return types;
		}
		
		public boolean areValuesConvertible(Object[] values, HdfDataType inputType, EditDataType editDataType) {
			if (isNumber()) {
				if (!inputType.isNumber()) {
					return false;
				}
				
				double min = getMin();
				double max = getMax();
				for (Object value : values) {
					double number = ((Number) value).doubleValue();
					if (Double.compare(number, min) < 0 || Double.compare(number, max) > 0) {
						return false;
					}
				}
			} else {
				int stringLength = editDataType != null ? editDataType.getStringLength() : AUTO_STRING_LENGTH;
				if (editDataType != null && editDataType.isFixed()) {
					for (Object value : values) {
						if (value.toString().length() > stringLength) {
							return false;
						}
					}
				} else {
					for (Object value : values) {
						int newStringLength = value.toString().length();
						stringLength = newStringLength > stringLength ? newStringLength : stringLength;
					}
					if (editDataType != null) {
						editDataType.setStringLength(stringLength);
					}
				}
			}
			
			return true;
		}
	}
	
	public static enum Endian {
		LITTLE_ENDIAN, BIG_ENDIAN
	}
	
	private static final Map<HdfDataType, Hdf5HdfDataType> BIG_ENDIAN_TYPES = new HashMap<>();

	private static final Map<HdfDataType, Hdf5HdfDataType> LITTLE_ENDIAN_TYPES = new HashMap<>();

	private final HdfDataType m_type;
	
	private final Endian m_endian;
	
	private final long[] m_constants = { -1, -1 };
	
	private long m_stringLength;
	
	private Hdf5HdfDataType(final HdfDataType type, final Endian endian) {
		m_type = type;
		m_endian = endian;
		
		boolean littleEndian = m_endian == Endian.LITTLE_ENDIAN;
		if (m_type != HdfDataType.STRING) {
			(littleEndian ? LITTLE_ENDIAN_TYPES : BIG_ENDIAN_TYPES).put(m_type, this);
		}
		
		switch (m_type) {
		case INT8:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_I8LE : HDF5Constants.H5T_STD_I8BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT8;
			break;
		case UINT8:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_U8LE : HDF5Constants.H5T_STD_U8BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT8;
			break;
		case INT16:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_I16LE : HDF5Constants.H5T_STD_I16BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT16;
			break;
		case UINT16:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_U16LE : HDF5Constants.H5T_STD_U16BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT16;
			break;
		case INT32:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_I32LE : HDF5Constants.H5T_STD_I32BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT32;
			break;
		case UINT32:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_U32LE : HDF5Constants.H5T_STD_U32BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT32;
			break;
		case INT64:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_I64LE : HDF5Constants.H5T_STD_I64BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_INT64;
			break;
		case UINT64:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_STD_U64LE : HDF5Constants.H5T_STD_U64BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_UINT64;
			break;
		case FLOAT32:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_IEEE_F32LE : HDF5Constants.H5T_IEEE_F32BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_FLOAT;
			break;
		case FLOAT64:
			m_constants[0] = littleEndian ? HDF5Constants.H5T_IEEE_F64LE : HDF5Constants.H5T_IEEE_F64BE;
			m_constants[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			break;
		case STRING:
			break;
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
	public static synchronized Hdf5HdfDataType getInstance(final HdfDataType type, final Endian endian) {
		if (type != HdfDataType.STRING) {
			Map<HdfDataType, Hdf5HdfDataType> types = endian == Endian.LITTLE_ENDIAN ? LITTLE_ENDIAN_TYPES : BIG_ENDIAN_TYPES;
			if (types.containsKey(type)) {
				return types.get(type);
			}
		}
		
		return new Hdf5HdfDataType(type, endian);
	}
	
	public static Hdf5HdfDataType createCopyFrom(Hdf5HdfDataType copyHdfType) {
		return getInstance(copyHdfType.getType(), copyHdfType.getEndian());
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
	
	public Endian getEndian() {
		return m_endian;
	}
	
	long[] getConstants() {
		return m_constants;
	}

	public long getStringLength() {
		return m_stringLength;
	}
	
	public boolean isSimilarTo(Hdf5HdfDataType hdfType) {
		return getType() == hdfType.getType() && getEndian() == hdfType.getEndian() && getStringLength() == hdfType.getStringLength();
	}

	public Object[] createArray(int length) throws UnsupportedDataTypeException {
		switch (m_type) {
		case INT8:
		case UINT8:
			return new Byte[length];
		case INT16:
		case UINT16:
			return new Short[length];
		case INT32:
		case UINT32:
			return new Integer[length];
		case INT64:
		case UINT64:
			return new Long[length];
		case FLOAT32:
			return new Float[length];
		case FLOAT64:
			return new Double[length];
		case STRING:
			return new String[length];
		default:
			throw new UnsupportedDataTypeException("Cannot create array of this dataType");
		}
	}
	
	@Override
	public String toString() {
		return m_type.toString() + "," + m_endian.toString();
	}
}
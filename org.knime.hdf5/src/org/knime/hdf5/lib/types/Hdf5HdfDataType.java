package org.knime.hdf5.lib.types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.hdf5.nodes.writer.edit.EditDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Manages the data types on hdf side which stores the enum
 * {@link HdfDataType} and more information.
 */
public class Hdf5HdfDataType {

	/**
	 * Enum which stores all supported data types on hdf side.
	 */
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

		static {
			for (HdfDataType hdfType : HdfDataType.values()) {
				LOOKUP.put(hdfType.getTypeId(), hdfType);
			}
		}

		private int m_typeId;

		private HdfDataType(int typeId) {
			m_typeId = typeId;
		}

		/**
		 * @param typeId the type id
		 * @return the data type
		 * @see HdfDataType#getTypeId()
		 */
		public static HdfDataType get(int typeId) {
			return LOOKUP.get(typeId);
		}
		
		/**
		 * @param type data type of a column spec
		 * @return the equivalent hdf type
		 */
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
		
		/**
		 * @param type data type of a flow variable
		 * @return the equivalent hdf type
		 */
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
		
		/**
		 * Returns the equivalent hdf type for the data type of the
		 * components in the input array considering the option
		 * that it can be unsigned. It will be String if no other
		 * data type fits.
		 * 
		 * @param values an array of objects
		 * @param unsigned if the data type is unsigned
		 * @return the equivalent hdf type
		 */
		public static HdfDataType getHdfDataType(Object[] values, boolean unsigned) {
			Class<?> type = values.getClass().getComponentType();
			if (type == Byte.class) {
				return unsigned ? UINT8 : INT8;
			} else if (type == Short.class) {
				return unsigned ? UINT16 : INT16;
			} else if (type == Integer.class) {
				return unsigned ? UINT32 : INT32;
			} else if (type == Long.class) {
				return unsigned ? UINT64 : INT64;
			} else if (type == Float.class) {
				return FLOAT32;
			} else if (type == Double.class) {
				return FLOAT64;
			} else {
				return STRING;
			}
		}
		
		/**
		 * Returns the convertible output types for the input considering
		 * its type and its min/max values or String lengths.
		 * 
		 * @param inputType the input hdf type
		 * @param values the input values
		 * @return the convertible output types
		 */
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
		
		/**
		 * The id has the following structure:
		 * <br>
		 * <br>
		 * {@code typeId = 100 * a + 10 * b + 1 * c}
		 * <br>
		 * <br>
		 * {@code a} is its size in bytes (only for numbers)
		 * <br>
		 * {@code b} is its base dataType ({@code b == 1} for int, {@code b == 2} for float)
		 * <br>
		 * {@code c} is its sign ({@code c == 0} for signed, {@code c == 1} for unsigned)
		 * or the id for String ({@code c == 2})
		 * 
		 * @return the id for this hdf type
		 */
		public int getTypeId() {
			return m_typeId;
		}
		
		public boolean isUnsigned() {
			return m_typeId % 10 == 1;
		}

		/**
		 * @return the signed data type for this hdf type
		 */
		public HdfDataType getSignedType() {
			return isUnsigned() ? get(m_typeId - 1) : this;
		}
		
		/**
		 * @return the unsigned data type for this hdf type or {@code null}
		 * 	if it does not exist
		 */
		public HdfDataType getUnsignedType() {
			return isUnsigned() ? this : get(m_typeId + 1);
		}
		
		public boolean isNumber() {
			return (m_typeId / 10) % 10 > 0;
		}

		public boolean isFloat() {
			return (m_typeId / 10) % 10 == 2;
		}
		
		/**
		 * @return if the min-max-range of this hdf type fits
		 * 	into the min-max-range of the input type
		 */
		public boolean fitMinMaxValuesIntoType(HdfDataType type) {
			return isNumber() && type.isNumber()
					&& getMinValue() >= type.getMinValue() && getMaxValue() <= type.getMaxValue();
		}
		
		/**
		 * @return the number of bits to store this hdf type (or 0 for Strings)
		 */
		public int getSize() {
			return 8 * (m_typeId / 100);
		}
		
		/**
		 * @return the min value of the range for this hdf type (or 0 for Strings)
		 */
		private double getMinValue() {
			if (isNumber()) {
				if (!isFloat()) {
					return isUnsigned() ? 0 : -Math.pow(2, getSize()-1);
				} else if (this == FLOAT32) {
					return ((Float) (-Float.MAX_VALUE)).doubleValue();
				} else if (this == FLOAT64) {
					return -Double.MAX_VALUE;
				}
				return -Double.MAX_VALUE;
			}
			
			return 0;
		}

		/**
		 * @return the max value of the range for this hdf type (or 0 for Strings)
		 */
		private double getMaxValue() {
			if (isNumber()) {
				if (!isFloat()) {
					return Math.pow(2, getSize() - (isUnsigned() ? 0 : 1)) - 1;
				} else if (this == FLOAT32) {
					return  ((Float) Float.MAX_VALUE).doubleValue();
				} else if (this == FLOAT64) {
					return Double.MAX_VALUE;
				}
				return Double.MAX_VALUE;
			}
			
			return 0;
		}

		/**
		 * @return the types which are always convertible from this hdf type
		 */
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
		
		/**
		 * @return the types which may be convertible from this hdf type
		 * 	depending on the values
		 */
		public List<HdfDataType> getPossiblyConvertibleHdfTypes() {
			List<HdfDataType> types = new ArrayList<>();
			
			switch (this) {
			case INT8:
			case UINT8:
			case INT16:
			case UINT16:
			case INT32:
			case UINT32:
			case INT64:
			case UINT64:
			case FLOAT32:
			case FLOAT64:
				types.add(HdfDataType.INT8);
				types.add(HdfDataType.UINT8);
				types.add(HdfDataType.INT16);
				types.add(HdfDataType.UINT16);
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
		
		/**
		 * Returns if the input values can be converted to this hdf
		 * type (output type). If this hdf type is {@code STRING}, the input
		 * editDataType is used to check the string length or set the string
		 * length if the string length is not fixed.
		 * 
		 * @param values the input values
		 * @param inputType the input hdf type
		 * @param editDataType more information for the output data type
		 * @return if the input values can be converted to this hdf
		 * 	type
		 */
		public boolean areValuesConvertible(Object[] values, HdfDataType inputType, EditDataType editDataType) {
			if (isNumber()) {
				// if the inputType is String, it is not convertible to a number
				if (!inputType.isNumber()) {
					return false;
				}
				
				double min = getMinValue();
				double max = getMaxValue();
				for (Object value : values) {
					double number = ((Number) value).doubleValue();
					if (Double.compare(number, min) < 0 || Double.compare(number, max) > 0) {
						return false;
					}
				}
			} else if (editDataType != null) {
				if (editDataType.isFixedStringLength()) {
					// check if no value exceeds the fixed string length 
					int stringLength = editDataType.getStringLength();
					for (Object value : values) {
						if (value.toString().length() > stringLength) {
							return false;
						}
					}
				} else {
					// set the string length to the maximum of the values
					int stringLength = editDataType.getStringLength();
					for (Object value : values) {
						int newStringLength = value.toString().length();
						stringLength = newStringLength > stringLength ? newStringLength : stringLength;
					}
					editDataType.setStringLength(stringLength);
				}
			}
			
			return true;
		}
	}
	
	/**
	 * Enum for the endians.
	 */
	public static enum Endian {
		LITTLE_ENDIAN, BIG_ENDIAN
	}
	
	/**
	 * map such that only one {@code Hdf5HdfDataType} exists per {@code HdfDataType} for big endians
	 */
	private static final Map<HdfDataType, Hdf5HdfDataType> BIG_ENDIAN_TYPES = new HashMap<>();

	/**
	 * map such that only one {@code Hdf5HdfDataType} exists per {@code HdfDataType} for little endians
	 */
	private static final Map<HdfDataType, Hdf5HdfDataType> LITTLE_ENDIAN_TYPES = new HashMap<>();

	private final HdfDataType m_type;
	
	private final Endian m_endian;
	
	private final long[] m_constants = { -1, -1 };
	
	private long m_stringLength;
	
	private Hdf5HdfDataType(HdfDataType type, Endian endian) {
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
	 * Returns the hdf data type with more information which are needed to
	 * create an object in an hdf file. Creates a new instance if none exists
	 * for the input type and endian or returns the already existing instance.
	 * For String, it always creates a new instance.
	 * 
	 * @param type the hdf type
	 * @param endian the endian
	 * @return the hdf data type with more information
	 */
	public static synchronized Hdf5HdfDataType getInstance(HdfDataType type, Endian endian) {
		if (type != HdfDataType.STRING) {
			Map<HdfDataType, Hdf5HdfDataType> types = endian == Endian.LITTLE_ENDIAN ? LITTLE_ENDIAN_TYPES : BIG_ENDIAN_TYPES;
			if (types.containsKey(type)) {
				return types.get(type);
			}
		}
		
		return new Hdf5HdfDataType(type, endian);
	}
	
	/**
	 * Returns a new copy of the input {@code copyHdfType}. This returns only
	 * a new instance for String data types. In the other cases, it returns
	 * the input again.
	 * 
	 * @param copyHdfType the hdf type to copy
	 * @return the copy hdf type which might be the same instance
	 */
	public static Hdf5HdfDataType createCopyFrom(Hdf5HdfDataType copyHdfType) {
		return getInstance(copyHdfType.getType(), copyHdfType.getEndian());
	}
	
	/**
	 * Creates the String data type if the hdf type is a String. In the other
	 * cases, it does nothing.
	 * 
	 * @param stringLength the String length for which the String data type
	 * 	is created for
	 * @throws IOException if an error occurred in the hdf library while creating
	 */
	void createHdfDataTypeString(long stringLength) throws IOException {
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
				throw new IOException("String dataType could not be created: " + hle.getMessage(), hle);
			}
		}
	}
	
	/**
	 * Opens the String data type if the hdf type is a String. In the other
	 * cases, it does nothing.
	 * 
	 * @param elementId the id of the hdf object of this String data type
	 * @throws IOException if an error occurred in the hdf library while opening
	 */
	void openHdfDataTypeString(long elementId) throws IOException {
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
				throw new IOException("String dataType could not be opened: " + hle.getMessage(), hle);
			}
		}
	}
	
	/**
	 * @return the hdf type (as enum)
	 */
	public HdfDataType getType() {
		return m_type;
	}

	public Endian getEndian() {
		return m_endian;
	}

	/**
	 * @return array of 2 constants for the file type and memory type of an hdf data type
	 */
	long[] getConstants() {
		return m_constants;
	}

	/**
	 * @return the string length of the String data type (or 0 for non-Strings)
	 */
	public long getStringLength() {
		return m_stringLength;
	}
	
	/**
	 * @param hdfType the hdf type to compare
	 * @return if type, endian and stringLength are equal, <br>
	 * 	i.e. {@code toString().equals(hdfType.toString())}
	 */
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
		return "{ type=" + m_type.toString() + ",endian=" + m_endian.toString()
				+ (m_type == HdfDataType.STRING ? ",stringLength=" + m_stringLength : "") + " }";
	}
}
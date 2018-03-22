package org.knime.hdf5.lib.types;

import javax.activation.UnsupportedDataTypeException;

import org.knime.hdf5.lib.Hdf5Attribute;

import hdf.hdf5lib.HDF5Constants;

public class Hdf5DataType {
	
	private static final int POW_2_8 = (int) Math.pow(2, 8);
	
	private static final int POW_2_16 = (int) Math.pow(2, 16);
	
	private static final long POW_2_32 = (long) Math.pow(2, 32);
	
	private static final double POW_2_64 = Math.pow(2, 64);
	
	public static final int DEFAULT_STRING_TYPE_SIZE = 0;
	
	private final Hdf5HdfDataType m_hdfType;

	private final Hdf5KnimeDataType m_knimeType;

	private final boolean m_vlen; // variable length
	
	private final boolean m_fromDS;
	
	/**
	 * 
	 * @param type dataType class name ( starts with H5T_ )
	 * @param size size in byte
	 * @param unsigned true if the dataType is unsigned
	 * @param vlen true if dataType has a variable length
	 * @param fromDS true if the dataType is from a dataSet
	 */
	public Hdf5DataType(long classId, int size, boolean unsigned, boolean vlen, boolean fromDS) {
		m_fromDS = fromDS;
		m_vlen = vlen;
		
		// see Hdf5HdfDataType for the structure of the typeId
		// if typeId cannot be defined differently, it will stay a STRING
		int typeId = Hdf5HdfDataType.STRING;
		
		if (classId == HDF5Constants.H5T_INTEGER) {
			typeId = 100 * size + 10 + (unsigned ? 1 : 0);
			
		} else if (classId == HDF5Constants.H5T_FLOAT) {
			typeId = 100 * size + 20;
			
		}/* else if (classId == HDF5Constants.H5T_CHAR) {
			typeId = 30 + (unsigned ? 1 : 0);
		}*/
		
		m_hdfType = Hdf5HdfDataType.getInstance(typeId);
		
		switch (m_hdfType.getTypeId()) {
		case Hdf5HdfDataType.BYTE:
		case Hdf5HdfDataType.UBYTE:
		case Hdf5HdfDataType.SHORT:
		case Hdf5HdfDataType.USHORT:
		case Hdf5HdfDataType.INTEGER:
			m_knimeType = Hdf5KnimeDataType.INTEGER;
			break;
		case Hdf5HdfDataType.UINTEGER:
		case Hdf5HdfDataType.LONG:
			if (fromDS) {
				m_knimeType = Hdf5KnimeDataType.LONG;
				break;
			}
		case Hdf5HdfDataType.ULONG:
		case Hdf5HdfDataType.FLOAT:
		case Hdf5HdfDataType.DOUBLE:
			m_knimeType = Hdf5KnimeDataType.DOUBLE;
			break;
		case Hdf5HdfDataType.CHAR:
		case Hdf5HdfDataType.UCHAR:
		case Hdf5HdfDataType.STRING:
			m_knimeType = Hdf5KnimeDataType.STRING;
			break;
		default:
			m_knimeType = Hdf5KnimeDataType.UNKNOWN;
			break;
		}
	}
	
	public static Hdf5DataType getTypeByArray(Object[] objects) throws UnsupportedDataTypeException {
		Object type = objects.getClass().getComponentType();
		if (type.equals(Integer.class)) {
			return new Hdf5DataType(HDF5Constants.H5T_INTEGER, 4, false, false, false);
		} else if (type.equals(Double.class)) {
			return new Hdf5DataType(HDF5Constants.H5T_FLOAT, 8, false, false, false);
		} else if (type.equals(String.class)) {
			return new Hdf5DataType(HDF5Constants.H5T_STRING, DEFAULT_STRING_TYPE_SIZE, false, false, false);
		}
		throw new UnsupportedDataTypeException("KnimeDataType of array is not supported");
	}
	
	public Hdf5HdfDataType getHdfType() {
		return m_hdfType;
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}
	
	public boolean isVlen() {
		return m_vlen;
	}

	public long[] getConstants() {
		return m_hdfType.getConstants();
	}
	
	public boolean isHdfType(int hdfTypeId) {
		return m_hdfType.getTypeId() == hdfTypeId;
	}

	public boolean isKnimeType(Hdf5KnimeDataType knimeType) {
		return m_knimeType == knimeType;
	}
	
	public boolean equalTypes() {
		switch (m_knimeType) {
		case INTEGER:
			return isHdfType(Hdf5HdfDataType.INTEGER);
		case LONG:
			return isHdfType(Hdf5HdfDataType.LONG);
		case DOUBLE:
			return isHdfType(Hdf5HdfDataType.DOUBLE);
		case STRING:
			return isHdfType(Hdf5HdfDataType.STRING);
		default:
			return false;
		}
	}
	
	public Hdf5Attribute<?> createAttribute(String name, Object[] data) throws UnsupportedDataTypeException {
		switch (m_knimeType) {
		case INTEGER:
			return new Hdf5Attribute<Integer>(name, (Integer[]) data);
		case DOUBLE:
			return new Hdf5Attribute<Double>(name, (Double[]) data);
		case STRING:
			return new Hdf5Attribute<String>(name, (String[]) data);
		default:
			throw new UnsupportedDataTypeException("Unsupported knimeDataType for attribute");
		}
	}
	
	public Class<?> getHdfClass() throws UnsupportedDataTypeException {
		switch(m_hdfType.getTypeId()) {
		case Hdf5HdfDataType.BYTE:
		case Hdf5HdfDataType.UBYTE:
			return Byte.class;
		case Hdf5HdfDataType.SHORT:
		case Hdf5HdfDataType.USHORT:
			return Short.class;
		case Hdf5HdfDataType.UINTEGER:
		case Hdf5HdfDataType.INTEGER:
			return Integer.class;
		case Hdf5HdfDataType.LONG:
		case Hdf5HdfDataType.ULONG:
			return Long.class;
		case Hdf5HdfDataType.FLOAT:
			return Float.class;
		case Hdf5HdfDataType.DOUBLE:
			return Double.class;
		case Hdf5HdfDataType.CHAR:
		case Hdf5HdfDataType.UCHAR:
			return Character.class;
		case Hdf5HdfDataType.STRING:
			return String.class;
		default:
			throw new UnsupportedDataTypeException("Unknown hdfDataType");
		}
	}
	
	public Class<?> getKnimeClass() throws UnsupportedDataTypeException {
		switch (m_knimeType) {
		case INTEGER:
			return Integer.class;
		case DOUBLE:
			return Double.class;
		case LONG:
			return Long.class;
		case STRING:
			return String.class;
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	public <T, S> S hdfToKnime(Class<T> hdfClass, T in, Class<S> knimeClass) throws UnsupportedDataTypeException {
		if (hdfClass == in.getClass() && hdfClass == getHdfClass()) {
			if (isHdfType(Hdf5HdfDataType.INTEGER) || isHdfType(Hdf5HdfDataType.DOUBLE) || isHdfType(Hdf5HdfDataType.CHAR)
					|| isHdfType(Hdf5HdfDataType.UCHAR) || isHdfType(Hdf5HdfDataType.STRING)) {
				return knimeClass.cast(in);
			}
			
			if (hdfClass == Byte.class) {
				int byteValue = (int) (byte) in;
				return knimeClass.cast(byteValue + (isHdfType(Hdf5HdfDataType.UBYTE) && byteValue < 0 ? POW_2_8 : 0));
				
			} else if (hdfClass == Short.class) {
				int shortValue = (int) (short) in;
				return knimeClass.cast(shortValue + (isHdfType(Hdf5HdfDataType.USHORT) && shortValue < 0 ? POW_2_16 : 0));
				
			} else if (hdfClass == Integer.class) {
				long uintegerValue = (long) (int) in;
				uintegerValue = uintegerValue + (uintegerValue < 0 ? POW_2_32 : 0);
				if (m_fromDS) {
					return knimeClass.cast(uintegerValue);
				} else {
					return knimeClass.cast((double) uintegerValue);
				}
			} else if (hdfClass == Long.class) {
				if (isHdfType(Hdf5HdfDataType.LONG)) {
					if (m_fromDS) {
						return knimeClass.cast(in);
					} else {
						return knimeClass.cast((double) (long) in);
					}
				} else {
					double ulongValue = (double) (long) in;
					return knimeClass.cast(ulongValue + (ulongValue < 0 ? POW_2_64 : 0));
				}
			} else if (hdfClass == Float.class) {
				return knimeClass.cast((double) (float) in);
				
			} else {
				throw new UnsupportedDataTypeException("Unknown hdfDataType");
			}
		}
		
		throw new UnsupportedDataTypeException("Incorrect hdfClass or input class");
	}
	
	@Override
	public String toString() {
		return "{ hdfType=" + m_hdfType + ",knimeType=" + m_knimeType + ",vlen=" + m_vlen + ",fromDS=" + m_fromDS + " }";
	}
}

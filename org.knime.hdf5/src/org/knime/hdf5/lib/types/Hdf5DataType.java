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
		} else if (type.equals(Long.class)) {
			return new Hdf5DataType(HDF5Constants.H5T_INTEGER, 8, false, false, false);
		} else if (type.equals(Double.class)) {
			return new Hdf5DataType(HDF5Constants.H5T_FLOAT, 8, false, false, false);
		} else if (type.equals(String.class)) {
			return new Hdf5DataType(HDF5Constants.H5T_STRING, DEFAULT_STRING_TYPE_SIZE, false, false, false);
		}
		throw new UnsupportedDataTypeException("Datatype of array is not supported");
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
			throw new UnsupportedDataTypeException("Unsupported dataType for attribute");
		}
	}

	//public <T, S extends Number, S extends String> S hdfToKnime(T in) throws UnsupportedDataTypeException {
	public Object hdfToKnime(Object in) throws UnsupportedDataTypeException {
		switch(m_hdfType.getTypeId()) {
		case Hdf5HdfDataType.BYTE:
			return (int) (byte) in;
		case Hdf5HdfDataType.UBYTE:
			int ubyteValue = (int) (byte) in;
			return ubyteValue + (ubyteValue < 0 ? POW_2_8 : 0);
		case Hdf5HdfDataType.SHORT:
			return (int) (short) in;
		case Hdf5HdfDataType.USHORT:
			int ushortValue = (int) (short) in;
			return ushortValue + (ushortValue < 0 ? POW_2_16 : 0);
		case Hdf5HdfDataType.UINTEGER:
			long uintegerValue = (long) (int) in;
			uintegerValue = uintegerValue + (uintegerValue < 0 ? POW_2_32 : 0);
			if (m_fromDS) {
				return uintegerValue;
			} else {
				return (double) uintegerValue;
			}
		case Hdf5HdfDataType.LONG:
			if (m_fromDS) {
				return in;
			} else {
				return (double) (long) in;
			}
		case Hdf5HdfDataType.ULONG:
			double ulongValue = (double) (long) in;
			return ulongValue + (ulongValue < 0 ? POW_2_64 : 0);
		case Hdf5HdfDataType.FLOAT:
			return (double) (float) in;
		case Hdf5HdfDataType.INTEGER:
		case Hdf5HdfDataType.DOUBLE:
		case Hdf5HdfDataType.CHAR:
		case Hdf5HdfDataType.UCHAR:
		case Hdf5HdfDataType.STRING:
			return in;
		default:
			throw new UnsupportedDataTypeException("Unknown dataType");
		}
	}
	
	@Override
	public String toString() {
		return "{ hdfType=" + m_hdfType + ",knimeType=" + m_knimeType + ",vlen=" + m_vlen + ",fromDS=" + m_fromDS + " }";
	}
}

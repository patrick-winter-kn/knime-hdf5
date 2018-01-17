package org.knime.hdf5.lib.types;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataType {
	
	public static final int DEFAULT_STRING_SIZE = 0;
	
	public static int stringAmount = 0;
	
	private final Hdf5HdfDataType m_hdfType;

	private final Hdf5KnimeDataType m_knimeType;
	
	private final boolean m_fromDS;
	
	/**
	 * 
	 * @param type dataType class name ( starts with H5T_ )
	 * @param size size in byte
	 * @param unsigned true if the dataType is unsigned
	 * @param fromDS true if the DataType is from a dataSet
	 */
	public Hdf5DataType(String type, int size, boolean unsigned, boolean fromDS) {
		m_fromDS = fromDS;
		int typeCode = 0;
		
		switch(type) {
		case "H5T_INTEGER":
			typeCode = 100 * size + 10 + (unsigned ? 1 : 0);
			break;
		case "H5T_FLOAT":
			typeCode = 100 * size + 20;
			break;
		case "H5T_CHAR":
			typeCode = 30 + (unsigned ? 1 : 0);
		}
		
		switch (typeCode) {
		case 110:
			m_hdfType = Hdf5HdfDataType.BYTE;
			break;
		case 111:
			m_hdfType = Hdf5HdfDataType.UBYTE;
			break;
		case 210:
			m_hdfType = Hdf5HdfDataType.SHORT;
			break;
		case 211:
			m_hdfType = Hdf5HdfDataType.USHORT;
			break;
		case 410:
			m_hdfType = Hdf5HdfDataType.INTEGER;
			break;
		case 411:
			m_hdfType = Hdf5HdfDataType.UINTEGER;
			break;
		case 810:
			m_hdfType = Hdf5HdfDataType.LONG;
			break;
		case 811:
			m_hdfType = Hdf5HdfDataType.ULONG;
			break;
		case 420:
			m_hdfType = Hdf5HdfDataType.FLOAT;
			break;
		case 820:
			m_hdfType = Hdf5HdfDataType.DOUBLE;
			break;
		case 30:
			m_hdfType = Hdf5HdfDataType.CHAR;
			break;
		case 31:
			m_hdfType = Hdf5HdfDataType.UCHAR;
			break;
		default:
			m_hdfType = Hdf5HdfDataType.STRING;
			stringAmount++;
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
			return new Hdf5DataType("H5T_STRING", DEFAULT_STRING_SIZE, false, false);
		}
		return null;
	}

	// TODO is there parallelization?
	public static void closeString() {
		if (stringAmount == 1) {
			// Terminate access to the file and memory type (see Hdf5DataSet.updateDimensions())
			Hdf5HdfDataType dataType = Hdf5HdfDataType.STRING;
			try {
				if (dataType.getConstants()[0] >= 0) {
	 				H5.H5Tclose(dataType.getConstants()[0]);
	 				dataType.getConstants()[0] = -1;
	 			}
				if (dataType.getConstants()[1] >= 0) {
	 				H5.H5Tclose(dataType.getConstants()[1]);
	 				dataType.getConstants()[1] = -1;
	 			}
			} catch (HDF5LibraryException e) {
				e.printStackTrace();
			}
		}
		if (stringAmount >= 1) {
			stringAmount--;
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
	
	// TODO preconditions for argument in
	public Object hdfToKnime(Object in) {
		switch (m_hdfType) {
		case BYTE:
			return (Integer) (int) (byte) in;
		case UBYTE:
			return (Integer) ((int) (byte) in + ((int) (byte) in < 0 ? (int) Math.pow(2, 8) : 0));
		case SHORT:
			return (Integer) (int) (short) in;
		case USHORT:
			return (Integer) ((int) (short) in + ((int) (short) in < 0 ? (int) Math.pow(2, 16) : 0));
		case INTEGER:
			return in;
		case UINTEGER:
			if (m_fromDS) {
				return (Long) ((long) (int) in + ((long) (int) in < 0 ? (long) Math.pow(2, 32) : 0));
			} else {
				return (Double) (double) ((long) (int) in + ((long) (int) in < 0 ? (long) Math.pow(2, 32) : 0));
			}
		case LONG:
			if (m_fromDS) {
				return in;
			} else {
				return (Double) (double) (long) in;
			}
		case ULONG:
			return (Double) ((double) (long) in + ((double) (long) in < 0 ? Math.pow(2, 64) : 0));
		case FLOAT:
			return (Double) (double) (float) in;
		case DOUBLE:
			return in;
		case CHAR:
			return in;
		case UCHAR:
			return in;
		case STRING:
			return in;
		default:
			return null;
		}
	}
	
	@Override
	public String toString() {
		return "hdfType=" + m_hdfType + ",knimeType=" + m_knimeType + ",fromDS=" + m_fromDS;
	}
}

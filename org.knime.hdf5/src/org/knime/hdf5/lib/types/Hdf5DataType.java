package org.knime.hdf5.lib.types;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataType {
	
	private static final int POW_2_8 = (int) Math.pow(2, 8);
	
	private static final int POW_2_16 = (int) Math.pow(2, 16);
	
	private static final long POW_2_32 = (long) Math.pow(2, 32);
	
	private static final double POW_2_64 = Math.pow(2, 64);
	
	private final Hdf5HdfDataType m_hdfType;

	private final Hdf5KnimeDataType m_knimeType;

	private final boolean m_vlen; // variable length
	
	private final boolean m_fromDS;
	
	protected Hdf5DataType(Hdf5HdfDataType hdfType, Hdf5KnimeDataType knimeType, 
			boolean vlen, boolean fromDS) {
		m_hdfType = hdfType;
		m_knimeType = knimeType;
		m_vlen = vlen;
		m_fromDS = fromDS;
	}
	
	protected Hdf5DataType(long elementId, Hdf5DataTypeTemplate templ) {
		m_hdfType = templ.getHdfTypeTemplate().getInstance(elementId);
		m_knimeType = templ.getKnimeType();
		m_vlen = templ.isVlen();
		m_fromDS = templ.isFromDS();
	}
	
	/**
	 * 
	 * @param type dataType class name ( starts with H5T_ )
	 * @param size size in byte
	 * @param unsigned true if the dataType is unsigned
	 * @param vlen true if dataType has a variable length
	 * @param fromDS true if the dataType is from a dataSet
	 * @throws HDF5LibraryException 
	 */
	private Hdf5DataType(long elementId, long classId, int size, boolean unsigned, boolean vlen) throws HDF5LibraryException {
		m_vlen = vlen;
		m_fromDS = H5.H5Iget_type(elementId) == HDF5Constants.H5I_DATASET;
		
		// see Hdf5HdfDataType for the structure of the typeId
		// if typeId cannot be defined differently, it will stay a STRING
		int typeId = HdfDataType.STRING.getTypeId();
		
		if (classId == HDF5Constants.H5T_INTEGER) {
			typeId = 100 * size + 10 + (unsigned ? 1 : 0);
			
		} else if (classId == HDF5Constants.H5T_FLOAT) {
			typeId = 100 * size + 20;
			
		}/* else if (classId == HDF5Constants.H5T_CHAR) {
			typeId = 30 + (unsigned ? 1 : 0);
		}*/
		
		m_hdfType = Hdf5HdfDataType.getInstance(HdfDataType.get(typeId), elementId);
		
		switch (m_hdfType.getType()) {
		case BYTE:
		case UBYTE:
		case SHORT:
		case USHORT:
		case INTEGER:
			m_knimeType = Hdf5KnimeDataType.INTEGER;
			break;
		case UINTEGER:
		case LONG:
			if (m_fromDS) {
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
	
	private static Hdf5DataType getInstance(long elementId, long classId, int size, boolean unsigned, boolean vlen) {
		Hdf5DataType dataType = null;
		
		try {
			int elementTypeId = H5.H5Iget_type(elementId);
			if (elementTypeId != HDF5Constants.H5I_DATASET && elementTypeId != HDF5Constants.H5I_ATTR) {
				throw new IllegalStateException("DataType can only be for a DataSet or Attribute");
			}
			
			dataType = new Hdf5DataType(elementId, classId, size, unsigned, vlen);
		
		} catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("Invalid elementId", hle);
			/* dataType stays null */            
		} 
		
		return dataType;
	}
	
	public static Hdf5DataType createDataType(long elementId, long classId, int size, boolean unsigned, boolean vlen, long stringLength) {
		Hdf5DataType dataType = null;
		
		try {
			dataType = getInstance(elementId, classId, size, unsigned, vlen);
			dataType.getHdfType().createHdfDataType(elementId, stringLength);
    		
		} catch (NullPointerException | IllegalArgumentException npiae) {
            NodeLogger.getLogger("HDF5 Files").error("DataType could not be created: " + npiae.getMessage(), npiae);
			/* dataType stays null */
        }
		
		return dataType;
	}
	
	public static Hdf5DataType openDataType(long elementId, long classId, int size, boolean unsigned, boolean vlen) {
		Hdf5DataType dataType = null;
		
		try {
			dataType = getInstance(elementId, classId, size, unsigned, vlen);
			dataType.getHdfType().openHdfDataType(elementId);
			
		} catch (NullPointerException | IllegalArgumentException npiae) {
            NodeLogger.getLogger("HDF5 Files").error("DataType could not be created: " + npiae.getMessage(), npiae);
			/* dataType stays null */
        }
		
		return dataType;
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
	
	protected boolean isFromDS() {
		return m_fromDS;
	}

	public long[] getConstants() {
		return m_hdfType.getConstants();
	}
	
	public boolean isHdfType(HdfDataType hdfType) {
		return m_hdfType.getType() == hdfType;
	}

	public boolean isKnimeType(Hdf5KnimeDataType knimeType) {
		return m_knimeType == knimeType;
	}
	
	public boolean equalTypes() throws UnsupportedDataTypeException {
		switch (m_knimeType) {
		case INTEGER:
			return isHdfType(HdfDataType.INTEGER);
		case LONG:
			return isHdfType(HdfDataType.LONG);
		case DOUBLE:
			return isHdfType(HdfDataType.DOUBLE);
		case STRING:
			return isHdfType(HdfDataType.STRING);
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	public Class<?> getHdfClass() throws UnsupportedDataTypeException {
		switch(m_hdfType.getType()) {
		case BYTE:
		case UBYTE:
			return Byte.class;
		case SHORT:
		case USHORT:
			return Short.class;
		case UINTEGER:
		case INTEGER:
			return Integer.class;
		case LONG:
		case ULONG:
			return Long.class;
		case FLOAT:
			return Float.class;
		case DOUBLE:
			return Double.class;
		case CHAR:
		case UCHAR:
			return Character.class;
		case STRING:
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
			if (isHdfType(HdfDataType.INTEGER) || isHdfType(HdfDataType.DOUBLE) || isHdfType(HdfDataType.CHAR)
					|| isHdfType(HdfDataType.UCHAR) || isHdfType(HdfDataType.STRING)) {
				return knimeClass.cast(in);
			}
			
			if (hdfClass == Byte.class) {
				int byteValue = (int) (byte) in;
				return knimeClass.cast(byteValue + (isHdfType(HdfDataType.UBYTE) && byteValue < 0 ? POW_2_8 : 0));
				
			} else if (hdfClass == Short.class) {
				int shortValue = (int) (short) in;
				return knimeClass.cast(shortValue + (isHdfType(HdfDataType.USHORT) && shortValue < 0 ? POW_2_16 : 0));
				
			} else if (hdfClass == Integer.class) {
				long uintegerValue = (long) (int) in;
				uintegerValue = uintegerValue + (uintegerValue < 0 ? POW_2_32 : 0);
				if (m_fromDS) {
					return knimeClass.cast(uintegerValue);
				} else {
					return knimeClass.cast((double) uintegerValue);
				}
			} else if (hdfClass == Long.class) {
				if (isHdfType(HdfDataType.LONG)) {
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

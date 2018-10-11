package org.knime.hdf5.lib.types;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataType {
	
	static final int POW_2_8 = (int) Math.pow(2, 8);
	
	static final int POW_2_16 = (int) Math.pow(2, 16);
	
	static final long POW_2_32 = (long) Math.pow(2, 32);
	
	static final double POW_2_64 = Math.pow(2, 64);
	
	private final Hdf5HdfDataType m_hdfType;

	private final Hdf5KnimeDataType m_knimeType;

	private final boolean m_vlen; // variable length
	
	private final boolean m_fromDS;
	
	private Hdf5DataType(Hdf5HdfDataType hdfType, Hdf5KnimeDataType knimeType, 
			boolean vlen, boolean fromDS) {
		// TODO throw exception if hdfType and knimeType do not fit together
		m_hdfType = hdfType;
		m_knimeType = knimeType;
		m_vlen = vlen;
		m_fromDS = fromDS;
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
	private Hdf5DataType(boolean fromDS, long classId, int size, Endian endian, boolean unsigned, boolean vlen) throws HDF5LibraryException {
		m_vlen = vlen;
		m_fromDS = fromDS;
		
		// see Hdf5HdfDataType for the structure of the typeId
		// if typeId cannot be defined differently, it will stay a STRING
		int typeId = HdfDataType.STRING.getTypeId();
		
		if (classId == HDF5Constants.H5T_INTEGER) {
			typeId = 100 * size + 10 + (unsigned ? 1 : 0);
			
		} else if (classId == HDF5Constants.H5T_FLOAT) {
			typeId = 100 * size + 20;
		}
		
		m_hdfType = Hdf5HdfDataType.getInstance(HdfDataType.get(typeId), endian);
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(m_hdfType.getType(), m_fromDS);
	}
	
	public static Hdf5DataType createDataType(Hdf5HdfDataType hdfType, Hdf5KnimeDataType knimeType, 
			boolean vlen, boolean fromDS, long stringLength) {
		Hdf5DataType dataType = null;
		
		try {
			dataType = new Hdf5DataType(hdfType, knimeType, vlen, fromDS);
			dataType.getHdfType().createHdfDataTypeString(stringLength);
    		
		} catch (NullPointerException | IllegalArgumentException npiae) {
            NodeLogger.getLogger("HDF5 Files").error("DataType could not be created: " + npiae.getMessage(), npiae);
			/* dataType stays null */
        }
		
		return dataType;
	}
	
	public static Hdf5DataType createCopyFrom(Hdf5DataType copyDataType) {
		return createDataType(Hdf5HdfDataType.createCopyFrom(copyDataType.getHdfType()),
				copyDataType.getKnimeType(), copyDataType.isVlen(), copyDataType.isFromDS(), copyDataType.getHdfType().getStringLength());
	}
	
	public static Hdf5DataType openDataType(long elementId) throws HDF5LibraryException, UnsupportedDataTypeException, IllegalArgumentException {
		int elementTypeId = H5.H5Iget_type(elementId);
		if (elementTypeId != HDF5Constants.H5I_DATASET && elementTypeId != HDF5Constants.H5I_ATTR) {
			throw new IllegalArgumentException("DataType can only be for a DataSet or Attribute");
		}
		
		boolean fromDS = elementTypeId == HDF5Constants.H5I_DATASET;
		long typeId = fromDS ? H5.H5Dget_type(elementId) : H5.H5Aget_type(elementId);
		long classId = H5.H5Tget_class(typeId);
		int size = (int) H5.H5Tget_size(typeId);
		Endian endian = H5.H5Tget_order(typeId) == HDF5Constants.H5T_ORDER_LE ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN;
		boolean vlen = classId == HDF5Constants.H5T_VLEN || H5.H5Tis_variable_str(typeId);
		boolean unsigned = false;
		if (classId == HDF5Constants.H5T_INTEGER) {
			unsigned = HDF5Constants.H5T_SGN_NONE == H5.H5Tget_sign(typeId);
		}
		H5.H5Tclose(typeId);

		if (classId == HDF5Constants.H5T_VLEN || classId == HDF5Constants.H5T_REFERENCE || classId == HDF5Constants.H5T_COMPOUND) {
			throw new UnsupportedDataTypeException("DataType " + H5.H5Tget_class_name(classId) + " is not supported");
		}
		
		Hdf5DataType dataType = new Hdf5DataType(fromDS, classId, size, endian, unsigned, vlen);
		dataType.getHdfType().openHdfDataTypeString(elementId);
			
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
	
	private boolean isFromDS() {
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
	
	public boolean isSimilarTo(Hdf5DataType dataType) {
		return getHdfType().isSimilarTo(dataType.getHdfType()) /* not necessary: && isKnimeType(dataType.getKnimeType()) */
				&& isVlen() == dataType.isVlen() && isFromDS() == dataType.isFromDS();
	}
	
	public boolean hdfTypeEqualsKnimeType() throws UnsupportedDataTypeException {
		switch (m_knimeType) {
		case INTEGER:
			return isHdfType(HdfDataType.INT32);
		case LONG:
			return isHdfType(HdfDataType.INT64);
		case DOUBLE:
			return isHdfType(HdfDataType.FLOAT64);
		case STRING:
			return isHdfType(HdfDataType.STRING);
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	public Class<?> getHdfClass() throws UnsupportedDataTypeException {
		switch(m_hdfType.getType()) {
		case INT8:
		case UINT8:
			return Byte.class;
		case INT16:
		case UINT16:
			return Short.class;
		case INT32:
		case UINT32:
			return Integer.class;
		case INT64:
		case UINT64:
			return Long.class;
		case FLOAT32:
			return Float.class;
		case FLOAT64:
			return Double.class;
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

	public <T, S> S knimeToKnime(Class<T> inputClass, T inputValue, Class<S> outputClass, Hdf5DataType outputType, Rounding rounding) throws UnsupportedDataTypeException {
		if (inputClass == inputValue.getClass() && inputClass == getKnimeClass() && outputClass == outputType.getKnimeClass()) {
			if (inputClass == outputClass) {
				return outputClass.cast(inputValue);
			}
			
			if (outputType.isKnimeType(Hdf5KnimeDataType.STRING)) {
				return outputClass.cast(inputValue.toString());
			}
			
			switch (getKnimeType()) {
			case INTEGER:
				int inputValueInt = (int) inputValue;
				switch (outputType.getKnimeType()) {
				case LONG:
					return outputClass.cast((long) inputValueInt);
				case DOUBLE:
					return outputClass.cast((double) inputValueInt);
				default:
					break;
				}
			case LONG:
				long inputValueLong = (long) inputValue;
				switch (outputType.getKnimeType()) {
				case INTEGER:
					return outputClass.cast((int) inputValueLong);
				case DOUBLE:
					return outputClass.cast((double) inputValueLong);
				default:
					break;
				}
			case DOUBLE:
				double inputValueDouble = (double) inputValue;
				switch (outputType.getKnimeType()) {
				case INTEGER:
					return outputClass.cast((int) inputValueDouble);
				case LONG:
					return outputClass.cast((long) inputValueDouble);
				default:
					break;
				}
			case STRING:
				String inputValueString = (String) inputValue;
				switch (outputType.getKnimeType()) {
				case INTEGER:
					return outputClass.cast(Integer.parseInt(inputValueString));
				case LONG:
					return outputClass.cast(Long.parseLong(inputValueString));
				case DOUBLE:
					return outputClass.cast(Double.parseDouble(inputValueString));
				default:
					break;
				}
			default:
				break;
			}
		}
		
		throw new UnsupportedDataTypeException("Incorrect combination of input classes");
	}

	public <T, S> S knimeToHdf(Class<T> knimeClass, T knimeValue, Class<S> hdfClass, Rounding rounding) throws UnsupportedDataTypeException {
		if (knimeClass == knimeValue.getClass() && knimeClass == getKnimeClass() && hdfClass == getHdfClass()) {
			if (isHdfType(HdfDataType.STRING)) {
				return hdfClass.cast(knimeValue.toString());
			}
			
			switch (getKnimeType()) {
			case INTEGER:
				int knimeValueInt = (int) knimeValue;
				if (isHdfType(HdfDataType.INT8) || isHdfType(HdfDataType.UINT8)) {
					return hdfClass.cast((byte) knimeValueInt);
					
				} else if (isHdfType(HdfDataType.INT16) || isHdfType(HdfDataType.UINT16)) {
					return hdfClass.cast((short) knimeValueInt);
					
				} else if (isHdfType(HdfDataType.INT32) || isHdfType(HdfDataType.UINT32)) {
					return hdfClass.cast(knimeValueInt);
					
				} else if (isHdfType(HdfDataType.INT64) || isHdfType(HdfDataType.UINT64)) {
					return hdfClass.cast((long) knimeValueInt);
					
				} else if (isHdfType(HdfDataType.FLOAT32)) {
					return hdfClass.cast((float) knimeValueInt);
				
				} else if (isHdfType(HdfDataType.FLOAT64)) {
					return hdfClass.cast((double) knimeValueInt);
				}
				break;
			case LONG:
				long knimeValueLong = (long) knimeValue;
				if (isHdfType(HdfDataType.INT8) || isHdfType(HdfDataType.UINT8)) {
					return hdfClass.cast((byte) knimeValueLong);
					
				} else if (isHdfType(HdfDataType.INT16) || isHdfType(HdfDataType.UINT16)) {
					return hdfClass.cast((short) knimeValueLong);
					
				} else if (isHdfType(HdfDataType.INT32) || isHdfType(HdfDataType.UINT32)) {
					return hdfClass.cast((int) knimeValueLong);
					
				} else if (isHdfType(HdfDataType.INT64) || isHdfType(HdfDataType.UINT64)) {
					return hdfClass.cast(knimeValueLong);
					
				} else if (isHdfType(HdfDataType.FLOAT32)) {
					return hdfClass.cast((float) knimeValueLong);
				
				} else if (isHdfType(HdfDataType.FLOAT64)) {
					return hdfClass.cast((double) knimeValueLong);
				}
				break;
			case DOUBLE:
				if (!getHdfType().getType().isFloat()) {
					long knimeValueRounded = rounding.round((double) knimeValue);
					if (isHdfType(HdfDataType.INT8) || isHdfType(HdfDataType.UINT8)) {
						return hdfClass.cast((byte) knimeValueRounded);
						
					} else if (isHdfType(HdfDataType.INT16) || isHdfType(HdfDataType.UINT16)) {
						return hdfClass.cast((short) knimeValueRounded);
						
					} else if (isHdfType(HdfDataType.INT32) || isHdfType(HdfDataType.UINT32)) {
						return hdfClass.cast((int) knimeValueRounded);
						
					} else if (isHdfType(HdfDataType.INT64) || isHdfType(HdfDataType.UINT64)) {
						return hdfClass.cast((long) knimeValueRounded);
					}
				} else {
					double knimeValueDouble = (double) knimeValue;
					if (isHdfType(HdfDataType.FLOAT32)) {
						return hdfClass.cast((float) knimeValueDouble);
						
					} else if (isHdfType(HdfDataType.FLOAT64)) {
						return hdfClass.cast(knimeValueDouble);
					}
				}
				break;
			default:
				break;
			}
		}
		
		throw new UnsupportedDataTypeException("Incorrect combination of input classes");
	}
	
	public <T, S> S hdfToKnime(Class<T> hdfClass, T hdfValue, Class<S> knimeClass) throws UnsupportedDataTypeException {
		if (hdfClass == hdfValue.getClass() && hdfClass == getHdfClass() && knimeClass == getKnimeClass()) {
			if (isHdfType(HdfDataType.INT32) || isHdfType(HdfDataType.FLOAT64) || isHdfType(HdfDataType.STRING)) {
				return knimeClass.cast(hdfValue);
			}
			
			if (hdfClass == Byte.class) {
				int byteValue = (int) (byte) hdfValue;
				return knimeClass.cast(byteValue + (isHdfType(HdfDataType.UINT8) && byteValue < 0 ? POW_2_8 : 0));
				
			} else if (hdfClass == Short.class) {
				int shortValue = (int) (short) hdfValue;
				return knimeClass.cast(shortValue + (isHdfType(HdfDataType.UINT16) && shortValue < 0 ? POW_2_16 : 0));
				
			} else if (hdfClass == Integer.class) {
				long uintegerValue = (long) (int) hdfValue;
				uintegerValue = uintegerValue + (uintegerValue < 0 ? POW_2_32 : 0);
				if (m_fromDS) {
					return knimeClass.cast(uintegerValue);
				} else {
					return knimeClass.cast((double) uintegerValue);
				}
			} else if (hdfClass == Long.class) {
				if (isHdfType(HdfDataType.INT64)) {
					if (m_fromDS) {
						return knimeClass.cast(hdfValue);
					} else {
						return knimeClass.cast((double) (long) hdfValue);
					}
				} else {
					double ulongValue = (double) (long) hdfValue;
					return knimeClass.cast(ulongValue + (ulongValue < 0 ? POW_2_64 : 0));
				}
			} else if (hdfClass == Float.class) {
				return knimeClass.cast((double) (float) hdfValue);
				
			} else {
				throw new UnsupportedDataTypeException("Unknown hdfDataType");
			}
		}
		
		throw new UnsupportedDataTypeException("Incorrect combination of input classes");
	}
	
	@Override
	public String toString() {
		return "{ hdfType=" + m_hdfType + ",knimeType=" + m_knimeType + ",vlen=" + m_vlen + ",fromDS=" + m_fromDS + " }";
	}
}

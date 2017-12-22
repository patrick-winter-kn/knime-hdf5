package org.knime.hdf5.lib;

import java.util.Arrays;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Attribute<Type> {

	private final String m_name;
	
	private final Type[] m_value;
	
	private long m_dataspaceId = -1;
	
	private long m_attributeId = -1;
	
	private long m_dimension;
	
	private boolean m_open;
	
	private final Hdf5DataType m_type;
	
	/**
	 * Creates an attribute of the type of {@code type}. <br>
	 * Possible types of numbers are Integer, Long, Double and String. <br>
	 * <br>
	 * {@code value} always needs to be an array. <br>
	 * If it's a String[], it should only have the length 1.
	 * 
	 * @param name
	 * @param value
	 */
	public Hdf5Attribute(final String name, final Type[] value) {
		m_name = name;
		m_value = value;
		m_dimension = value.length;
		m_type = Hdf5DataType.get(Hdf5DataType.getTypeIdByArray(value));
		
		if (m_type.isString()) {
			setDimension(((String) value[0]).length() + 1);
		}
	}
	
	static Hdf5Attribute<?> getInstance(final Hdf5TreeElement treeElement, final String name) {
		Hdf5Attribute<?> attribute = null;
		
		Hdf5DataType dataType = treeElement.findAttributeType(name);
		
		long attributeId = -1;
		try {
			attributeId = H5.H5Aopen(treeElement.getElementId(), name, HDF5Constants.H5P_DEFAULT);
			long dataspaceId = -1;

			long[] dims = new long[1];
			if (dataType.isString()) {
				long filetypeId = -1;
				long memtypeId = -1;
				
				// Get the datatype and its size.
				if (attributeId >= 0) {
					filetypeId = H5.H5Aget_type(attributeId);
				}
				if (filetypeId >= 0) {
    				dims[0] = H5.H5Tget_size(filetypeId) + 1;
    				// (+1) for: Make room for null terminator
				}
				
				// Create the memory datatype.
				memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				if (memtypeId >= 0) {
					H5.H5Tset_size(memtypeId, dims[0]);
				}
				
				dataType.getConstants()[0] = filetypeId;
				dataType.getConstants()[1] = memtypeId;
				
			} else {
				// Get dataspace and allocate memory for read buffer.
				if (attributeId >= 0) {
					dataspaceId = H5.H5Aget_space(attributeId);
				}
				
				if (dataspaceId >= 0) {
					H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
				}
			}
			
			// TODO do it more simple 
			if (dataType == Hdf5DataType.INTEGER) {
				Integer[] dataInt = new Integer[(int) dims[0]];
				if (dataType.getConstants()[1] == HDF5Constants.H5T_NATIVE_INT8) {
					Byte[] data = new Byte[(int) dims[0]];
					H5.H5Aread(attributeId, dataType.getConstants()[1], data);
					for (int i = 0; i < data.length; i++) {
						dataInt[i] = (Integer) (int) (byte) data[i];
					}
				} else if (dataType.getConstants()[1] == HDF5Constants.H5T_NATIVE_INT16) {
					Short[] data = new Short[(int) dims[0]];
					H5.H5Aread(attributeId, dataType.getConstants()[1], data);
					for (int i = 0; i < data.length; i++) {
						dataInt[i] = (Integer) (int) (short) data[i];
					}
				} else {
					Integer[] data = new Integer[(int) dims[0]];
					H5.H5Aread(attributeId, dataType.getConstants()[1], data);
					dataInt = Arrays.copyOf(data, data.length);
				}
				
				attribute = new Hdf5Attribute<Integer>(name, dataInt);
				
			} else if (dataType == Hdf5DataType.DOUBLE) {
				Double[] dataDouble = new Double[(int) dims[0]];
				if (dataType.getConstants()[1] == HDF5Constants.H5T_NATIVE_INT64) {
					Long[] data = new Long[(int) dims[0]];
					H5.H5Aread(attributeId, dataType.getConstants()[1], data);
					for (int i = 0; i < data.length; i++) {
						dataDouble[i] = (Double) (double) (long) data[i];
					}
				} else if (dataType.getConstants()[1] == HDF5Constants.H5T_NATIVE_FLOAT) {
					Float[] data = new Float[(int) dims[0]];
					H5.H5Aread(attributeId, dataType.getConstants()[1], data);
					for (int i = 0; i < data.length; i++) {
						dataDouble[i] = (Double) (double) (float) data[i];
					}
				} else {
					Double[] data = new Double[(int) dims[0]];
					H5.H5Aread(attributeId, dataType.getConstants()[1], data);
					dataDouble = Arrays.copyOf(data, data.length);
				}
				
				attribute = new Hdf5Attribute<Double>(name, dataDouble);
				
			} else if (dataType == Hdf5DataType.STRING) {
				byte[] dataByte = new byte[(int) dims[0]];
				H5.H5Aread(attributeId, dataType.getConstants()[1], dataByte);
				char[] dataChar = new char[dataByte.length];
				for (int i = 0; i < dataChar.length; i++) {
					dataChar[i] = (char) dataByte[i];
				}
				String[] data = new String[]{String.copyValueOf(dataChar)};
				attribute = new Hdf5Attribute<String>(name, data);
			}
			attribute.setDataspaceId(dataspaceId);
			attribute.setAttributeId(attributeId);
    		treeElement.getAttributes().add(attribute);
        	attribute.setOpen(true);
        	
		} catch (HDF5DatatypeInterfaceException hdtie) {
			try {
				// TODO ckeck if other things also have to be closed
				H5.H5Aclose(attributeId);
				// TODO error of info?
				NodeLogger.getLogger("HDF5 Files").error("DataType of \"" + name + "\" is not supported");
			} catch (HDF5LibraryException hle) {
				hle.printStackTrace();
			}
		} catch (NullPointerException | HDF5Exception lnphe) {
			lnphe.printStackTrace();
		}
		
		return attribute;
	}
	
	public String getName() {
		return m_name;
	}
	
	public Type[] getValue() {
		return m_value;
	}

	long getDataspaceId() {
		return m_dataspaceId;
	}

	void setDataspaceId(long dataspaceId) {
		m_dataspaceId = dataspaceId;
	}

	long getAttributeId() {
		return m_attributeId;
	}

	void setAttributeId(long attributeId) {
		m_attributeId = attributeId;
	}

	public long getDimension() {
		return m_dimension;
	}

	private void setDimension(long dimension) {
		m_dimension = dimension;
	}

	boolean isOpen() {
		return m_open;
	}

	void setOpen(boolean open) {
		m_open = open;
	}
	
	public Hdf5DataType getType() {
		return m_type;
	}
	
	/**
	 * Updates the dimensions array after opening an attribute to ensure that the
	 * dimensions array is correct.
	 */
	
	void updateDimension() {
		// Get dataspace and allocate memory for read buffer.
		try {
			if (getAttributeId() >= 0) {
				setDataspaceId(H5.H5Aget_space(getAttributeId()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long[] dims = new long[1];
		if (getType().isString()) {
			long filetypeId = -1;
			long memtypeId = -1;
			
			// Get the datatype and its size.
			try {
				if (getAttributeId() >= 0) {
					filetypeId = H5.H5Aget_type(getAttributeId());
				}
				if (filetypeId >= 0) {
    				dims[0] = H5.H5Tget_size(filetypeId) + 1;
    				// (+1) for: Make room for null terminator
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Create the memory datatype.
			try {
				memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				if (memtypeId >= 0) {
					H5.H5Tset_size(memtypeId, dims[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			getType().getConstants()[0] = filetypeId;
			getType().getConstants()[1] = memtypeId;
		} else {
			try {
				if (getDataspaceId() >= 0) {
					H5.H5Sget_simple_extent_dims(getDataspaceId(), dims, null);
				}
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
		}

		setDimension(dims[0]);
	}
	
	public void close() {
		/* TODO at the site
		 * https://support.hdfgroup.org/ftp/HDF5/hdf-java/hdf-java-examples/jnative/h5/HDF5AttributeCreate.java
		 * this (Close the attribute.) is missing after the data was read
		 * 
		 */
        try {
            if (isOpen()) {
                if (getType().isString()) {
        	 		// Terminate access to the file and mem type.
        			for (long datatype: getType().getConstants()) {
        		 		if (datatype >= 0) {
    		 				H5.H5Tclose(datatype);
    		 				datatype = -1;
    		 			}
        			}
         		}

                // Close the dataspace.
                if (getDataspaceId() >= 0) {
                    H5.H5Sclose(getDataspaceId());
                    setDataspaceId(-1);
                }

                // Close the attribute.
                H5.H5Aclose(getAttributeId());
                setOpen(false);

                NodeLogger.getLogger("HDF5 Files").info("Attribute " + getName() + " closed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

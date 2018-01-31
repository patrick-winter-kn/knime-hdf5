package org.knime.hdf5.lib;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

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
	
	private long m_stringLength;
	
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
		m_type = Hdf5DataType.getTypeByArray(value);
		
		if (m_type == null) {
			NodeLogger.getLogger("HDF5 Files").error("Datatype of array is not supported",
					new UnsupportedDataTypeException());
		}
	}
	
	static Hdf5Attribute<?> getInstance(final Hdf5TreeElement treeElement, final String name) {
		Hdf5Attribute<?> attribute = null;
		Hdf5DataType dataType = treeElement.findAttributeType(name);
		
		long attributeId = -1;
		long dataspaceId = -1;
		int lsize = 1;
		try {
			attributeId = H5.H5Aopen(treeElement.getElementId(), name, HDF5Constants.H5P_DEFAULT);

			long stringLength = 0;
			
			// Get dataspace and allocate memory for read buffer.
			if (attributeId >= 0) {
				dataspaceId = H5.H5Aget_space(attributeId);
			}
			
			if (dataType.isHdfType(Hdf5HdfDataType.STRING)) {
				stringLength = dataType.updateStringTypes(attributeId);
			}
			
			if (dataspaceId >= 0) {
				int rank = H5.H5Sget_simple_extent_ndims(dataspaceId);
				if (rank > 0) {
					long[] dims = new long[rank];
					H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
                    for (int j = 0; j < dims.length; j++) {
                        lsize *= dims[j];
                    }
                }
			}
			
			Object[] dataOut = (Object[]) dataType.getKnimeType().createArray(lsize);
			
			if (dataType.isKnimeType(Hdf5KnimeDataType.STRING)) {
				long typeId = H5.H5Tget_native_type(dataType.getConstants()[0]);
                if (H5.H5Tis_variable_str(typeId) /* TODO should later be: dataType.isVlen() */) {
                	String[] strs = new String[lsize];
                    H5.H5AreadVL(attributeId, typeId, strs);
                    dataOut = strs;
                    
                } else {
					byte[] dataRead = new byte[lsize * ((int) stringLength + 1)];
					H5.H5Aread(attributeId, dataType.getConstants()[1], dataRead);
					
					dataOut = new String[lsize];
					char[][] dataChar = new char[lsize][(int) stringLength + 1];
					for (int i = 0; i < dataChar.length; i++) {
						for (int j = 0; j < dataChar[0].length; j++) {
							dataChar[i][j] = (char) dataRead[i * ((int) stringLength + 1) + j];
						}
						
						dataOut[i] = String.copyValueOf(dataChar[i]);
					}
				}
				H5.H5Tclose(typeId);
				
			} else if (dataType.equalTypes()) {
				H5.H5Aread(attributeId, dataType.getConstants()[1], dataOut);
				
			} else {
				Object[] dataRead = (Object[]) dataType.getHdfType().createArray(lsize);
                H5.H5Aread(attributeId, dataType.getConstants()[1], dataRead);
				
				for (int i = 0; i < dataRead.length; i++) {
					dataOut[i] = dataType.hdfToKnime(dataRead[i]);
				}
			}
			
			attribute = dataType.getKnimeType().createAttribute(name, dataOut);
			
			attribute.setDataspaceId(dataspaceId);
			attribute.setAttributeId(attributeId);
			attribute.setDimension(lsize);
			attribute.setStringLength(stringLength);
    		treeElement.getAttributes().add(attribute);
        	attribute.setOpen(true);
        	
		} catch (HDF5DatatypeInterfaceException hdtie) {
			try {
				close(attributeId, dataspaceId, dataType);
				NodeLogger.getLogger("HDF5 Files").error("DataType of Attribute \"" 
						+ treeElement.getPathFromFileWithName(true) + name + "\" is not supported");
			} catch (HDF5LibraryException hle) {
				hle.printStackTrace();
			}
		} catch (NullPointerException | HDF5Exception lnphe) {
			lnphe.printStackTrace();
		}
		
		return attribute;
	}
	
	private static void close(long attributeId, long dataspaceId, Hdf5DataType dataType) throws HDF5LibraryException {
		if (dataType.isHdfType(Hdf5HdfDataType.STRING)) {
	 		// Terminate access to the file and mem type.
			dataType.closeStringTypes();
 		}

        // Close the dataspace.
        if (dataspaceId >= 0) {
            H5.H5Sclose(dataspaceId);
        }

        // Close the attribute.
        H5.H5Aclose(attributeId);
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

	public long getStringLength() {
		return m_stringLength;
	}

	public void setStringLength(long m_stringLength) {
		this.m_stringLength = m_stringLength;
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
		if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
			setStringLength(getType().updateStringTypes(getAttributeId()));
			
		} else {
			try {
				if (getDataspaceId() >= 0) {
					H5.H5Sget_simple_extent_dims(getDataspaceId(), dims, null);
					setDimension(dims[0]);
				}
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
		}
	}
	
	public void close() {
		/* TODO at the site
		 * https://support.hdfgroup.org/ftp/HDF5/hdf-java/hdf-java-examples/jnative/h5/HDF5AttributeCreate.java
		 * this (Close the attribute.) is missing after the data was read
		 * 
		 */
        try {
            if (isOpen()) {
                if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
        	 		// Terminate access to the file and mem type.
                	getType().closeStringTypes();
         		}

                // Close the dataspace.
                if (getDataspaceId() >= 0) {
                    H5.H5Sclose(getDataspaceId());
                    setDataspaceId(-1);
                }

                // Close the attribute.
                H5.H5Aclose(getAttributeId());
                setOpen(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

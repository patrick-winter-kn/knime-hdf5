package org.knime.hdf5.lib;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
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
	
	private boolean m_open;
	
	private final Hdf5DataType m_type;
	
	/**
	 * Creates an attribute of the type of {@code type}. <br>
	 * Possible types of numbers are Integer, Long, Double and String.
	 * 
	 * @param name
	 * @param value
	 * @throws UnsupportedDataTypeException 
	 */
	public Hdf5Attribute(final String name, final Type[] value) throws UnsupportedDataTypeException {
		m_name = name;
		m_value = value;
		m_type = Hdf5DataType.getTypeByArray(value);
	}
	
	static Hdf5Attribute<?> getInstance(final Hdf5TreeElement treeElement, final String name) {
		Hdf5Attribute<?> attribute = null;
		long attributeId = -1;
		long dataspaceId = -1;
		int lsize = 1;
		
		try {
			Hdf5DataType dataType = treeElement.findAttributeType(name);
			
			try {
				attributeId = H5.H5Aopen(treeElement.getElementId(), name, HDF5Constants.H5P_DEFAULT);
				dataspaceId = H5.H5Aget_space(attributeId);
				
				if (dataspaceId >= 0) {
					int ndims = H5.H5Sget_simple_extent_ndims(dataspaceId);
					if (ndims > 0) {
						long[] dims = new long[ndims];
						H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
	                    for (int j = 0; j < dims.length; j++) {
	                        lsize *= dims[j];
	                    }
	                }
				}
				
				Object[] dataOut = (Object[]) dataType.getKnimeType().createArray(lsize);
				
				if (dataType.isKnimeType(Hdf5KnimeDataType.STRING)) {
	                if (dataType.isVlen()) {
	    				long typeId = H5.H5Tget_native_type(dataType.getConstants()[0]);
	                	String[] strs = new String[lsize];
	                    H5.H5AreadVL(attributeId, typeId, strs);
	                    dataOut = strs;
	    				H5.H5Tclose(typeId);
	                    
	                } else {
	                	long stringLength = dataType.getHdfType().getStringLength();
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
					
				} else if (dataType.equalTypes()) {
					H5.H5Aread(attributeId, dataType.getConstants()[1], dataOut);
					
				} else {
					Object[] dataRead = (Object[]) dataType.getHdfType().createArray(lsize);
	                H5.H5Aread(attributeId, dataType.getConstants()[1], dataRead);
					
					for (int i = 0; i < dataRead.length; i++) {
						dataOut[i] = dataType.hdfToKnime(dataRead[i]);
					}
				}
				
				attribute = dataType.createAttribute(name, dataOut);
				
				attribute.setDataspaceId(dataspaceId);
				attribute.setAttributeId(attributeId);
				attribute.setDimension(lsize);
	    		treeElement.getAttributes().add(attribute);
	        	attribute.setOpen(true);
	        	
			} catch (HDF5DatatypeInterfaceException hdtie) {
				NodeLogger.getLogger("HDF5 Files").error("DataType of attribute \"" 
						+ treeElement.getPathFromFileWithName(true) + name + "\" is not supported", hdtie);
				closeInstance(attributeId, dataspaceId, dataType);
				
			} catch (HDF5Exception | NullPointerException | IllegalArgumentException hnpiae) {
				NodeLogger.getLogger("HDF5 Files").error(hnpiae.getMessage(), hnpiae);
				closeInstance(attributeId, dataspaceId, dataType);
			}
		} catch (UnsupportedDataTypeException udte) {
			NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
		}
		
		return attribute;
	}
	
	private static void closeInstance(final long attributeId, final long dataspaceId, Hdf5DataType dataType) {
		try {
			dataType.getHdfType().closeIfString();

	        if (dataspaceId >= 0) {
	            H5.H5Sclose(dataspaceId);
	        }
	        
	        H5.H5Aclose(attributeId);
	        
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF5 Files").error("Attribute could not be closed", hle);
		}
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
	
	public void close() {
		/* TODO at the site
		 * https://support.hdfgroup.org/ftp/HDF5/hdf-java/hdf-java-examples/jnative/h5/HDF5AttributeCreate.java
		 * this (Close the attribute.) is missing after the data was read
		 * 
		 */
        try {
            if (isOpen()) {
                m_type.getHdfType().closeIfString();

                // Close the dataspace.
                if (m_dataspaceId >= 0) {
                    H5.H5Sclose(m_dataspaceId);
                    m_dataspaceId = -1;
                }

                // Close the attribute.
                H5.H5Aclose(m_attributeId);
                setOpen(false);
            }
        } catch (Exception e) {
			NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + getName() + "\" could not be closed");
        }
	}
}

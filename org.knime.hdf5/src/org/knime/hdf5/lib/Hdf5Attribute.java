package org.knime.hdf5.lib;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Attribute<Type> {

	private final String m_name;
	
	private Type[] m_value;
	
	private long m_dataspaceId = -1;
	
	private long m_attributeId = -1;

	private final Hdf5DataType m_type;
	
	private long m_dimension;
	
	private boolean m_open;
	
	private String m_pathFromFile = "";
	
	private Hdf5TreeElement m_parent;
	
	/**
	 * Creates an attribute with the generic type defined by parameter {@code value}. <br>
	 * Possible types are {@code Integer}, {@code Double} and {@code String}.
	 * 
	 * @param name - the final name of the attribute
	 * @param value - the final data array of the attribute; its type defines the generic type of the attribute
	 * @throws UnsupportedDataTypeException if the type of {@code value} is neither {@code Integer} nor {@code Double} nor {@code String}
	 */
	private Hdf5Attribute(final Hdf5TreeElement parent, final String name, final long dimension,
			final Hdf5DataType type, final boolean create) throws UnsupportedDataTypeException {
		if (name == null) {
			throw new IllegalArgumentException("name cannot be null");
			
		} else if (name.equals("")) {
			throw new IllegalArgumentException("name cannot be the Empty String");
			
		} else if (name.contains("/")) {
			throw new IllegalArgumentException("name \"" + name + "\" cannot contain '/'");
		}
		
		m_name = name;
		m_type = type;
		
		if (create) {
	        try {
	        	createDimension(dimension);
				
	        	// Create the attribute and write the data of the Hdf5Attribute into it.
	            if (parent.getElementId() >= 0 && m_dataspaceId >= 0 && m_type.getConstants()[0] >= 0) {
	            	m_attributeId = H5.H5Acreate(parent.getElementId(), m_name,
	                		m_type.getConstants()[0], m_dataspaceId,
	                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
	            	parent.addAttribute(this);
	            	setOpen(true);
	            }	            
	        } catch (HDF5Exception | NullPointerException hnpe) {
	            NodeLogger.getLogger("HDF5 Files").error("Attribute could not be created", hnpe);
	        }
		} else {
        	parent.addAttribute(this);
        	open();
		}
	}
	
	static Hdf5Attribute<?> getInstance(final Hdf5TreeElement parent, final String name, final long dimension,
			final Hdf5DataType type, final boolean create) {
		Hdf5Attribute<?> attribute = null;
		
		try {
			switch (type.getKnimeType()) {
			case INTEGER:
				attribute = new Hdf5Attribute<Integer>(parent, name, dimension, type, create);
				break;
			case DOUBLE:
				attribute = new Hdf5Attribute<Double>(parent, name, dimension, type, create);
				break;
			case STRING:
				attribute = new Hdf5Attribute<String>(parent, name, dimension, type, create);
				break;
			default:
				NodeLogger.getLogger("HDF5 Files").warn("Attribute \""
						+ parent.getPathFromFileWithName() + name + "\" has an unknown dataType");
				/* attribute stays null */
			}
		} catch (UnsupportedDataTypeException udte) {
			NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
		}
		
		return attribute;
	}
	
	/**
	 * 
	 * @return the name of this attribute
	 */
	public String getName() {
		return m_name;
	}
	
	/**
	 * 
	 * @return the data array of this attribute
	 */
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
	
	/**
	 * 
	 * @return the dataType of the this attribute
	 */
	public Hdf5DataType getType() {
		return m_type;
	}

	/**
	 * 
	 * @return the dimension (length) of the data array of this attribute
	 */
	public long getDimension() {
		return m_dimension;
	}

	boolean isOpen() {
		return m_open;
	}

	void setOpen(boolean open) {
		m_open = open;
	}
	
	public String getPathFromFile() {
		return m_pathFromFile;
	}

	protected void setPathFromFile(String pathFromFile) {
		m_pathFromFile = pathFromFile;
	}

	public Hdf5TreeElement getParent() {
		return m_parent;
	}

	protected void setParent(Hdf5TreeElement parent) {
		m_parent = parent;
	}
	
	public String getPathFromFileWithName() {
		return getPathFromFile() + getName();
	}
	
	public boolean write(final Type[] value) {
        try {
			H5.H5Awrite(m_attributeId, m_type.getConstants()[1], value);
			m_value = value;
			return true;
			
		} catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + getName() + "\" could not be written", hnpe);
		}
        
        return false;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Type[] read() {
		Type[] dataOut = null;
		
		try {
			// TODO long to int cast
			int dim = (int) m_dimension;
			dataOut = (Type[]) m_type.getKnimeType().createArray(dim);
			
			if (m_type.isKnimeType(Hdf5KnimeDataType.STRING)) {
	            if (m_type.isVlen()) {
					long typeId = H5.H5Tget_native_type(m_type.getConstants()[0]);
	                H5.H5AreadVL(m_attributeId, typeId, (String[]) dataOut);
					H5.H5Tclose(typeId);
	                
	            } else {
	            	long stringLength = m_type.getHdfType().getStringLength();
					byte[] dataRead = new byte[dim * ((int) stringLength + 1)];
					H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataRead);
					
					dataOut = (Type[]) new String[dim];
					char[][] dataChar = new char[dim][(int) stringLength + 1];
					for (int i = 0; i < dataChar.length; i++) {
						for (int j = 0; j < dataChar[0].length; j++) {
							dataChar[i][j] = (char) dataRead[i * ((int) stringLength + 1) + j];
						}
						
						dataOut[i] = (Type) String.copyValueOf(dataChar[i]);
					}
				}
				
			} else if (m_type.equalTypes()) {
				H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataOut);
				
			} else {
				Object[] dataRead = (Object[]) m_type.getHdfType().createArray(dim);
	            H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataRead);
	
				Class hdfClass = m_type.getHdfClass();
				Class knimeClass = m_type.getKnimeClass();
				for (int i = 0; i < dataRead.length; i++) {
					dataOut[i] = (Type) m_type.hdfToKnime(hdfClass, hdfClass.cast(dataRead[i]), knimeClass);
				}
			}
		} catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException hludtnpe) {
            NodeLogger.getLogger("HDF5 Files").error(hludtnpe.getMessage(), hludtnpe);
        }
		
		m_value = dataOut;
		return dataOut;
	}
	
	public void open() {
		try {
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				m_attributeId = H5.H5Aopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT);
				setOpen(true);
				loadDimension();
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be opened", hlnpe);
        }
	}
	
	private void createDimension(long dimension) {
		m_dimension = dimension;
		
    	// Create the dataSpace for the dataSet.
        try {
        	long[] dims = { m_dimension };
        	m_dataspaceId = H5.H5Screate_simple(1, dims, null);
            
        } catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("DataSpace could not be created", hnpe);
        }
	}
	
	/**
	 * Updates the dimensions array after opening a dataSet to ensure that the
	 * dimensions array is correct.
	 */
	private void loadDimension() {
		// Get dataSpace and allocate memory for read buffer.
		try {
			if (isOpen()) {
				m_dataspaceId = H5.H5Aget_space(m_attributeId);
				
				long dimension = 1;
				int ndims = H5.H5Sget_simple_extent_ndims(m_dataspaceId);
				if (ndims > 0) {
					long[] dims = new long[ndims];
					H5.H5Sget_simple_extent_dims(m_dataspaceId, dims, null);
	                for (int j = 0; j < dims.length; j++) {
	                    dimension *= dims[j];
	                }
	            }
				
				m_dimension = dimension;
				
			} else {
				throw new IllegalStateException("Attribute is not open");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Dimensions could not be loaded", hlnpe);
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
                //m_type.getHdfType().closeIfString();

                H5.H5Sclose(m_dataspaceId);
                m_dataspaceId = -1;

                H5.H5Aclose(m_attributeId);
                setOpen(false);
            }
        } catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + getName() + "\" could not be closed");
        }
	}
}

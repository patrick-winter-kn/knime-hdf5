package org.knime.hdf5.lib;

import java.util.Arrays;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Representer of an attribute for Hdf5 and the respective flow variable for KNIME.
 * The generic parameter, which can be {@code Integer}, {@code Double} or {@code String},
 * represents the type of the attribute.
 */
public class Hdf5Attribute<Type> {

	private long m_attributeId = -1;
	
	private final String m_name;

	private Hdf5TreeElement m_parent;
	
	private Type[] m_value;

	private Hdf5DataType m_type;
	
	private long m_dataspaceId = -1;
	
	private long m_dimension;
	
	private boolean m_open;
	
	private String m_pathFromFile = "";
	
	private Hdf5Attribute(final String name, final Hdf5DataType type) {
		if (name == null) {
			throw new IllegalArgumentException("name cannot be null");
			
		} else if (name.equals("")) {
			throw new IllegalArgumentException("name cannot be the Empty String");
		}
		
		m_name = name;
		m_type = type;
	}
	
	private static Hdf5Attribute<?> getInstance(final Hdf5TreeElement parent, final String name,
			final Hdf5DataType type) throws IllegalStateException {
		if (parent == null) {
			throw new IllegalArgumentException("Parent group of attribute \"" + name + "\" cannot be null");
			
		} else if (!parent.isOpen()) {
			throw new IllegalStateException("Parent group \"" + parent.getPathFromFileWithName() + "\" is not open");
		
		} else if (type == null) {
			throw new IllegalArgumentException("DataType for attribute \"" + parent.getPathFromFileWithName() + name + "\" cannot be null");
		}
		
		switch (type.getKnimeType()) {
		case INTEGER:
			return new Hdf5Attribute<Integer>(name, type);
		case DOUBLE:
			return new Hdf5Attribute<Double>(name, type);
		case STRING:
			return new Hdf5Attribute<String>(name, type);
		default:
			NodeLogger.getLogger("HDF5 Files").warn("Attribute \"" + name + "\" in \""
					+ parent.getPathFromFileWithName() + "\" has an unknown dataType");
			return null;
		}
	}
	
	static Hdf5Attribute<?> createAttribute(final Hdf5TreeElement parent, final String name,
			final long dimension, final Hdf5DataType type) {
		Hdf5Attribute<?> attribute = null;
		
		try {
			attribute = getInstance(parent, name, type);
			attribute.createDimension(dimension);
        	attribute.setAttributeId(H5.H5Acreate(parent.getElementId(), attribute.getName(),
        			attribute.getType().getConstants()[0], attribute.getDataspaceId(),
                    HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
        	
        	parent.addAttribute(attribute);
        	attribute.setOpen(true);
        	
        } catch (HDF5Exception | NullPointerException | IllegalArgumentException | IllegalStateException hnpiaise) {
            NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + name + "\" in \""
            		+ parent.getPathFromFileWithName() + "\" could not be created successfully: "
            		+ hnpiaise.getMessage(), hnpiaise);
			/* attribute stays null */
        }
		
		return attribute;
	}
	
	static Hdf5Attribute<?> openAttribute(final Hdf5TreeElement parent, final String name) throws UnsupportedDataTypeException {
		Hdf5Attribute<?> attribute = null;
		
		try {
			Hdf5DataType type = parent.findAttributeType(name);
			attribute = getInstance(parent, name, type);
			
	    	parent.addAttribute(attribute);
	    	attribute.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
            NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + name + "\" in \""
            		+ parent.getPathFromFileWithName() + "\" could not be opened: "
					+ npiaise.getMessage(), npiaise);
			/* attribute stays null */
        }
		
		return attribute;
	}

	public static Object[] getFlowVariableValues(FlowVariable flowVariable) {
		switch (flowVariable.getType()) {
		case INTEGER:
			return new Integer[] { flowVariable.getIntValue() };
			
		case DOUBLE:
			return new Double[] { flowVariable.getDoubleValue() };

		default:
			String attr = flowVariable.getValueAsString().trim();
			try {
				if (attr.matches("\\[.*\\] \\(.*\\) *")) {
					String[] parts = attr.split("\\] \\(");
					if (parts.length == 2) {
						String value = parts[0].substring(1);
						String type = parts[1].substring(0, parts[1].length() - 1);
						String[] stringValues = value.isEmpty() ? new String[0] : value.split(", ");
						
						if (type.equals(Hdf5KnimeDataType.INTEGER.toString())) {
							Integer[] intValues = new Integer[stringValues.length];
							for (int i = 0; i < intValues.length; i++) {
								intValues[i] = Integer.parseInt(stringValues[i]);
							}
							return intValues;
							
						} else if (type.equals(Hdf5KnimeDataType.DOUBLE.toString())) {
							Double[] doubleValues = new Double[stringValues.length];
							for (int i = 0; i < doubleValues.length; i++) {
								doubleValues[i] = Double.parseDouble(stringValues[i]);
							}
							return doubleValues;
							
						} else if (type.equals(Hdf5KnimeDataType.STRING.toString())) {
							return stringValues;
						} 
					} else {
						NodeLogger.getLogger(Hdf5Attribute.class).warn("FlowVariable " + flowVariable.getName()
								+ " has incompatible array format (needs exactly one '] (' sequence)");
					}
				}
			} catch (NumberFormatException nfe) {
				// nothing to do
			}
			return new String[] { attr };
		}
	}
	
	long getAttributeId() {
		return m_attributeId;
	}

	void setAttributeId(long attributeId) {
		m_attributeId = attributeId;
	}
	
	/**
	 * Returns the name of this attribute.
	 * 
	 * @return the name of this attribute
	 */
	public String getName() {
		return m_name;
	}

	public Hdf5TreeElement getParent() {
		return m_parent;
	}

	protected void setParent(Hdf5TreeElement parent) {
		m_parent = parent;
	}
	
	/**
	 * Returns the data array as value of this attribute.
	 * 
	 * @return the value of this attribute
	 */
	public Type[] getValue() {
		return m_value;
	}
	
	/**
	 * Returns the dataType of this attribute.
	 * 
	 * @return the dataType of this attribute
	 */
	public Hdf5DataType getType() {
		return m_type;
	}
	
	private long getDataspaceId() {
		return m_dataspaceId;
	}

	/**
	 * Returns the length of the value of this attribute.
	 * 
	 * @return the dimension of this attribute
	 */
	public long getDimension() {
		return m_dimension;
	}

	boolean isOpen() {
		return m_open;
	}

	private void setOpen(boolean open) {
		m_open = open;
	}
	
	/**
	 * Returns the path considering the file containing this attribute as root directory.
	 * 
	 * @return the path from the file containing this attribute
	 */
	public String getPathFromFile() {
		return m_pathFromFile;
	}

	protected void setPathFromFile(String pathFromFile) {
		m_pathFromFile = pathFromFile;
	}
	
	/**
	 * Returns the path from the file containing this attribute concatenated with the name of
	 * this attribute.
	 * 
	 * @return the path from the file containing this attribute with with the name of this
	 * 			attribute
	 */
	public String getPathFromFileWithName() {
		return getPathFromFile() + getName();
	}
	
	/**
	 * Writes {@code value} in this attribute in Hdf5 and saves it as the new value of this
	 * attribute which can be retrieved with {@code getValue()}
	 * 
	 * @param value the new data array which should be written
	 * @return {@code true} if writing was successful,
	 * 			{@code false} otherwise
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean write(final Type[] value, Rounding rounding) {
        try {
			int dim = (int) m_dimension;
        	if (dim > 0) {
            	if (m_type.isHdfType(HdfDataType.STRING)) {
    	            long stringLength = m_type.getHdfType().getStringLength();
    				byte[] dataIn = new byte[dim * ((int) stringLength + 1)];
    				
    				for (int i = 0; i < dim; i++) {
    					char[] dataChar = value[i].toString().toCharArray();
    					
    					for (int j = 0; j < dataChar.length; j++) {
    						dataIn[i * ((int) stringLength + 1) + j] = (byte) dataChar[j];
    					}
    				}
    				
    				H5.H5Awrite(m_attributeId, m_type.getConstants()[1], dataIn);
    				
    			} else if (m_type.hdfTypeEqualsKnimeType()) {
    				H5.H5Awrite(m_attributeId, m_type.getConstants()[1], value);
    				
    			} else {
    				Object[] dataIn = m_type.getHdfType().createArray(dim);
    	
    				Class hdfClass = m_type.getHdfClass();
    				Class knimeClass = m_type.getKnimeClass();
    				for (int i = 0; i < dataIn.length; i++) {
    					dataIn[i] = m_type.knimeToHdf(knimeClass,
    							knimeClass.cast(value[i]), hdfClass, rounding);
    				}
    				
    	            H5.H5Awrite(m_attributeId, m_type.getConstants()[1], dataIn);
    			}
        	}
        	
			m_value = value;
			return true;
			
		} catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException hudtnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + getPathFromFileWithName()
            		+ "\" could not be written: " + hudtnpe.getMessage(), hudtnpe);
		}
        
        return false;
	}
	
	public boolean copyValuesTo(Hdf5Attribute<?> attribute) {
		try {
			return H5.H5Acopy(getAttributeId(), attribute.getAttributeId()) >= 0;
			
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger(getClass()).error(hle.getMessage(), hle);
		}
		
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public boolean clearData() {
		try {
			Type[] dataIn = (Type[]) m_type.getKnimeType().createArray((int) m_dimension);
			Arrays.fill(dataIn, (Type) m_type.getKnimeType().getMissingValue());
			return write(dataIn, Rounding.DOWN);
			
		} catch (UnsupportedDataTypeException udte) {
			NodeLogger.getLogger(getClass()).error(udte.getMessage(), udte);
		}
		
		return false;
	}
	
	/**
	 * Reads the data array of this attribute in Hdf5 and saves it as the new value of this
	 * attribute which can be retrieved with {@code getValue()}.
	 * 
	 * @return the read value of this attribute
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Type[] read() {
		Type[] dataOut = null;
		
		try {
			if (m_dimension > Integer.MAX_VALUE) {
	            NodeLogger.getLogger("HDF5 Files").warn("Attribute \"" + getPathFromFileWithName()
	            		+ "\" contains more values than it could be read");
			}
			int dim = (int) m_dimension;
			dataOut = (Type[]) m_type.getKnimeType().createArray(dim);
			
			if (dim > 0) {
				if (m_type.isHdfType(HdfDataType.STRING)) {
		            if (m_type.isVlen()) {
						long typeId = H5.H5Tget_native_type(m_type.getConstants()[0]);
		                H5.H5AreadVL(m_attributeId, typeId, (String[]) dataOut);
						H5.H5Tclose(typeId);
		                
		            } else {
		            	long stringLength = m_type.getHdfType().getStringLength();
						byte[] dataRead = new byte[dim * ((int) stringLength + 1)];
						H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataRead);
						
						dataOut = (Type[]) new String[dim];
						char[][] dataChar = new char[dim][(int) stringLength];
						for (int i = 0; i < dataChar.length; i++) {
							for (int j = 0; j < dataChar[0].length; j++) {
								dataChar[i][j] = (char) dataRead[i * ((int) stringLength + 1) + j];
							}
							
							dataOut[i] = (Type) String.copyValueOf(dataChar[i]).trim();
						}
					}
				} else if (m_type.hdfTypeEqualsKnimeType()) {
					H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataOut);
					
				} else {
					Object[] dataRead = m_type.getHdfType().createArray(dim);
		            H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataRead);
		
					Class hdfClass = m_type.getHdfClass();
					Class knimeClass = m_type.getKnimeClass();
					for (int i = 0; i < dataRead.length; i++) {
						dataOut[i] = (Type) m_type.hdfToKnime(hdfClass,
								hdfClass.cast(dataRead[i]), knimeClass);
					}
				}
			}
		} catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException hudtnpe) {
            NodeLogger.getLogger("HDF5 Files").error(hudtnpe.getMessage(), hudtnpe);
        }
		
		m_value = dataOut;
		return dataOut;
	}
	
	/**
	 * Opens this attribute. Does nothing if it is already open. If the parent is not open, the
	 * parent will also be opened so that the attribute can be opened too. Also opens the dataSpace
	 * of this attribute to load the dimension of this attribute. <br>
	 * Be careful that the returning {@code boolean} of this method is not the same as calling
	 * {@code isOpen()} afterwards.
	 * 
	 * @return {@code true} if this attribute was not open before and is open now,
	 * 			{@code false} otherwise
	 */
	public boolean open() throws IllegalStateException {
		try {
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				m_attributeId = H5.H5Aopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT);
				
				setOpen(true);
		    	loadDimension();
		    	
				return true;
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + getPathFromFileWithName()
            		+ "\" could not be opened", hlnpe);
        }
		
		return false;
	}
	
	private void createDimension(long dimension) {
		m_dimension = dimension;
		
    	// Create the dataSpace for the dataSet.
        try {
        	long[] dims = { m_dimension };
        	m_dataspaceId = dimension == 1 ? H5.H5Screate(HDF5Constants.H5S_SCALAR) : H5.H5Screate_simple(1, dims, null);
            
        } catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("DataSpace for attribute \""
					+ getPathFromFileWithName() + "\" could not be created", hnpe);
        }
	}
	
	private void loadDimension() throws IllegalStateException {
		// Get dataSpace and allocate memory for read buffer.
		try {
			if (isOpen()) {
				m_dataspaceId = H5.H5Aget_space(m_attributeId);
				
				long dimension = 1;
				int ndims = H5.H5Sget_simple_extent_ndims(m_dataspaceId);
				// scalar attributes have ndims == 0 by definition
				if (ndims > 0) { 
					long[] dims = new long[ndims];
					H5.H5Sget_simple_extent_dims(m_dataspaceId, dims, null);
	                for (int j = 0; j < dims.length; j++) {
	                    dimension *= dims[j];
	                }
	            }
				
				m_dimension = dimension;
				
			} else {
				throw new IllegalStateException("Attribute \""
						+ getPathFromFileWithName() + "\" is not open");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Dimensions for attribute \""
					+ getPathFromFileWithName() + "\" could not be loaded", hlnpe);
        }
	}
	
	/**
	 * Closes this attribute. Does nothing if it is already closed. The dataSpace of this attribute
	 * will also be closed. <br>
	 * Be careful that the returning {@code boolean} of this method is not the same as calling
	 * {@code !isOpen()} afterwards.
	 * 
	 * @return {@code true} if this attribute was open before and is not open now,
	 * 			{@code false} otherwise
	 */
	public boolean close() {
		/* TODO at the site
		 * https://support.hdfgroup.org/ftp/HDF5/hdf-java/hdf-java-examples/jnative/h5/
		 * HDF5AttributeCreate.java
		 * this (Close the attribute.) is missing after the data was read
		 * 
		 */
        try {
            if (isOpen()) {
                H5.H5Sclose(m_dataspaceId);
                m_dataspaceId = -1;

                H5.H5Aclose(m_attributeId);
                
                setOpen(false);
                return true;
            }
        } catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + getPathFromFileWithName()
					+ "\" could not be closed");
        }
        
        return false;
	}
}

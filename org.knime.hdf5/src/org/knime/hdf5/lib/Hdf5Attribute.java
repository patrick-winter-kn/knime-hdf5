package org.knime.hdf5.lib;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Representer of an attribute for HDF5 (accessed through {@code H5A} in the hdf api)
 * and the respective flow variable for KNIME.
 * <br>
 * The generic parameter, which can be {@code Integer}, {@code Double} or {@code String},
 * represents the type of the attribute in KNIME. 
 */
public class Hdf5Attribute<Type> {

	private long m_attributeId = -1;
	
	private final String m_name;

	private Hdf5TreeElement m_parent;
	
	private Type[] m_value;

	private Hdf5DataType m_type;
	
	private long m_dataspaceId = -1;
	
	private long m_dimension;
	
	private String m_pathFromFile = "";

	/**
	 * Manages the access to the opening and closing process of this attribute
	 * to guarantee that no thread closes an open attribute while another
	 * thread is reading it.
	 */
	private final ReentrantReadWriteLock m_openLock = new ReentrantReadWriteLock(true);
	
	private Hdf5Attribute(String name, Hdf5DataType type) {
		if (name == null) {
			throw new IllegalArgumentException("name cannot be null");
			
		} else if (name.equals("")) {
			throw new IllegalArgumentException("name cannot be the empty String");
		}
		
		m_name = name;
		m_type = type;
	}
	
	private static Hdf5Attribute<?> getInstance(Hdf5TreeElement parent, String name,
			Hdf5DataType type) throws IllegalStateException {
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
			NodeLogger.getLogger(Hdf5Attribute.class).warn("Attribute \"" + name + "\" in \""
					+ parent.getPathFromFileWithName() + "\" has an unknown dataType");
			return null;
		}
	}
	
	static Hdf5Attribute<?> createAttribute(Hdf5TreeElement parent, String name,
			long dimension, Hdf5DataType type) throws IOException {
		Hdf5Attribute<?> attribute = null;
		
		try {
			attribute = getInstance(parent, name, type);
			attribute.createDataspace(dimension);

			/*
			 * parent.lockReadOpen() is not needed here because write access
			 * for the whole file is already needed for creating the attribute,
			 * so no other readers or writers can access parent anyway
			 */
        	attribute.setAttributeId(H5.H5Acreate(parent.getElementId(), attribute.getName(),
        			attribute.getType().getConstants()[0], attribute.getDataspaceId(),
                    HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
        	
        	parent.addAttribute(attribute);
        	
        } catch (HDF5Exception | NullPointerException | IllegalArgumentException | IllegalStateException hnpiaise) {
        	throw new IOException("Attribute \"" + name + "\" in \""
            		+ parent.getPathFromFileWithName() + "\" could not be created successfully: "
            		+ hnpiaise.getMessage(), hnpiaise);
        }
		
		return attribute;
	}
	
	static Hdf5Attribute<?> openAttribute(Hdf5TreeElement parent, String name) throws IOException {
		Hdf5Attribute<?> attribute = null;
		
		try {
			Hdf5DataType type = parent.findAttributeType(name);
			attribute = getInstance(parent, name, type);
			
	    	parent.addAttribute(attribute);
	    	attribute.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
        	throw new IOException("Attribute \"" + name + "\" in \""
            		+ parent.getPathFromFileWithName() + "\" could not be opened: "
					+ npiaise.getMessage(), npiaise);
        }
		
		return attribute;
	}

	/**
	 * Returns the value of the flow variable or the array that is represented
	 * by the String flow variable with the structure '[0, 1, ..., n] (knime data type)'
	 * 
	 * @param flowVariable the flow variable
	 * @return the values of the flow variable
	 */
	public static Object[] getFlowVariableValues(FlowVariable flowVariable) {
		switch (flowVariable.getType()) {
		case INTEGER:
			return new Integer[] { flowVariable.getIntValue() };
			
		case DOUBLE:
			return new Double[] { flowVariable.getDoubleValue() };

		default:
			String attr = flowVariable.getValueAsString().trim();
			try {
				// check if the flow variable represents an array
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
								+ " has incompatible array format (needs exactly one '] (' sequence). Its type is a scalar string now.");
					}
				}
			} catch (NumberFormatException nfe) {
				NodeLogger.getLogger(Hdf5Attribute.class).warn("FlowVariable " + flowVariable.getName()
						+ " has wrong type to convert is as array. Its type is a scalar string now.");
			}
			return new String[] { attr };
		}
	}
	
	/**
	 * Updates the data type of this attribute.
	 * 
	 * @throws UnsupportedDataTypeException if the data type is unsupported now
	 * @throws IOException if this attribute does not exist or an internal error
	 * 	occurred
	 * @see Hdf5TreeElement#findAttributeType(String)
	 * @see Hdf5Attribute#loadDataspace()
	 */
	void updateAttribute() throws UnsupportedDataTypeException, IOException {
		m_type = m_parent.findAttributeType(m_name);
	}
	
	long getAttributeId() {
		return m_attributeId;
	}

	void setAttributeId(long attributeId) {
		m_attributeId = attributeId;
	}
	
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
	 * @return the data array (value) of this attribute
	 */
	public Type[] getValue() {
		return m_value;
	}
	
	public Hdf5DataType getType() {
		return m_type;
	}
	
	private long getDataspaceId() {
		return m_dataspaceId;
	}

	/**
	 * @return the length of the value of this attribute
	 */
	public long getDimension() {
		return m_dimension;
	}

	public boolean isOpen() {
		return m_attributeId >= 0;
	}

	/**
	 * @return the path using the hdf file as root directory and '/' as separator
	 */
	public String getPathFromFile() {
		return m_pathFromFile;
	}

	protected void setPathFromFile(String pathFromFile) {
		m_pathFromFile = pathFromFile;
	}
	
	/**
	 * @return the concatenation of the path from file with the name of this
	 * 	attribute (does not change the '/' in the name)
	 */
	public String getPathFromFileWithName() {
		return getPathFromFile() + getName();
	}

	/**
	 * @return if this attribute exists in the hdf file
	 * @throws IOException if an error occurred in the hdf library while
	 * 	checking the existence
	 */
	public boolean exists() throws IOException {
		return m_parent != null && m_parent.existsAttribute(m_name);
	}

	/**
	 * @throws IOException if this attribute does not exist
	 */
	private void checkExists() throws IOException {
    	if (!exists()) {
			throw new IOException("Attribute does not exist");
		}
	}

	/**
	 * @throws IOException if this attribute is not open
	 */
	private void checkOpen() throws IOException {
		checkExists();
    	if (!isOpen()) {
			throw new IOException("Attribute is not open");
		}
	}
	
	/**
	 * Acquires the lock such that no other thread can see/edit if this attribute is
	 * open.
	 */
	private void lockWriteOpen() {
		m_openLock.writeLock().lock();
	}

	/**
	 * @see #lockWriteOpen()
	 */
	private void unlockWriteOpen() {
		m_openLock.writeLock().unlock();
	}

	/**
	 * Acquires the lock such that no other thread can edit if this attribute is
	 * open.
	 */
	private void lockReadOpen() {
		m_openLock.readLock().lock();
	}

	/**
	 * @see #lockReadOpen()
	 */
	private void unlockReadOpen() {
		m_openLock.readLock().unlock();
	}

	/**
	 * @param prefix the prefix for the name like "temp_"
	 * @return the instance for the newly created copy of this attribute
	 * 	in the hdf file
	 * @throws IOException if an error occurred in the hdf library while creating
	 */
	public Hdf5Attribute<?> createBackup(String prefix) throws IOException {
		return m_parent.copyAttribute(this, Hdf5TreeElement.getUniqueName(Arrays.asList(m_parent.loadAttributeNames()), prefix + m_name));
	}
	
	/**
	 * Writes the knime data to this attribute using the rounding if there is a cast
	 * from float to int when converting from knime to hdf.
	 * 
	 * @param dataIn the new data that should be written to this attribute
	 * @return if the knime data was successfully written
	 * @throws IOException if an error occurred in the hdf library
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean write(Type[] dataIn, Rounding rounding) throws IOException {
		Object[] dataWrite = Arrays.copyOf(dataIn, dataIn.length);
		
		if (!m_type.isHdfType(HdfDataType.STRING) && !m_type.hdfTypeEqualsKnimeType()) {
            dataWrite = m_type.getHdfType().createArray((int) m_dimension);
            
			Class knimeClass = m_type.getKnimeClass();
			Class hdfClass = m_type.getHdfClass();
			for (int i = 0; i < dataWrite.length; i++) {
				dataWrite[i] = m_type.knimeToHdf(knimeClass, knimeClass.cast(dataIn[i]), hdfClass, rounding);
			}
		}
		
		m_value = dataIn;
		return writeHdf(dataWrite);
	}
	
	private boolean writeHdf(Object[] dataWrite) throws IOException {
        try {
        	lockReadOpen();
        	checkOpen();
        	checkDimension();
        	
			int dim = (int) m_dimension;
        	if (dim > 0) {
            	if (m_type.isHdfType(HdfDataType.STRING)) {
    	            int stringLength = (int) m_type.getHdfType().getStringLength();
    				byte[] dataByte = new byte[dim * (stringLength + 1)];
    				
    				for (int i = 0; i < dim; i++) {
    					char[] dataChar = dataWrite[i].toString().toCharArray();
    					
    					int length = Math.min(stringLength, dataChar.length);
    					for (int j = 0; j < length; j++) {
    						dataByte[i * (stringLength + 1) + j] = (byte) dataChar[j];
    					}
    				}
    				
    				H5.H5Awrite(m_attributeId, m_type.getConstants()[1], dataByte);
    				
    			} else {
    				H5.H5Awrite(m_attributeId, m_type.getConstants()[1], dataWrite);
    			}
        	}
        	
			return true;
			
		} catch (HDF5Exception | IOException | NullPointerException hionpe) {
            throw new IOException("Attribute \"" + getPathFromFileWithName()
            		+ "\" could not be written: " + hionpe.getMessage(), hionpe);
            
		} finally {
			unlockReadOpen();
		}
	}
	
	/**
	 * Copies the values from {@code attribute} to this attribute using the
	 * rounding if there is a cast from float to int.
	 * 
	 * @param attribute the attribute to be read
	 * @param rounding the rounding for a cast from float to int
	 * @return if the data was successfully written
	 * @throws HDF5DataspaceInterfaceException if the size of
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean copyValuesFrom(Hdf5Attribute<?> attribute, Rounding rounding) throws HDF5DataspaceInterfaceException, IOException {
		try {
			if (m_dimension != attribute.getDimension()) {
				throw new HDF5DataspaceInterfaceException("Attributes \"" + getPathFromFileWithName() + "\" and \""
						+ attribute.getPathFromFileWithName() + "\" do not have the same length");
			}
			
			if (m_type.isSimilarTo(attribute.getType())) {
				try {
					attribute.lockReadOpen();
					lockWriteOpen();
					attribute.checkOpen();
					checkOpen();
					
					return H5.H5Acopy(attribute.getAttributeId(), m_attributeId) >= 0;
					
				} finally {
					attribute.unlockReadOpen();
					unlockWriteOpen();
				}
			}
			
			Object[] dataRead = attribute.readHdf();
			Object[] dataWrite = m_type.getHdfType().createArray((int) m_dimension);

			Hdf5DataType type = attribute.getType();
			Class inputClass = type.getHdfClass();
			Class outputClass = m_type.getHdfClass();
			for (int i = 0; i < dataWrite.length; i++) {
				dataWrite[i] = type.hdfToHdf(inputClass, inputClass.cast(dataRead[i]), outputClass, m_type, rounding);
			}
			
			return writeHdf(dataWrite);

		} catch (HDF5Exception | IOException | NullPointerException hionpe) {
			throw new IOException("Values of attribute \"" + getPathFromFileWithName() + "\" could not be copied to \""
					+ attribute.getPathFromFileWithName() + "\": " + hionpe.getMessage(), hionpe);
	    }
	}

	/**
	 * Sets the data of this attribute back to the standard value which is 0
	 * for numbers and the empty String for Strings.
	 * 
	 * @return if the data was successfully reset
	 * @throws IOException if an error occurred in the hdf library while writing
	 */
	@SuppressWarnings("unchecked")
	public boolean clearData() throws IOException {
		boolean success = false;
		try {
			success = true;
			Type[] dataIn = (Type[]) m_type.getKnimeType().createArray((int) m_dimension);
			Arrays.fill(dataIn, (Type) m_type.getKnimeType().getStandardValue());
			return write(dataIn, Rounding.DOWN);

		} catch (UnsupportedDataTypeException udte) {
			NodeLogger.getLogger(getClass()).error(udte.getMessage(), udte);
		}
		
		return success;
	}
	
	/**
	 * @return the max string length of the values of this attribute
	 * @throws IOException if the values could not be read
	 */
	public long getMaxStringLengthOfValues() throws IOException {
		long maxStringLength = 0L;
		
		if (m_type.isHdfType(HdfDataType.STRING) && !m_type.isVlen()) {
			maxStringLength = m_type.getHdfType().getStringLength();
			
		} else {
			for (Object val : readHdf()) {
				int stringLength = val.toString().length();
				maxStringLength = stringLength > maxStringLength ? stringLength : maxStringLength;
			}
		}
		
		return maxStringLength;
	}
	
	/**
	 * Reads the data array of this attribute in Hdf5 and saves it as the new value of this
	 * attribute which can be retrieved with {@code getValue()}.
	 * 
	 * @return the read value of this attribute
	 * @throws IOException 
	 */

	/**
	 * Reads the data of this attribute.
	 * Returns the knime data after converting from hdf to knime.
	 * 
	 * @return the knime output data
	 * @throws IOException if an error occurred in the hdf library
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Type[] read() throws IOException {
		Type[] dataOut = null;
		
		if (!m_type.isHdfType(HdfDataType.STRING) && !m_type.hdfTypeEqualsKnimeType()) {
			Object[] dataRead = readHdf();
			dataOut = (Type[]) m_type.getKnimeType().createArray(dataRead.length);
			
            Class hdfClass = m_type.getHdfClass();
			Class knimeClass = m_type.getKnimeClass();
			for (int i = 0; i < dataRead.length; i++) {
				dataOut[i] = (Type) m_type.hdfToKnime(hdfClass, hdfClass.cast(dataRead[i]), knimeClass);
			}
		} else {
			dataOut = (Type[]) readHdf();
		}

		m_value = dataOut;
		return dataOut;
	}
	
	private Object[] readHdf() throws IOException {
		try {
			lockReadOpen();
        	checkOpen();
        	checkDimension();

			int dim = (int) m_dimension;
			Object[] dataRead = m_type.getHdfType().createArray(dim);
			
			if (dim > 0) {
				if (m_type.isHdfType(HdfDataType.STRING)) {
		            if (m_type.isVlen()) {
						long typeId = H5.H5Tget_native_type(m_type.getConstants()[0]);
		                H5.H5AreadVL(m_attributeId, typeId, (String[]) dataRead);
						H5.H5Tclose(typeId);
		                
		            } else {
		            	int stringLength = (int) m_type.getHdfType().getStringLength();
						byte[] dataByte = new byte[dim * (stringLength + 1)];
						H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataByte);
						
						char[][] dataChar = new char[dim][stringLength];
						for (int i = 0; i < dataChar.length; i++) {
							for (int j = 0; j < dataChar[i].length; j++) {
								dataChar[i][j] = (char) dataByte[i * (stringLength + 1) + j];
							}
							
							dataRead[i] = String.copyValueOf(dataChar[i]).trim();
						}
					}
				} else {
		            H5.H5Aread(m_attributeId, m_type.getConstants()[1], dataRead);
				}
			}
			
			return dataRead;
			
		} catch (HDF5Exception | IOException | NullPointerException hionpe) {
			throw new IOException("Attribute \"" + getPathFromFileWithName()
					+ "\" could not be read: " + hionpe.getMessage(), hionpe);
        
		} finally {
			unlockReadOpen();
		}
	}
	
	/**
	 * @throws HDF5DataspaceInterfaceException if the length of the dimension of
	 * 	this attribute overflows the Integer values
	 */
	private void checkDimension() throws HDF5DataspaceInterfaceException {
		long numberOfValues = m_dimension;
		
		if (numberOfValues <= Integer.MAX_VALUE && m_type.isHdfType(HdfDataType.STRING)) {
			numberOfValues *= m_type.getHdfType().getStringLength() + 1;
		}

		if (numberOfValues > Integer.MAX_VALUE) {
			throw new HDF5DataspaceInterfaceException("Number of values to read/write in attribute \"" + getPathFromFileWithName()
					+ "\" has overflown the Integer values.");
		}
	}

	/**
	 * Creates the data space with the size of {@code dimension} for this attribute.
	 * 
	 * @param dimension the dimension for this attribute
	 * @throws IOException if an error occurred in the hdf library
	 */
	private void createDataspace(long dimension) throws IOException {
		m_dimension = dimension;
		
    	// Create the dataSpace for this attribute.
        try {
        	long[] dims = { m_dimension };
        	m_dataspaceId = dimension == 1 ? H5.H5Screate(HDF5Constants.H5S_SCALAR) : H5.H5Screate_simple(1, dims, null);
            
        } catch (HDF5Exception | NullPointerException hnpe) {
        	throw new IOException("DataSpace for attribute \""
					+ getPathFromFileWithName() + "\" could not be created", hnpe);
        }
	}

	/**
	 * Loads the data space and updates the dimension.
	 * 
	 * @throws IOException if an error occurred in the hdf library
	 */
	private void loadDataspace() throws IOException, IllegalStateException {
		try {
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
				
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			throw new IOException("Dimension for attribute \""
					+ getPathFromFileWithName() + "\" could not be loaded: " + hlnpe.getMessage(), hlnpe);
        }
	}
	
	/**
	 * Opens this attribute and all of its ancestors (if necessary).
	 * Does nothing if it is already open.
	 * 
	 * @return if this attribute is open
	 * @throws IOException if an error occurred in the hdf library
	 */
	public boolean open() throws IOException, IllegalStateException {
		try {
			lockWriteOpen();
			
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				checkExists();
				
				m_attributeId = H5.H5Aopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT);
				
		    	loadDataspace();
			}
	    	
			return true;
			
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Attribute \"" + getPathFromFileWithName()
					+ "\" could not be opened: " + hlionpe.getMessage(), hlionpe);
			
        } finally {
        	unlockWriteOpen();
        }
	}
	
	/**
	 * Closes this attribute. Does nothing if it is already closed.
	 * 
	 * @return if this attribute is closed
	 * @throws IOException if an error occurred in the hdf library
	 */
	public boolean close() throws IOException {
        try {
        	lockWriteOpen();
        	checkExists();
        	
        	boolean success = true;
        	
            if (isOpen()) {
                success &= H5.H5Sclose(m_dataspaceId) >= 0;
                if (success) {
                	m_dataspaceId = -1;
                }

                success &= H5.H5Aclose(m_attributeId) >= 0;
	    		if (success) {
	    			m_attributeId = -1;
	    		}
            }
            
            return success;
            
        } catch (HDF5LibraryException | IOException hlioe) {
			throw new IOException("Attribute \"" + getPathFromFileWithName()
					+ "\" could not be closed: " + hlioe.getMessage(), hlioe);
		
	    } finally {
	    	unlockWriteOpen();
	    }
	}
	
	@Override
	public String toString() {
		return "{ name=" + m_name + ",pathFromFile=" + m_pathFromFile + ",open=" + isOpen()
				+ ",dimension=" + m_dimension + ",type=" + m_type
				+ (m_value != null ? ",value=" + m_value : "") + " }";
	}
}

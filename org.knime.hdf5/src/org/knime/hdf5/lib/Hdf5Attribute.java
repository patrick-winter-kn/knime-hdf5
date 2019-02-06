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
	
	private final ReentrantReadWriteLock m_openLock = new ReentrantReadWriteLock(true);
	
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
			final long dimension, final Hdf5DataType type) throws IOException {
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
        	throw new IOException("Attribute \"" + name + "\" in \""
            		+ parent.getPathFromFileWithName() + "\" could not be created successfully: "
            		+ hnpiaise.getMessage(), hnpiaise);
        }
		
		return attribute;
	}
	
	static Hdf5Attribute<?> openAttribute(final Hdf5TreeElement parent, final String name) throws IOException {
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
	
	void updateAttribute() throws UnsupportedDataTypeException, IOException {
		m_type = m_parent.findAttributeType(m_name);
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
	
	public boolean exists() throws IOException {
		return m_parent != null && m_parent.existsAttribute(m_name);
	}
	
	private void checkExists() throws IOException {
    	if (!exists()) {
			throw new IOException("Attribute does not exist");
		}
	}
	
	private void checkOpen() throws IOException {
		checkExists();
    	if (!m_open) {
			throw new IOException("Attribute is not open");
		}
	}
	
	private void lockWriteOpen() {
		m_openLock.writeLock().lock();
	}
	
	private void unlockWriteOpen() {
		m_openLock.writeLock().unlock();
	}

	private void lockReadOpen() {
		m_openLock.readLock().lock();
	}
	
	private void unlockReadOpen() {
		m_openLock.readLock().unlock();
	}
	
	/* TODO maybe find a way to use a construction like this:
	private void useOpenLock() {
		try {
			lockOpen();
			
			someMethod();
		
		} finally {
			unlockOpen();
		}
	}
	*/

	public Hdf5Attribute<?> createBackup(String prefix) throws IOException {
		return m_parent.copyAttribute(this, m_parent, Hdf5TreeElement.getUniqueName(Arrays.asList(m_parent.loadAttributeNames()), prefix + m_name));
	}
	
	/**
	 * Writes {@code value} in this attribute in Hdf5 and saves it as the new value of this
	 * attribute which can be retrieved with {@code getValue()}
	 * 
	 * @param value the new data array which should be written
	 * @return {@code true} if writing was successful,
	 * 			{@code false} otherwise
	 * @throws IOException 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean write(final Type[] dataIn, Rounding rounding) throws IOException {
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
	
	public boolean writeHdf(final Object[] dataWrite) throws IOException {
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
	
	@SuppressWarnings("unchecked")
	public boolean clearData() throws IOException {
		Type[] dataIn = (Type[]) m_type.getKnimeType().createArray((int) m_dimension);
		Arrays.fill(dataIn, (Type) m_type.getKnimeType().getMissingValue());
		return write(dataIn, Rounding.DOWN);
	}
	
	public long getStringLength() throws IOException {
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
	
	public Object[] readHdf() throws IOException {
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
	 * Opens this attribute. Does nothing if it is already open. If the parent is not open, the
	 * parent will also be opened so that the attribute can be opened too. Also opens the dataSpace
	 * of this attribute to load the dimension of this attribute.
	 * 
	 * @return {@code true} if this attribute is open
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
				
				setOpen(true);
		    	loadDimension();
			}
	    	
			return true;
			
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Attribute \"" + getPathFromFileWithName()
					+ "\" could not be opened: " + hlionpe.getMessage(), hlionpe);
			
        } finally {
        	unlockWriteOpen();
        }
	}
	
	private void createDimension(long dimension) throws IOException {
		m_dimension = dimension;
		
    	// Create the dataSpace for the dataSet.
        try {
        	long[] dims = { m_dimension };
        	m_dataspaceId = dimension == 1 ? H5.H5Screate(HDF5Constants.H5S_SCALAR) : H5.H5Screate_simple(1, dims, null);
            
        } catch (HDF5Exception | NullPointerException hnpe) {
        	throw new IOException("DataSpace for attribute \""
					+ getPathFromFileWithName() + "\" could not be created", hnpe);
        }
	}
	
	private void loadDimension() throws IOException, IllegalStateException {
		// Get dataSpace and allocate memory for read buffer.
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
	 * Closes this attribute. Does nothing if it is already closed. The dataSpace of this attribute
	 * will also be closed.
	 * 
	 * @return {@code true} if this attribute is closed
	 * @throws IOException 
	 */
	public boolean close() throws IOException {
		/* TODO at the site
		 * https://support.hdfgroup.org/ftp/HDF5/hdf-java/hdf-java-examples/jnative/h5/
		 * HDF5AttributeCreate.java
		 * this (Close the attribute.) is missing after the data was read
		 * 
		 */
        try {
        	lockWriteOpen();
        	checkExists();
        	
        	boolean success = true;
        	
            if (isOpen()) {
                success &= H5.H5Sclose(m_dataspaceId) >= 0;
                
                if (success) {
                	m_dataspaceId = -1;
                    success &= H5.H5Aclose(m_attributeId) >= 0;
                }

                if (success) {
                    setOpen(false);
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
}

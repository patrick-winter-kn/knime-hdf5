package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.MissingCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
	private long m_stringLength;
	
	private final Hdf5DataType m_type;
	
	/**
	 * Creates a dataSet of the type of {@code type}. <br>
	 * Possible types of numbers are Integer, Long, Double and String. <br>
	 * <br>
	 * The last index of the dimensions array represents the maximum length of
	 * the strings plus one more space for the null terminator. <br>
	 * So if you want the stringLength 7, the last index of the array should be 8.
	 * 
	 * @param name
	 * @param dimensions
	 * @param type
	 */
	private Hdf5DataSet(final Hdf5Group parent, final String name, long[] dimensions,
			long stringLength, final Hdf5DataType type, boolean create) {
		super(name, parent.getFilePath());
		m_type = type;

		if (create) {
			createDimensions(dimensions, stringLength);
	        
	        // Create the dataset.
	        try {
	            if (getDataspaceId() >= 0 && getType().getConstants()[0] >= 0) {
	                setElementId(H5.H5Dcreate(parent.getElementId(), getName(),
	                        getType().getConstants()[0], getDataspaceId(),
	                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
	        		NodeLogger.getLogger("HDF5 Files").info("Dataset " + getName() + " created: " + getElementId());
	        		parent.addDataSet(this);
	                setOpen(true);
		        }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		} else {
			parent.addDataSet(this);
			open();
		}
	}
	
	/**
	 * Creates the new instance of the dataSet.
	 * 
	 * @param parent group
	 * @param name
	 * @param dimensions
	 * @param stringLength only matters for String dataSets
	 * @param type
	 * @param create {@code true} if the dataSet should also be created physically in the .h5 file
	 * @return dataSet
	 */
	static Hdf5DataSet<?> getInstance(final Hdf5Group parent, final String name,
			long[] dimensions, long stringLength, Hdf5DataType type, boolean create) {
		if (parent == null) {
			NodeLogger.getLogger("HDF5 Files").error("parent cannot be null",
					new NullPointerException());
			return null;
		} else if (!parent.isOpen()) {
			NodeLogger.getLogger("HDF5 Files").error("parent group " + parent.getName() + " is not open!",
					new IllegalStateException());
			return null;
		} else switch (type.getKnimeType()) {
		case INTEGER:
			return new Hdf5DataSet<Integer>(parent, name, dimensions, 0, type, create);
		case LONG:
			return new Hdf5DataSet<Long>(parent, name, dimensions, 0, type, create);
		case DOUBLE:
			return new Hdf5DataSet<Double>(parent, name, dimensions, 0, type, create);
		case STRING:
			return new Hdf5DataSet<String>(parent, name, dimensions, stringLength, type, create);
		default:
			return null;
		}
	}

	private long getDataspaceId() {
		return m_dataspaceId;
	}

	private void setDataspaceId(long dataspaceId) {
		m_dataspaceId = dataspaceId;
	}

	public long[] getDimensions() {
		return m_dimensions;
	}
	
	private void setDimensions(long[] dimensions) {
		m_dimensions = dimensions;
	}

	public long getStringLength() {
		return m_stringLength;
	}

	private void setStringLength(long stringLength) {
		m_stringLength = stringLength;
	}
	
	public Hdf5DataType getType() {
		return m_type;
	}

	private void createDimensions(long[] dimensions, long stringLength) {
		setDimensions(dimensions);
		
		if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
			setStringLength(stringLength);
			long filetypeId = -1;
			long memtypeId = -1;
			
			// Create file and memory datatypes. For this example we will save
			// the strings as FORTRAN strings, therefore they do not need space
			// for the null terminator in the file.
			try {
				filetypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
				if (filetypeId >= 0) {
					H5.H5Tset_size(filetypeId, getStringLength());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				if (memtypeId >= 0) {
					H5.H5Tset_size(memtypeId, getStringLength() + 1);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			getType().getConstants()[0] = filetypeId;
			getType().getConstants()[1] = memtypeId;
    	}
		
    	// Create the data space for the dataset.
        try {
            setDataspaceId(H5.H5Screate_simple(getDimensions().length,
            		getDimensions(), null));
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public long numberOfValues() {
		return numberOfValuesRange(0, getDimensions().length);
	}

	/**
	 * calculates the number of values/cells which can be saved within the range (fromIndex - toIndex)
	 * of the dimensions array
	 * 
	 * @param fromIndex inclusive
	 * @param toIndex exclusive
	 * @return product of the numbers in the dimensions array in this range 
	 */
	public long numberOfValuesRange(int fromIndex, int toIndex) {
		if (0 <= fromIndex && fromIndex <= toIndex && toIndex <= getDimensions().length) {
			long values = 1;
			long[] dims = Arrays.copyOfRange(getDimensions(), fromIndex, toIndex);
			for (long l: dims) {
				values *= l;
			}
			return values;
		}
		NodeLogger.getLogger("HDF5 Files").error("Hdf5DataSet.numberOfValuesRange(" + fromIndex + ", " + toIndex
				+ "): is out of range (0, " + getDimensions().length + ")", new IllegalArgumentException());
		return 0;
	}
	
	/**
	 * 
	 * @param dataIn
	 * @return {@code true} if the dataSet was written otherwise {@code false}
	 */
	public boolean write(Type[] dataIn) {
		// Write the data to the dataSet.
        try {
			if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
	        	if (isOpen() && (getType().getConstants()[1] >= 0)) {
					H5.H5Dwrite_string(getElementId(), getType().getConstants()[1],
							HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
							HDF5Constants.H5P_DEFAULT, (String[]) dataIn);
					return true;
				}
        	} else {
	            if (isOpen()) {
	                H5.H5Dwrite(getElementId(), getType().getConstants()[1],
	                        HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
	                        HDF5Constants.H5P_DEFAULT, dataIn);
	    			return true;
	            }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public Type[] read(Type[] dataRead) {
		try {
	        if (isOpen()) {
	        	Hdf5DataType dataType = getType();
				
				if (dataType.isKnimeType(Hdf5KnimeDataType.STRING) && dataType.getConstants()[1] >= 0) {
					H5.H5Dread_string(getElementId(), dataType.getConstants()[1],
							HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
							HDF5Constants.H5P_DEFAULT, (String[]) dataRead);
					
				} else if (dataType.equalTypes()) {
		            H5.H5Dread(getElementId(), dataType.getConstants()[1],
		                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
		                    HDF5Constants.H5P_DEFAULT, dataRead);
					
				} else {
					Object[] data = (Object[]) dataType.getHdfType().createArray((int) numberOfValues());
		            H5.H5Dread(getElementId(), dataType.getConstants()[1],
		                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
		                    HDF5Constants.H5P_DEFAULT, data);
					for (int i = 0; i < data.length; i++) {
						dataRead[i] = (Type) dataType.hdfToKnime(data[i]);
					}
				}
			}
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
		
		return dataRead;
	}
	
	public Type getCell(Type[] data, int... indices) {
		int index = 0;
		if (indices.length == getDimensions().length) {
			for (int i = 0; i < indices.length; i++) {
				if (indices[i] >= 0 && indices[i] < getDimensions()[i]) {
					index += indices[i] * (int) numberOfValuesRange(i+1, indices.length);
				} else {
					NodeLogger.getLogger("HDF5 Files").error("Index out of bounds: indices[" + i + "] = "
							+ indices[i] + " and should be from 0 to " + (getDimensions()[i] - 1),
							new IllegalArgumentException());
					return null;
				}
		    }
			return read(data)[index];
		}
		NodeLogger.getLogger("HDF5 Files").error("Not the correct amount (" + getDimensions().length
				+ ") of indices!: " + indices.length, new IllegalArgumentException());
		return null;
	}
	
	public DataCell getDataCell(boolean valid, Type value) {
		if (!valid) {
			return new MissingCell("(null) on joining dataSets");
		}
		
		switch (getType().getKnimeType()) {
		case INTEGER:
			return new IntCell((int) value);
		case LONG:
			return new LongCell((long) value);
		case DOUBLE:
			return new DoubleCell((double) value);
		case STRING:
			return new StringCell((String) value);
		default:
			return null;
		}
	}
	
	public void open() {
		try {
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				setElementId(H5.H5Dopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT));
				setOpen(true);
				updateDimensions();
				NodeLogger.getLogger("HDF5 Files").info("Dataset " + getName() + " opened: " + getElementId());
			}
		} catch (HDF5LibraryException | NullPointerException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Updates the dimensions array after opening a dataset to ensure that the
	 * dimensions array is correct.
	 */
	private void updateDimensions() {
		// Get dataspace and allocate memory for read buffer.
		try {
			if (isOpen()) {
				setDataspaceId(H5.H5Dget_space(getElementId()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		int ndims = -1;
		try {
			if (getDataspaceId() >= 0) {
				ndims = H5.H5Sget_simple_extent_ndims(getDataspaceId());
			}
		} catch (HDF5LibraryException le) {
			le.printStackTrace();
		}
		
		long[] dims = new long[ndims];
        
		try {
			if (getDataspaceId() >= 0) {
				H5.H5Sget_simple_extent_dims(getDataspaceId(), dims, null);
				setDimensions(dims);
			}
		} catch (HDF5LibraryException | NullPointerException lnpe) {
			lnpe.printStackTrace();
		}
        
		if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
			long filetypeId = -1;
			long memtypeId = -1;
			
    		// Get the datatype and its size.
    		try {
    			if (isOpen()) {
    				filetypeId = H5.H5Dget_type(getElementId());
    			}
    			if (filetypeId >= 0) {
    				setStringLength(H5.H5Tget_size(filetypeId));
    				// (+1) for: Make room for null terminator
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
    		// Create the memory datatype.
    		try {
    			memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
    			if (memtypeId >= 0) {
    				H5.H5Tset_size(memtypeId, getStringLength() + 1);
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
			getType().getConstants()[0] = filetypeId;
			getType().getConstants()[1] = memtypeId;
    	}
	}
	
	public void close() {
		// End access to the dataset and release resources used by it.
        try {
            if (isOpen()) {
        		Iterator<Hdf5Attribute<?>> iter = getAttributes().iterator();
        		while (iter.hasNext()) {
        			iter.next().close();
        		}

                if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
        			// Terminate access to the file and mem type.
					H5.H5Tclose(H5.H5Dget_type(getElementId()));
					H5.H5Tclose(H5.H5Tcopy(HDF5Constants.H5T_C_S1));
        		}
             
                // Terminate access to the data space.
                try {
                    if (getDataspaceId() >= 0) {
                    	NodeLogger.getLogger("HDF5 Files").info("Close dataspace of dataset " + getName()
                    			+  ": " + H5.H5Sclose(getDataspaceId()));
                        setDataspaceId(-1);
                    }
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
                
                NodeLogger.getLogger("HDF5 Files").info("Close dataset " + getName()
                		+  ": " + H5.H5Dclose(getElementId()));
                setOpen(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

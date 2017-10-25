package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Iterator;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
	private long m_stringLength;
	
	private Hdf5DataType m_type;
	
	/**
	 * Creates a dataSet of the type of {@code type}. <br>
	 * Possible types of numbers are Integer, Long and Double. <br>
	 * <br>
	 * You can also create a dataSet with Strings by using Byte as dataSet type. <br>
	 * The last index of the dimensions array represents the maximum length of
	 * the strings plus one more space for the null terminator. <br>
	 * So if you want the stringLength 7, the last index of the array should be 8.
	 * 
	 * @param name
	 * @param dimensions
	 * @param type
	 */
	private Hdf5DataSet(final Hdf5Group parent, final String name, long[] dimensions,
			Hdf5DataType type, boolean create) {
		super(name, parent.getFilePath());
		m_type = type;

		if (create) {
			this.setDimensions(dimensions);

			if (this.getType().isString()) {
				this.setStringLength(this.getDimensions()[this.getDimensions().length - 1] - 1);
				long filetypeId = -1;
				long memtypeId = -1;
				
				// Create file and memory datatypes. For this example we will save
				// the strings as FORTRAN strings, therefore they do not need space
				// for the null terminator in the file.
				try {
					filetypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
					if (filetypeId >= 0) {
						H5.H5Tset_size(filetypeId, this.getStringLength());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				try {
					memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
					if (memtypeId >= 0) {
						H5.H5Tset_size(memtypeId, this.getStringLength() + 1);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				this.getType().getConstants()[0] = filetypeId;
				this.getType().getConstants()[1] = memtypeId;
        	}
			
        	// Create the data space for the dataset.
	        try {
				/* The last value of this.getDimensions() is needed to know the stringLength,
				 * and not to have another dimension in the dataSet.
				 */
	        	int stringAdapt = this.getType().isString() ? 1 : 0;
	            this.setDataspaceId(H5.H5Screate_simple(this.getDimensions().length - stringAdapt,
	            		Arrays.copyOfRange(this.getDimensions(), 0, this.getDimensions().length - stringAdapt), null));
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        
	        // Create the dataset.
	        try {
	            if (this.getDataspaceId() >= 0 && this.getType().getConstants()[0] >= 0) {
	                this.setElementId(H5.H5Dcreate(parent.getElementId(), this.getName(),
	                        this.getType().getConstants()[0], this.getDataspaceId(),
	                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
	        		NodeLogger.getLogger("HDF5 Files").info("Dataset " + this.getName() + " created: " + this.getElementId());
	        		parent.addDataSet(this);
	                this.setOpen(true);
		        }
	        } catch (Exception e2) {
	            e2.printStackTrace();
	        }
		} else {
			parent.addDataSet(this);
			this.open();
		}
	}
	
	/**
	 * Creates the new instance of the dataSet.
	 * 
	 * @param parent group
	 * @param name
	 * @param dimensions
	 * @param type
	 * @param create {@code true} if the dataSet should also be created physically in the .h5 file
	 * @return dataSet
	 */
	// TODO here try to do it that the arguments only are group and name, recognize the rest
	static Hdf5DataSet<?> getInstance(final Hdf5Group parent, final String name,
			long[] dimensions, Hdf5DataType type, boolean create) {
		if (parent == null) {
			NodeLogger.getLogger("HDF5 Files").error("parent cannot be null",
					new NullPointerException());
			return null;
		} else if (!parent.isOpen()) {
			NodeLogger.getLogger("HDF5 Files").error("parent group " + parent.getName() + " is not open!",
					new IllegalStateException());
			return null;
		} else switch (type.getTypeId()) {
		case 0:
			return new Hdf5DataSet<Integer>(parent, name, dimensions, type, create);
		case 1:
			return new Hdf5DataSet<Long>(parent, name, dimensions, type, create);
		case 2:
			return new Hdf5DataSet<Double>(parent, name, dimensions, type, create);
		case 3:
			return new Hdf5DataSet<Byte>(parent, name, dimensions, type, create);
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

	// it is needed long[] because of the method H5.H5Screate_simple[int, long[], long[]]
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

	public void setType(Hdf5DataType m_type) {
		this.m_type = m_type;
	}

	public long numberOfValues() {
		return this.numberOfValuesRange(0, this.getDimensions().length);
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
		if (fromIndex >= 0 && toIndex <= this.getDimensions().length) {
			long values = 1;
			long[] dims = Arrays.copyOfRange(this.getDimensions(), fromIndex, toIndex);
			for (long l: dims) {
				values *= l;
			}
			return values;
		}
		NodeLogger.getLogger("HDF5 Files").error("Hdf5DataSet.numberOfValuesRange(" + fromIndex + ", " + toIndex
				+ "): is out of range (0, " + this.getDimensions().length + ")", new IllegalArgumentException());
		return 0;
	}
	
	/**
	 * 
	 * @param dataIn
	 * @return {@code true} if the dataSet was written otherwise {@code false}
	 */
	public boolean write(Type[] dataIn) {
		// Write the data to the dataset.
        try {
			if (this.getType().isString()) {
				int SDIM = (int) this.getDimensions()[this.getDimensions().length - 1];
				byte[][] dbyte = new byte[(int) this.numberOfValuesRange(0, this.getDimensions().length - 1)][SDIM];
				for (int i = 0; i < dataIn.length; i++) {
					dbyte[i/SDIM][i % SDIM] = (byte) dataIn[i];
				}
	        	if (this.isOpen() && (this.getType().getConstants()[1] >= 0)) {
					H5.H5Dwrite(this.getElementId(), this.getType().getConstants()[1], HDF5Constants.H5S_ALL,
							HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, dbyte);
					return true;
				}
        	} else {
	            if (this.isOpen()) {
	                H5.H5Dwrite(this.getElementId(), this.getType().getConstants()[1],
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
		if (this.getType().isString()) {
			int SDIM = (int) this.getDimensions()[this.getDimensions().length - 1];
			byte[][] dbyte = new byte[(int) this.numberOfValuesRange(0, this.getDimensions().length - 1)][SDIM];
			if (this.isOpen() && (this.getType().getConstants()[1] >= 0)) {
				try {
					H5.H5Dread(this.getElementId(), this.getType().getConstants()[1], HDF5Constants.H5S_ALL,
							HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, dbyte);
				} catch (NullPointerException | HDF5Exception e) {
					e.printStackTrace();
				}
				for (int i = 0; i < (int) this.numberOfValuesRange(0, this.getDimensions().length - 1); i++) {
					for (int j = 0; j < SDIM; j++) {
						dataRead[i * SDIM + j] = (Type) (Byte) dbyte[i][j];
					}
				}
			}
		} else {
			try {
		        if (this.isOpen()) {
		            H5.H5Dread(this.getElementId(), this.getType().getConstants()[1],
		                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
		                    HDF5Constants.H5P_DEFAULT, dataRead);
		        }
		    } catch (Exception e) {
		        e.printStackTrace();
		    }
		}
		return dataRead;
	}
	
	public Type getCell(Type[] data, int... indices) {
		int index = 0;
		if (indices.length == this.getDimensions().length) {
			for (int i = 0; i < indices.length; i++) {
				if (indices[i] >= 0 && indices[i] < this.getDimensions()[i]) {
					index += indices[i] * (int) this.numberOfValuesRange(i+1, indices.length);
				} else {
					NodeLogger.getLogger("HDF5 Files").error("Index out of bounds: indices[" + i + "] = "
							+ indices[i] + " and should be from 0 to " + (this.getDimensions()[i] - 1),
							new IllegalArgumentException());
					return null;
				}
		    }
			return this.read(data)[index];
		}
		NodeLogger.getLogger("HDF5 Files").error("Not the correct amount (" + this.getDimensions().length
				+ ") of indices!: " + indices.length, new IllegalArgumentException());
		return null;
	}
	
	/**
	 * This method should only be used for a dataset with the type Byte so that
	 * it is also possible to see the String values.  
	 * 
	 * @param data
	 * @param indices
	 * @return
	 */
	
	public String getStringCell(Byte[] data, int... indices) {
		if (this.getType().isString()) {
			int index = 0;
			int SDIM = (int) this.getStringLength() + 1;
			Byte[] dataByte = new Byte[SDIM];
			if (indices.length == this.getDimensions().length - 1) {
				for (int i = 0; i < indices.length; i++) {
					if (indices[i] >= 0 && indices[i] < this.getDimensions()[i]) {
						index += indices[i] * (int) this.numberOfValuesRange(i+1, indices.length);
					} else {
						NodeLogger.getLogger("HDF5 Files").error("Index out of bounds: indices["
								+ i + "] = " + indices[i] + " and should be from 0 to "
								+ (this.getDimensions()[i] - 1), new IllegalArgumentException());
						return null;
					}
			    }
				dataByte = Arrays.copyOfRange(data, SDIM * index, SDIM * (index + 1));
				char[] dataChar = new char[SDIM];
				for (int i = 0; i < dataByte.length; i++) {
					dataChar[i] = (char) (byte) dataByte[i];
				}
				return String.copyValueOf(dataChar);
			}
			NodeLogger.getLogger("HDF5 Files").error("Not the correct amount (" + (this.getDimensions().length - 1)
					+ ") of indices!: " + indices.length, new IllegalArgumentException());
		} else {
			NodeLogger.getLogger("HDF5 Files").error("Should only be used for datatype Byte!", new UnsupportedOperationException());
		}
		return null;
	}
	
	public void open() {
		try {
			if (!this.isOpen()) {
				this.setElementId(H5.H5Dopen(this.getParent().getElementId(), this.getName(),
						HDF5Constants.H5P_DEFAULT));
				this.setOpen(true);
				this.updateDimensions();
				NodeLogger.getLogger("HDF5 Files").info("Dataset " + this.getName() + " opened: " + this.getElementId());
			}
		} catch (HDF5LibraryException | NullPointerException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Updates the dimensions array after opening a dataset to ensure that the
	 * dimensions array is correct.
	 */
	public void updateDimensions() {
		// Get dataspace and allocate memory for read buffer.
		try {
			if (this.isOpen()) {
				this.setDataspaceId(H5.H5Dget_space(this.getElementId()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		int ndims = -1;
		try {
			if (this.getDataspaceId() >= 0) {
				ndims = H5.H5Sget_simple_extent_ndims(this.getDataspaceId());
			}
		} catch (HDF5LibraryException le) {
			le.printStackTrace();
		}
		
		long[] dims = new long[ndims];
        
		try {
			if (this.getDataspaceId() >= 0) {
				H5.H5Sget_simple_extent_dims(this.getDataspaceId(), dims, null);
			}
		} catch (HDF5LibraryException | NullPointerException lnpe) {
			lnpe.printStackTrace();
		}
        
		if (this.getType().isString()) {
			long filetypeId = -1;
			long memtypeId = -1;
			
    		// Get the datatype and its size.
    		try {
    			if (this.isOpen()) {
    				filetypeId = H5.H5Dget_type(this.getElementId());
    			}
    			if (filetypeId >= 0) {
    				this.setStringLength(H5.H5Tget_size(filetypeId));
    				dims = Arrays.copyOf(dims, ndims + 1);
    		        dims[dims.length - 1] = this.getStringLength() + 1;
    				// (+1) for: Make room for null terminator
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
    		// Create the memory datatype.
    		try {
    			memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
    			if (memtypeId >= 0) {
    				H5.H5Tset_size(memtypeId, this.getStringLength() + 1);
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
			this.getType().getConstants()[0] = filetypeId;
			this.getType().getConstants()[1] = memtypeId;
    	}
        
		this.setDimensions(dims);
	}
	
	public void close() {
		// End access to the dataset and release resources used by it.
        try {
            if (this.isOpen()) {
        		Iterator<Hdf5Attribute<?>> iter = this.getAttributes().iterator();
        		while (iter.hasNext()) {
        			iter.next().close();
        		}

                if (this.getType().isString()) {
        			// Terminate access to the file and mem type.
					H5.H5Tclose(H5.H5Dget_type(this.getElementId()));
					H5.H5Tclose(H5.H5Tcopy(HDF5Constants.H5T_C_S1));
        		}
             
                // Terminate access to the data space.
                try {
                    if (this.getDataspaceId() >= 0) {
                    	NodeLogger.getLogger("HDF5 Files").info("Close dataspace of dataset " + this.getName()
                    			+  ": " + H5.H5Sclose(this.getDataspaceId()));
                        this.setDataspaceId(-1);
                    }
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
                
                NodeLogger.getLogger("HDF5 Files").info("Close dataset " + this.getName()
                		+  ": " + H5.H5Dclose(this.getElementId()));
                this.setOpen(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

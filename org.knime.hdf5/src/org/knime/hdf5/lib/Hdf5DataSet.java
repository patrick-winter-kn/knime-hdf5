package org.knime.hdf5.lib;

import java.util.Arrays;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private final long[] datatype = new long[2];
	private long[] dimensions;
	private long stringLength;
	private boolean string;
	
	// TODO find a way to do it without the parameter Type type
	public Hdf5DataSet(final String name, long[] dimensions, Type type) {
		super(name);
		if (type != null) {
			if (type instanceof Byte) {
				this.string = true;
				this.stringLength = dimensions[dimensions.length - 1] - 1;
				// this.datatype will get values in addToFile()
			} else if (type instanceof Integer) {
				this.datatype[0] = HDF5Constants.H5T_STD_I32LE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_INT32;
			} else if (type instanceof Long) {
				this.datatype[0] = HDF5Constants.H5T_STD_I64LE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_INT64;
			} else if (type instanceof Double) {
				this.datatype[0] = HDF5Constants.H5T_IEEE_F64LE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			} else {
				System.err.println("Hdf5DataSet: Datatype is not supported");
			}
		}
		this.dimensions = dimensions;
	}
	
	public long[] getDatatype() {
		return datatype;
	}

	// changed to long[] because of the method H5.H5Screate_simple[int, long[], long[]]
	public long[] getDimensions() {
		return dimensions;
	}
	
	private void setDimensions(long[] dimensions) {
		this.dimensions = dimensions;
	}

	public long getStringLength() {
		return stringLength;
	}

	public void setStringLength(long stringLength) {
		this.stringLength = stringLength;
	}

	public boolean isString() {
		return string;
	}

	public void setString(boolean string) {
		this.string = string;
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
		System.err.println("Hdf5DataSet.numberOfValuesRange(" + fromIndex + ", " + toIndex
				+ "): is out of range (0, " + this.getDimensions().length + ")");
		return 0;
	}
	
	public void updateDimensions() {
		// Get dataspace and allocate memory for read buffer.
		long dataspace_id = -1;
		try {
			if (this.getElement_id() >= 0) {
				dataspace_id = H5.H5Dget_space(this.getElement_id());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		int ndims = -1;
		try {
			if (dataspace_id >= 0) {
				ndims = H5.H5Sget_simple_extent_ndims(dataspace_id);
			}
		} catch (HDF5LibraryException e1) {
			e1.printStackTrace();
		}
		
		long[] dims = new long[ndims];
		try {
			if (dataspace_id >= 0) {
				H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
			}
		} catch (HDF5LibraryException | NullPointerException e1) {
			e1.printStackTrace();
		}
		
		if (this.isString()) {
			long filetype_id = -1;
			long memtype_id = -1;
			
    		// Get the datatype and its size.
    		try {
    			if (this.getElement_id() >= 0) {
    				filetype_id = H5.H5Dget_type(this.getElement_id());
    			}
    			if (filetype_id >= 0) {
    				this.setStringLength(H5.H5Tget_size(filetype_id));
    				dims = Arrays.copyOf(dims, ndims + 1);
    				dims[dims.length - 1] = this.getStringLength() + 1;
    				// (+1) for: Make room for null terminator
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
    		// Create the memory datatype.
    		try {
    			memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
    			if (memtype_id >= 0) {
    				H5.H5Tset_size(memtype_id, this.getStringLength() + 1);
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
			this.getDatatype()[0] = filetype_id;
			this.getDatatype()[1] = memtype_id;
    	}
		
		// Terminate access to the data space.
        try {
            if (dataspace_id >= 0) {
                H5.H5Sclose(dataspace_id);
            }
        } catch (Exception e2) {
            e2.printStackTrace();
        }

		this.setDimensions(dims);
	}
	
	/**
	 * Adds this dataset to the group.
	 * 
	 * @param group
	 * @return {@code true} if the dataSet is successfully added to the group
	 */
	public boolean addToGroup(Hdf5Group group) {
		if (!group.getPathFromFile().contains("/")) {
			System.err.println("Cannot add dataset to a group which is not added to a group!");
		} else {
			if (group.getElement_id() >= 0 && group.getDataSet(this.getName()) == null) {
				if (this.isString()) {
					long filetype_id = -1;
					long memtype_id = -1;
					
					// Create file and memory datatypes. For this example we will save
					// the strings as FORTRAN strings, therefore they do not need space
					// for the null terminator in the file.
					try {
						filetype_id = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
						if (filetype_id >= 0) {
							H5.H5Tset_size(filetype_id, this.getStringLength());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					try {
						memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
						if (memtype_id >= 0) {
							H5.H5Tset_size(memtype_id, this.getStringLength() + 1);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					this.getDatatype()[0] = filetype_id;
					this.getDatatype()[1] = memtype_id;
	        	}
				
	        	
	        	// Create the data space for the dataset.
	        	long dataspace_id = -1;
	        	
		        try {
		            dataspace_id = H5.H5Screate_simple(this.getDimensions().length - 1, Arrays.copyOfRange(this.getDimensions(), 0, this.getDimensions().length - 1), null);
		        } catch (Exception e2) {
		            e2.printStackTrace();
		        }
		        System.out.println("Dataspace: " + dataspace_id);
		        // Create the dataset.
		        try {
		            if ((group.getElement_id() >= 0) && (dataspace_id >= 0) && (this.getDatatype()[0] >= 0)) {
		                this.setElement_id(H5.H5Dcreate(group.getElement_id(), this.getName(),
		                        this.getDatatype()[0], dataspace_id,
		                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
			            System.out.println("Create: " + group.getPathFromFile() + "/" + this.getName());
						this.setPathFromFile(group.getPathFromFile() + this.getName());
						this.setGroupAbove(group);
		                //group.addDataSet(this);
			        }
		        } catch (Exception e2) {
		            e2.printStackTrace();
		        }
	            
		        // Terminate access to the data space.
		        try {
		            if (dataspace_id >= 0) {
		                H5.H5Sclose(dataspace_id);
		            }
		        } catch (Exception e2) {
		            e2.printStackTrace();
		        }
		        
		        if (this.getElement_id() >= 0) {
		    		return true;
		        }
	        }
		}
        System.out.println("Dataset: " + this.getElement_id());
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public Type[] read(Type[] dataRead) {
		if (this.isString()) {
			int SDIM = (int) this.getDimensions()[this.getDimensions().length - 1];
			byte[][] dbyte = new byte[(int) this.numberOfValuesRange(0, this.getDimensions().length - 1)][SDIM];
			if ((this.getElement_id() >= 0) && (this.getDatatype()[1] >= 0)) {
				try {
					H5.H5Dread(this.getElement_id(), this.getDatatype()[1], HDF5Constants.H5S_ALL,
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
		        if (this.getElement_id() >= 0) {
		            H5.H5Dread(this.getElement_id(), this.getDatatype()[1],
		                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
		                    HDF5Constants.H5P_DEFAULT, dataRead);
		        }
		    } catch (Exception e) {
		        e.printStackTrace();
		    }
		}
		return dataRead;
	}
	
	/**
	 * 
	 * @param dataIn
	 * @return {@code true} if the dataSet was written otherwise {@code false}
	 */
	public boolean write(Type[] dataIn) {
		// Write the data to the dataset.
        try {
			if (this.isString()) {
				int SDIM = (int) this.getDimensions()[this.getDimensions().length - 1];
				byte[][] dbyte = new byte[(int) this.numberOfValuesRange(0, this.getDimensions().length - 1)][SDIM];
				for (int i = 0; i < dataIn.length; i++) {
					dbyte[i/SDIM][i % SDIM] = (byte) dataIn[i];
					System.out.println("dbyte[" + i/SDIM + "][" + i % SDIM + "] = " + (byte) dataIn[i]);
				}
	        	if ((this.getElement_id() >= 0) && (this.getDatatype()[1] >= 0)) {
					H5.H5Dwrite(this.getElement_id(), this.getDatatype()[1], HDF5Constants.H5S_ALL,
							HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, dbyte);
					return true;
				}
        	} else {
	            if (this.getElement_id() >= 0) {
	                H5.H5Dwrite(this.getElement_id(), this.getDatatype()[1],
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
	
	public void close() {
		// End access to the dataset and release resources used by it.
        try {
            if (this.getElement_id() >= 0) {
                H5.H5Dclose(this.getElement_id());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (this.isString()) {
			// Terminate access to the file type.
			try {
				if (this.getDatatype()[0] >= 0) {
					H5.H5Tclose(this.getDatatype()[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	
			// Terminate access to the mem type.
			try {
				if (this.getDatatype()[1] >= 0) {
					H5.H5Tclose(this.getDatatype()[1]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
        
        //System.out.println("Dataset " + this.getName() + " closed.");
	}

	public Type getCell(Type[] data, int... indices) {
		int index = 0;
		if (indices.length == this.getDimensions().length) {
			for (int i = 0; i < indices.length; i++) {
				if (indices[i] >= 0 && indices[i] < this.getDimensions()[i]) {
					index += indices[i] * (int) this.numberOfValuesRange(i+1, indices.length);
				} else {
					System.err.println("Hdf5DataSets.getCell() - Index out of bounds: indices["
							+ i + "] = " + indices[i] + " and should be from 0 to "
							+ (this.getDimensions()[i] - 1));
					return null;
				}
		    }
			return this.read(data)[index];
		}
		System.err.println("Hdf5DataSets.getCell() - Not the correct amount (" 
				+ this.getDimensions().length + ") of indices!: " + indices.length);
		return null;
	}
	
	public String getStringCell(Byte[] data, int... indices) {
		if (this.isString()) {
			int index = 0;
			int SDIM = (int) this.getStringLength() + 1;
			Byte[] dataByte = new Byte[SDIM];
			if (indices.length == this.getDimensions().length - 1) {
				for (int i = 0; i < indices.length; i++) {
					if (indices[i] >= 0 && indices[i] < this.getDimensions()[i]) {
						index += indices[i] * (int) this.numberOfValuesRange(i+1, indices.length);
					} else {
						System.err.println("Hdf5DataSets.getStringCell() - Index out of bounds: indices["
								+ i + "] = " + indices[i] + " and should be from 0 to "
								+ (this.getDimensions()[i] - 1));
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
			System.err.println("Hdf5DataSet.getStringCell() - Not the correct amount (" 
					+ (this.getDimensions().length - 1) + ") of indices!: " + indices.length);
		} else {
			System.err.println("Hdf5DataSet.getStringCell(): Should only be used for datatype Byte!");
		}
		return null;
	}
}

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
		if (type instanceof Integer) {
			this.datatype[0] = HDF5Constants.H5T_STD_I32LE;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_INT32;
		} else if (type instanceof Long) {
			this.datatype[0] = HDF5Constants.H5T_STD_I64LE;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_INT64;
		} else if (type instanceof Double) {
			this.datatype[0] = HDF5Constants.H5T_IEEE_F64LE;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
		} else if (type instanceof String) {
			this.string = true;
			//this.datatype will become values in addToFile()
		} else {
			System.err.println("Hdf5DataSet: Datatype is not supported");
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

	public void updateDimensions() {
		long dataspace_id = -1;
		try {
			dataspace_id = H5.H5Dget_space(getElement_id());
		} catch (HDF5LibraryException e1) {
			e1.printStackTrace();
		}
		
		int ndims = -1;
		try {
			ndims = H5.H5Sget_simple_extent_ndims(dataspace_id);
		} catch (HDF5LibraryException e1) {
			e1.printStackTrace();
		}
		
		long[] dims = new long[ndims];
		try {
			H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
		} catch (HDF5LibraryException | NullPointerException e1) {
			e1.printStackTrace();
		}
		
		System.out.println("Datasetdims: " + dims[0] + "  " + dims[1]);
		
		this.setDimensions(dims);
	}
	
	/**
	 * 
	 * @param fromIndex inclusive
	 * @param toIndex exclusive
	 * @return
	 */
	public long numberOfValues() {
		return this.numberOfValuesRange(0, this.getDimensions().length);
	}
	
	public long numberOfValuesRange(int fromIndex, int toIndex) {
		long values = 1;
		long[] dims = Arrays.copyOfRange(this.getDimensions(), fromIndex, toIndex);
		for (long l: dims) {
			values *= l;
		}
		return values;
	}
	
	/**
	 * 
	 * @param file
	 * @param dspath may not end with '/'
	 * @return {@code true} if the dataSet is successfully added,
	 * 			{@code false} if it already existed (dimensions could be changed) or it couldn't be opened
	 */
	public boolean addToFile(Hdf5File file, String dspath) {
		Hdf5Group group = file;
		// Open dataset using the default properties.
        try {
            if (file.getElement_id() >= 0) {
            	if (!dspath.matches("/?")) {
            		group = Hdf5Group.addToFile(file, dspath);
        		}
            	
                this.setElement_id(H5.H5Dopen(file.getElement_id(), dspath + "/" + this.getName(),
                		HDF5Constants.H5P_DEFAULT));
            	group.addDataSet(this);
            	this.updateDimensions();
            	
            	if (this.isString()) {
            		// Get the datatype and its size.
            		try {
            			if (this.getElement_id() >= 0) {
            				this.getDatatype()[0] = H5.H5Dget_type(this.getElement_id());
            			}
            			if (this.getDatatype()[0] >= 0) {
            				this.setStringLength(H5.H5Tget_size(this.getDatatype()[0]) + 1);
            				// (+1) for: Make room for null terminator
            			}
            		}
            		catch (Exception e) {
            			e.printStackTrace();
            		}

            		// Get dataspace and allocate memory for read buffer.
            		long dataspace_id = -1;
            		try {
            			if (this.getElement_id() >= 0)
            				dataspace_id = H5.H5Dget_space(this.getElement_id());
            		}
            		catch (Exception e) {
            			e.printStackTrace();
            		}

            		try {
            			if (dataspace_id >= 0)
            				H5.H5Sget_simple_extent_dims(dataspace_id, this.getDimensions(), null);
            		}
            		catch (Exception e) {
            			e.printStackTrace();
            		}
            		
            		// Create the memory datatype.
            		try {
            			this.getDimensions()[1] = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
            			if (this.getDimensions()[1] >= 0)
            				H5.H5Tset_size(this.getDimensions()[1], this.getStringLength());
            		}
            		catch (Exception e) {
            			e.printStackTrace();
            		}
            		
            		// Terminate access to the data space.
        	        try {
        	            if (dataspace_id >= 0) {
        	                H5.H5Sclose(dataspace_id);
        	            }
        	        }
        	        catch (Exception e2) {
        	            e2.printStackTrace();
        	        }
            	}
            }
        } catch (HDF5LibraryException e) {
        	if (this.isString()) {
				long filetype_id = -1;
				long memtype_id = -1;
				int SDIM = 8;
				// Create file and memory datatypes. For this example we will save
				// the strings as FORTRAN strings, therefore they do not need space
				// for the null terminator in the file.
				try {
					filetype_id = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
					if (filetype_id >= 0) {
						H5.H5Tset_size(filetype_id, SDIM - 1);
					}
				}
				catch (Exception e2) {
					e.printStackTrace();
				}
				try {
					memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
					if (memtype_id >= 0) {
						H5.H5Tset_size(memtype_id, SDIM);
					}
				}
				catch (Exception e2) {
					e.printStackTrace();
				}
				this.getDatatype()[0] = filetype_id;
				this.getDatatype()[1] = memtype_id;
        	}
			
        	
        	// Create the data space for the dataset.
        	long dataspace_id = -1;
        	
	        try {
	            dataspace_id = H5.H5Screate_simple(this.getDimensions().length, this.getDimensions(), null);
	        } catch (Exception e2) {
	            e2.printStackTrace();
	        }
	        System.out.println("Dataspace: " + dataspace_id);
	        // Create the dataset.
	        try {
	            if ((file.getElement_id() >= 0) && (dataspace_id >= 0)) {
	                this.setElement_id(H5.H5Dcreate(file.getElement_id(), dspath + "/" + this.getName(),
	                        this.getDatatype()[0], dataspace_id,
	                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
		            group.addDataSet(this);
		        }
	        } catch (Exception e2) {
	            e2.printStackTrace();
	        }
            
	        // Terminate access to the data space.
	        try {
	            if (dataspace_id >= 0) {
	                H5.H5Sclose(dataspace_id);
	            }
	        }
	        catch (Exception e2) {
	            e2.printStackTrace();
	        }
	        
	        if (this.getElement_id() >= 0) {
	    		return true;
	        }
        }
        System.out.println("Dataset: " + this.getElement_id());
		return false;
	}

	@SuppressWarnings("unchecked")
	public Type[] read(Type[] dataRead) {
		if (this.isString()) {
			int DIM0 = (int) this.getDimensions()[0];
			int DIM1 = (int) this.getDimensions()[1];
			int SDIM = 8;
			byte[][][] dbyte = new byte[DIM0][DIM1][SDIM];
			if ((this.getElement_id() >= 0) && (this.getDatatype()[1] >= 0)) {
				try {
					H5.H5Dread(this.getElement_id(), this.getDatatype()[1], HDF5Constants.H5S_ALL,
							HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, dbyte);
				} catch (NullPointerException | HDF5Exception e) {
					e.printStackTrace();
				}
				byte[] tempbuf = new byte[SDIM];
				for (int indx = 0; indx < DIM0; indx++) {
					for (int kndx = 0; kndx < DIM1; kndx++) {
						for (int jndx = 0; jndx < SDIM; jndx++) {
							tempbuf[jndx] = dbyte[indx][kndx][jndx];
						}
						dataRead[indx * DIM1 + kndx] = (Type) new String(tempbuf);
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
				int DIM0 = (int) this.getDimensions()[0];
				int DIM1 = (int) this.getDimensions()[1];
				int SDIM = 8;
				byte[][][] dbyte = new byte[DIM0][DIM1][SDIM];
				for (int i = 0; i < dataIn.length; i++) {
					dbyte[i/SDIM/DIM1][(i/SDIM) % DIM1][i % SDIM] = Byte.parseByte((String) dataIn[i]);
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
			}
			catch (Exception e) {
				e.printStackTrace();
			}
	
			// Terminate access to the mem type.
			try {
				if (this.getDatatype()[1] >= 0) {
					H5.H5Tclose(this.getDatatype()[1]);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public Type getCell(Type[] data, int... indices) {
		int index = 0;
		if (indices.length == this.getDimensions().length) {
			for (int i = 0; i < indices.length; i++) {
				index += indices[i] * (int) this.numberOfValuesRange(i+1, indices.length);
		    }
			return this.read(data)[index];
		}
		System.err.println("Hdf5DataSets.getCell(): Not the correct amount of indices!");
		return null;
	}
}

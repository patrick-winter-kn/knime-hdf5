package org.knime.hdf5.lib;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Attribute<Type> {

	private final String name;
	private final Type[] value;
	private final long[] datatype = new long[2];
	private long dataspace_id = -1;
	private long attribute_id = -1;
	private long dimension;
	private boolean string;
	
	public Hdf5Attribute(final String name, final Type[] value) {
		this.name = name;
		this.value = value;
		this.setDimension(value.length);
		if (value[0] instanceof Byte) {
			this.string = true;
			this.setDimension(this.getDimension() + 1);
			//this.datatype will get values in writeToTreeElement()
		} else if (value[0] instanceof Integer) {
			this.datatype[0] = HDF5Constants.H5T_STD_I32BE;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_INT32;
		} else if (value[0] instanceof Long) {
			this.datatype[0] = HDF5Constants.H5T_STD_I64BE;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_INT64;
		}  else if (value[0] instanceof Double) {
			this.datatype[0] = HDF5Constants.H5T_ALPHA_F64;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
		} else {
			System.err.println("Hdf5Attribute: Datatype is not supported");
		}
	}
	
	public String getName() {
		return this.name;
	}
	
	public Type[] getValue() {
		return this.value;
	}

	public long[] getDatatype() {
		return datatype;
	}

	public long getDataspace_id() {
		return dataspace_id;
	}

	public void setDataspace_id(long dataspace_id) {
		this.dataspace_id = dataspace_id;
	}

	public long getAttribute_id() {
		return attribute_id;
	}

	public void setAttribute_id(long attribute_id) {
		this.attribute_id = attribute_id;
	}

	public long getDimension() {
		return dimension;
	}

	public void setDimension(long dimension) {
		this.dimension = dimension;
	}

	public boolean isString() {
		return string;
	}

	public void setString(boolean string) {
		this.string = string;
	}
	
	public void updateDimensions() {
		// Get dataspace and allocate memory for read buffer.
		try {
			if (this.getAttribute_id() >= 0) {
				this.setDataspace_id(H5.H5Aget_space(this.getAttribute_id()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long[] dims = new long[1];
		if (this.isString()) {
			long filetype_id = -1;
			long memtype_id = -1;
			
			// Get the datatype and its size.
			try {
				if (this.getAttribute_id() >= 0) {
					filetype_id = H5.H5Aget_type(this.getAttribute_id());
				}
				if (filetype_id >= 0) {
    				dims[0] = H5.H5Tget_size(filetype_id) + 1;
    				// (+1) for: Make room for null terminator
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Create the memory datatype.
			try {
				memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				if (memtype_id >= 0) {
					H5.H5Tset_size(memtype_id, dims[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			this.getDatatype()[0] = filetype_id;
			this.getDatatype()[1] = memtype_id;
		} else {
			try {
				if (this.getDataspace_id() >= 0) {
					H5.H5Sget_simple_extent_dims(this.getDataspace_id(), dims, null);
				}
			} catch (HDF5LibraryException | NullPointerException e1) {
				e1.printStackTrace();
			}
		}

		this.setDimension(dims[0]);
	}
	
	
	// TODO find a better method name
	/**
	 * 
	 * @param dataSet
	 * @return {@code true} if the attribute is successfully added and written to the dataSet,
	 * 			{@code false} if it already existed (value, dims could be changed) or it couldn't be opened
	 */
	public boolean writeToTreeElement(Hdf5TreeElement treeElement) {
		try {
			System.out.println(this.getName());
            if (treeElement.getElement_id() >= 0) {
                this.setAttribute_id(H5.H5Aopen_by_name(treeElement.getElement_id(), ".", this.getName(), 
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                treeElement.addAttribute(this);
                
                this.updateDimensions();
            }
        } catch (HDF5LibraryException e) {
        	if (this.isString()) {
				long filetype_id = -1;
				long memtype_id = -1;
				
	        	// Create file and memory datatypes. For this example we will save
	    		// the strings as FORTRAN strings, therefore they do not need space
	    		// for the null terminator in the file.
	    		try {
	    			filetype_id = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
	    			if (filetype_id >= 0) {
	    				H5.H5Tset_size(filetype_id, this.getDimension() - 1);
	    			}
	    		} catch (Exception e2) {
	    			e.printStackTrace();
	    		}
	    		
	    		try {
	    			memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
	    			if (memtype_id >= 0) {
	    				H5.H5Tset_size(memtype_id, this.getDimension());
	    			}
	    		} catch (Exception e2) {
	    			e.printStackTrace();
	    		}
	    		
				this.getDatatype()[0] = filetype_id;
				this.getDatatype()[1] = memtype_id;
        	}
        	
        	// Create the data space for the attribute.
        	long[] dims = { isString() ? 1 : this.getDimension() };
            try {
                this.setDataspace_id(H5.H5Screate_simple(1, dims, null));
            }
            catch (Exception e2) {
                e.printStackTrace();
            }

            // Create a dataset attribute.
            try {
                if (treeElement.getElement_id() >= 0 && this.getDataspace_id() >= 0 && this.getDatatype()[0] >= 0) {
                    this.setAttribute_id(H5.H5Acreate(treeElement.getElement_id(), this.getName(),
                    		this.getDatatype()[0], this.getDataspace_id(),
                            HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                    return this.write(treeElement);
                }
            }
            catch (Exception e2) {
                e.printStackTrace();
            }
        }
		return false;
	}
	
	public Type[] read(Type[] attrData) {
		// Allocate array of pointers to two-dimensional arrays (the
        // elements of the dataset. (Type[] attrData)
		
		/*
		// Get dataspace and allocate memory for read buffer.
        try {
            if (this.getAttribute_id() >= 0) {
                this.setDataspace_id(H5.H5Aget_space(this.getAttribute_id()));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            if (this.getDataspace_id() >= 0)
                H5.H5Sget_simple_extent_dims(this.getDataspace_id(), this.getDimensions(), null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        */
		
        // Read data.
        try {
            if (this.getAttribute_id() >= 0 && this.getDatatype()[1] >= 0) {
                H5.H5Aread(this.getAttribute_id(), this.getDatatype()[1], attrData);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        return attrData;
	}
	
	private boolean write(Hdf5TreeElement treeElement) {
		// Write the attribute data.
        try {
            if (this.getAttribute_id() >= 0 && this.getDatatype()[1] >= 0) {
                H5.H5Awrite(this.getAttribute_id(), this.getDatatype()[1], this.getValue());
                treeElement.addAttribute(this);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
	}
	
	public void close() {
		/* TODO at the site
		 * https://support.hdfgroup.org/ftp/HDF5/hdf-java/hdf-java-examples/jnative/h5/HDF5AttributeCreate.java
		 * this (Close the attribute.) is missing after the data was read
		 * 
		 */
		if (this.isString()) {
	 		// Terminate access to the file type.
	 		try {
	 			if (this.getDatatype()[0] >= 0)
	 				H5.H5Tclose(this.getDatatype()[0]);
	 		} catch (Exception e) {
	 			e.printStackTrace();
	 		}
	
	 		// Terminate access to the mem type.
	 		try {
	 			if (this.getDatatype()[1] >= 0)
	 				H5.H5Tclose(this.getDatatype()[1]);
	 		} catch (Exception e) {
	 			e.printStackTrace();
	 		}
 		}
 		
        // Close the attribute.
        try {
            if (this.getAttribute_id() >= 0) {
                H5.H5Aclose(this.getAttribute_id());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Close the dataspace.
        try {
            if (this.getDataspace_id() >= 0) {
                H5.H5Sclose(this.getDataspace_id());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println("Attribute " + this.getName() + " closed.");
	}
}

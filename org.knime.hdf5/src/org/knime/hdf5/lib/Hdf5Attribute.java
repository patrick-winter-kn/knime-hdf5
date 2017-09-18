package org.knime.hdf5.lib;

import java.util.Arrays;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

// TODO Strings/Chars
public class Hdf5Attribute<Type> {

	private final String name;
	private final Type[] value;
	private final long[] dims = new long[1];
	private final long[] datatype = new long[2];
	private long dataspace_id = -1;
	private long attribute_id = -1;
	
	public Hdf5Attribute(final String name, final Type[] value, final long[] dims) {
		this.name = name;
		if (value[0] instanceof String) {
		// TODO try out:
			/*H5Tcopy(H5T_C_S1);
        H5Tset_size(atype, 5);
        H5Tset_strpad(atype,H5T_STR_NULLTERM);*/
			this.value = Arrays.copyOfRange(value, 0, 1);
			this.dims[0] = 1;
			this.datatype[0] = HDF5Constants.H5T_C_S1;
			this.datatype[1] = HDF5Constants.H5T_NATIVE_CHAR;
		} else {
			this.value = value;
			this.dims[0] = dims[0];
			if (value[0] instanceof Byte) {
				this.datatype[0] = HDF5Constants.H5T_STD_I8BE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_INT8;
			} else if (value[0] instanceof Short) {
				this.datatype[0] = HDF5Constants.H5T_STD_I16BE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_INT16;
			} else if (value[0] instanceof Integer) {
				this.datatype[0] = HDF5Constants.H5T_STD_I32BE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_INT32;
			} else if (value[0] instanceof Long) {
				this.datatype[0] = HDF5Constants.H5T_STD_I64BE;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_INT64;
			} else if (value[0] instanceof Float) {
				this.datatype[0] = HDF5Constants.H5T_ALPHA_F32;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_FLOAT;
			} else if (value[0] instanceof Double) {
				this.datatype[0] = HDF5Constants.H5T_ALPHA_F64;
				this.datatype[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
			} else {
				System.err.println("Hdf5Attribute: Datatype is not supported");
			}
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

	public long[] getDims() {
		return dims;
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

	public long numberOfValues() {
		long values = 1;
		for (long l: this.getDims()) {
			values *= l;
		}
		return values;
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
            if (treeElement.getElement_id() >= 0) {
                this.setAttribute_id(H5.H5Aopen_by_name(treeElement.getElement_id(), ".", this.getName(), 
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                treeElement.addAttribute(this);
            }
        }
        catch (Exception e) {
        	// Create the data space for the attribute.
            try {
                this.setDataspace_id(H5.H5Screate_simple(1, this.getDims(), null));
            }
            catch (Exception e2) {
                e.printStackTrace();
            }

            // Create a dataset attribute.
            try {
                if ((treeElement.getElement_id() >= 0) && (this.getDataspace_id() >= 0)) {
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
                H5.H5Sget_simple_extent_dims(this.getDataspace_id(), this.getDims(), null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        // Read data.
        try {
            if (this.getAttribute_id() >= 0) {
                H5.H5Aread(this.getAttribute_id(), this.getDatatype()[1], attrData);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        return attrData;
	}
	
	private boolean write(Hdf5TreeElement treeElement) {
		if (this.getValue()[0] instanceof Character) {
			
		}
		
		// Write the attribute data.
        try {
            if (this.getAttribute_id() >= 0) {
                H5.H5Awrite(this.getAttribute_id(), this.getDatatype()[1], this.getValue());
                treeElement.addAttribute(this);
                return true;
            }
        }
        catch (Exception e) {
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
		
        // Close the attribute.
        try {
            if (this.getAttribute_id() >= 0) {
                H5.H5Aclose(this.getAttribute_id());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Close the dataspace.
        try {
            if (this.getDataspace_id() >= 0) {
                H5.H5Sclose(this.getDataspace_id());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	}
}

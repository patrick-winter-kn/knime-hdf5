package org.knime.hdf5.lib;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Attribute<Type> {

	private final String m_name;
	
	private final Type[] m_value;
	
	private final long[] m_datatype = new long[2];
	
	private long m_dataspaceId = -1;
	
	private long m_attributeId = -1;
	
	private long m_dimension;
	
	private boolean m_open;
	
	private boolean m_string;
	
	/**
	 * Creates an attribute of the type of {@code type}. <br>
	 * Possible types of numbers are Integer, Long and Double. <br>
	 * <br>
	 * You can also create an attribute with Strings by using Byte as attribute type. <br>
	 * {@code value} always needs to be an array.
	 * 
	 * @param name
	 * @param value
	 */
	public Hdf5Attribute(final String name, final Type[] value) {
		m_name = name;
		m_value = value;
		this.setDimension(value.length);
		if (value[0] instanceof Byte) {
			m_string = true;
			this.setDimension(this.getDimension() + 1);
			//this.datatype will get values in writeToTreeElement()
		} else if (value[0] instanceof Integer) {
			m_datatype[0] = HDF5Constants.H5T_STD_I32BE;
			m_datatype[1] = HDF5Constants.H5T_NATIVE_INT32;
		} else if (value[0] instanceof Long) {
			m_datatype[0] = HDF5Constants.H5T_STD_I64BE;
			m_datatype[1] = HDF5Constants.H5T_NATIVE_INT64;
		} else if (value[0] instanceof Double) {
			m_datatype[0] = HDF5Constants.H5T_ALPHA_F64;
			m_datatype[1] = HDF5Constants.H5T_NATIVE_DOUBLE;
		} else {
			// TODO find out if there's another way to find the name "HDF5 Files"
			NodeLogger.getLogger("HDF5 Files").error("Datatype is not supported", new IllegalArgumentException());
		}
	}
	
	public String getName() {
		return m_name;
	}
	
	private Type[] getValue() {
		return m_value;
	}

	private long[] getDatatype() {
		return m_datatype;
	}

	private long getDataspaceId() {
		return m_dataspaceId;
	}

	private void setDataspaceId(long dataspaceId) {
		m_dataspaceId = dataspaceId;
	}

	private long getAttributeId() {
		return m_attributeId;
	}

	private void setAttributeId(long attributeId) {
		m_attributeId = attributeId;
	}

	public long getDimension() {
		return m_dimension;
	}

	private void setDimension(long dimension) {
		m_dimension = dimension;
	}

	private boolean isOpen() {
		return m_open;
	}

	private void setOpen(boolean open) {
		m_open = open;
	}

	private boolean isString() {
		return m_string;
	}

	private void setString(boolean string) {
		m_string = string;
	}
	
	/**
	 * Updates the dimensions array after opening an attribute to ensure that the
	 * dimensions array is correct.
	 */
	
	private void updateDimension() {
		// Get dataspace and allocate memory for read buffer.
		try {
			if (this.getAttributeId() >= 0) {
				this.setDataspaceId(H5.H5Aget_space(this.getAttributeId()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long[] dims = new long[1];
		if (this.isString()) {
			long filetypeId = -1;
			long memtypeId = -1;
			
			// Get the datatype and its size.
			try {
				if (this.getAttributeId() >= 0) {
					filetypeId = H5.H5Aget_type(this.getAttributeId());
				}
				if (filetypeId >= 0) {
    				dims[0] = H5.H5Tget_size(filetypeId) + 1;
    				// (+1) for: Make room for null terminator
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Create the memory datatype.
			try {
				memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
				if (memtypeId >= 0) {
					H5.H5Tset_size(memtypeId, dims[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			this.getDatatype()[0] = filetypeId;
			this.getDatatype()[1] = memtypeId;
		} else {
			try {
				if (this.getDataspaceId() >= 0) {
					H5.H5Sget_simple_extent_dims(this.getDataspaceId(), dims, null);
				}
			} catch (HDF5LibraryException | NullPointerException e1) {
				e1.printStackTrace();
			}
		}

		this.setDimension(dims[0]);
	}
	
	
	// TODO find a better method name
	/**
	 * @param treeElement
	 * @return {@code true} if the attribute is successfully added and written to the dataSet,
	 * 			{@code false} if it already existed (value, dims could be changed) or it couldn't be opened
	 */
	public boolean writeToTreeElement(Hdf5TreeElement treeElement) {
		try {
            if (treeElement.getElementId() >= 0) {
                this.setAttributeId(H5.H5Aopen_by_name(treeElement.getElementId(), ".", this.getName(), 
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                this.setOpen(true);
                NodeLogger.getLogger("HDF5 Files").info("Attribute " + this.getName() + " opened: " + this.getAttributeId());
                treeElement.addAttribute(this);
                
                this.updateDimension();
            }
        } catch (HDF5LibraryException e) {
        	if (this.isString()) {
				long filetypeId = -1;
				long memtypeId = -1;
				
	        	// Create file and memory datatypes. For this example we will save
	    		// the strings as FORTRAN strings, therefore they do not need space
	    		// for the null terminator in the file.
	    		try {
	    			filetypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
	    			if (filetypeId >= 0) {
	    				H5.H5Tset_size(filetypeId, this.getDimension() - 1);
	    			}
	    		} catch (Exception e2) {
	    			e.printStackTrace();
	    		}
	    		
	    		try {
	    			memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
	    			if (memtypeId >= 0) {
	    				H5.H5Tset_size(memtypeId, this.getDimension());
	    			}
	    		} catch (Exception e2) {
	    			e.printStackTrace();
	    		}
	    		
				this.getDatatype()[0] = filetypeId;
				this.getDatatype()[1] = memtypeId;
        	}
        	
        	// Create the data space for the attribute.
        	// the array length can only be 1 for Strings
        	long[] dims = { isString() ? 1 : this.getDimension() };
            try {
                this.setDataspaceId(H5.H5Screate_simple(1, dims, null));
            }
            catch (Exception e2) {
                e.printStackTrace();
            }

            // Create a dataset attribute.
            try {
                if (treeElement.getElementId() >= 0 && this.getDataspaceId() >= 0 && this.getDatatype()[0] >= 0) {
                    this.setAttributeId(H5.H5Acreate(treeElement.getElementId(), this.getName(),
                    		this.getDatatype()[0], this.getDataspaceId(),
                            HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                    this.setOpen(true);
                    NodeLogger.getLogger("HDF5 Files").info("Attribute " + this.getName() + " created: " + this.getAttributeId());
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
        // Read data.
        try {
            if (this.getAttributeId() >= 0 && this.getDatatype()[1] >= 0) {
                H5.H5Aread(this.getAttributeId(), this.getDatatype()[1], attrData);
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
            if (this.getAttributeId() >= 0 && this.getDatatype()[1] >= 0) {
                H5.H5Awrite(this.getAttributeId(), this.getDatatype()[1], this.getValue());
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
        try {
            if (this.isOpen()) {
                if (this.isString()) {
        	 		// Terminate access to the file and mem type.
        			for (long datatype: this.getDatatype()) {
        		 		if (datatype >= 0) {
    		 				H5.H5Tclose(datatype);
    		 				datatype = -1;
    		 			}
        			}
         		}

                // Close the dataspace.
                if (this.getDataspaceId() >= 0) {
                    H5.H5Sclose(this.getDataspaceId());
                    this.setDataspaceId(-1);
                }

                // Close the attribute.
                H5.H5Aclose(this.getAttributeId());
                this.setOpen(false);

                NodeLogger.getLogger("HDF5 Files").info("Attribute " + this.getName() + " closed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

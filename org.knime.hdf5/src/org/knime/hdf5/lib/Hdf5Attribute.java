package org.knime.hdf5.lib;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

//TODO filehandling
public class Hdf5Attribute<Type> {

	private final String name;
	private final Type value;
	private long[] dims;
	private long dataspace_id = -1;
	private long attribute_id = -1;
	
	public Hdf5Attribute(final String name, final Type value, final long[] dims) {
		this.name = name;
		this.value = value;
		this.dims = dims;
	}
	
	public String getName() {
		return this.name;
	}
	
	public Type getValue() {
		return this.value;
	}

	public long[] getDims() {
		return dims;
	}

	public void setDims(long[] dims) {
		this.dims = dims;
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

	// TODO find a better method name
	
	/**
	 * 
	 * @param dataSet
	 * @return {@code true} if the attribute is successfully added and written to the dataSet,
	 * 			{@code false} if it already existed (value, dims could be changed) or it couldn't be opened
	 */
	public boolean writeToDataSet(Hdf5DataSet<?> dataSet) {
		try {
            if (dataSet.getDataset_id() >= 0) {
                this.setAttribute_id(H5.H5Aopen_by_name(dataSet.getDataset_id(), ".", this.getName(), 
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
            }

            // Get dataspace and allocate memory for read buffer.
            try {
                if (this.getAttribute_id() >= 0) {
                    this.setDataspace_id(H5.H5Aget_space(this.getAttribute_id()));
                }
            }
            catch (Exception e) {
                e.printStackTrace();
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
                if ((dataSet.getDataset_id() >= 0) && (this.getDataspace_id() >= 0)) {
                    this.setAttribute_id(H5.H5Acreate(dataSet.getDataset_id(), this.getName(),
                    		HDF5Constants.H5T_STD_I32BE, this.getDataspace_id(),
                            HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                    return this.write();
                }
            }
            catch (Exception e2) {
                e.printStackTrace();
            }
        }
		return false;
	}
	
	private boolean write() {
		// Write the attribute data.
        try {
            if (this.getAttribute_id() >= 0) {
                H5.H5Awrite(this.getAttribute_id(), HDF5Constants.H5T_NATIVE_INT, this.getValue());
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

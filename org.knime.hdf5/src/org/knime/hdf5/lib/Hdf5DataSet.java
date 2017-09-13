package org.knime.hdf5.lib;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

//TODO
public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private final String name;
	private long[] dimensions;
    private long dataset_id = -1;
	
	public Hdf5DataSet(final String name, long[] dimensions) {
		this.name = name;
		this.dimensions = dimensions;
	}
	
	public String getName() {
		return name;
	}
	
	// changed to long[] because of the method H5.H5Screate_simple[int, long[], long[]]
	public long[] getDimensions() {
		return dimensions;
	}
	
	public long getDataset_id() {
		return dataset_id;
	}

	public void setDataset_id(long dataset_id) {
		this.dataset_id = dataset_id;
	}
	
	public long numberOfValues() {
		long values = 1;
		for (long l: this.getDimensions()) {
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
            if (file.getFile_id() >= 0) {
            	if (!dspath.matches("/?")) {
            		group = Hdf5Group.addToFile(file, dspath);
        		}
            	
                this.setDataset_id(H5.H5Dopen(file.getFile_id(), dspath + "/" + this.getName(),
                		HDF5Constants.H5P_DEFAULT));
            	group.addDataSet(this);
                // TODO if the dataSet already exists, the dimensions can be different than set in the constructor
        		// long[] test = {2,3,4};
        		// this.dimensions = test;
            }
            // TODO maybe use a better Exception, more like HDF5DatasetInterfaceException
        } catch (Exception e) {
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
	            if ((file.getFile_id() >= 0) && (dataspace_id >= 0)) {
	                this.setDataset_id(H5.H5Dcreate(file.getFile_id(), dspath + "/" + this.getName(),
	                        HDF5Constants.H5T_STD_I32LE, dataspace_id,
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
	        
	        if (this.getDataset_id() >= 0) {
	    		return true;
	        }
        }
        System.out.println("Dataset: " + this.getDataset_id());
		return false;
	}

	public Type[] read(Type[] dataRead) {
		// Type[] dataRead = new Type[this.numberOfValues()];
		try {
	        if (this.getDataset_id() >= 0) {
	            H5.H5Dread(this.getDataset_id(), HDF5Constants.H5T_NATIVE_INT,
	                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
	                    HDF5Constants.H5P_DEFAULT, dataRead);
	            }
	    } catch (Exception e) {
	        e.printStackTrace();
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
            if (this.getDataset_id() >= 0) {
                H5.H5Dwrite(this.getDataset_id(), HDF5Constants.H5T_NATIVE_INT,
                        HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, dataIn);
            	return true;
            }	
        } catch (Exception e) {
            e.printStackTrace();
        }
		return false;
	}
	
	public void close() {
		// End access to the dataset and release resources used by it.
        try {
            if (this.getDataset_id() >= 0) {
                H5.H5Dclose(this.getDataset_id());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	public Type getCell(int... indices) {
		// TODO
		return null;
	}
}

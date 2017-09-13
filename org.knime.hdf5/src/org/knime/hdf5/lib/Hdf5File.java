package org.knime.hdf5.lib;

import java.io.File;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5FileInterfaceException;

// TODO
public class Hdf5File extends Hdf5Group {
	
	private final String path;
	private long file_id = -1;
	
	/**
	 * Open file using the default properties if it exists at the path.
	 * Otherwise create a new file using default properties.
	 * 
	 * @param path The path to the file. The separator is '/'.
	 */

	// TODO test what happens if you want to create 2 Hdf5File-Objects with the same name
	public Hdf5File(final String path) {
		super(path.substring(path.lastIndexOf(File.separator)));
        this.path = path;
		try {
			this.setFile_id(H5.H5Fopen(this.getPath(), HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT));
        } catch (HDF5FileInterfaceException e) {
	    	try {
				this.setFile_id(H5.H5Fcreate(this.getPath(), HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
			} catch (Exception e2) {
				e2.printStackTrace();
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public String getPath() {
		return path;
	}
	
	public long getFile_id() {
		return file_id;
	}

	public void setFile_id(long file_id) {
		this.file_id = file_id;
		this.setGroup_id(file_id);
	}

	public void close() {
		// Close the file.
		// TODO test if some things in the file are still open
        try {
            if (this.getFile_id() >= 0) {
                H5.H5Fclose(this.getFile_id());
                this.setFile_id(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}
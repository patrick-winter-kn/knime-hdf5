package org.knime.hdf5.lib;

import java.io.File;
import java.util.Iterator;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5FileInterfaceException;

public class Hdf5File extends Hdf5Group {
	
	private final String path;
	
	/**
	 * Open file using the default properties if it exists at the path.
	 * Otherwise create a new file using default properties. <br>
	 * If the File.separator is not '/', the part of the path after the last File.separator
	 * may not contain '/'.
	 * 
	 * @param path The path to the file from the src directory.
	 */

	/* test what happens if you want to create 2 Hdf5File-Objects with the same name:
	 * if you create 2 instances with the same name and you use the close()-method of the 2nd,
	 * you're still able to use the 1st one until you close it too.
	 */
	public Hdf5File(final String path) {
		super(path.substring(path.lastIndexOf(File.separator) + 1));
        this.path = path;
        this.setPathFromFile("/");
		try {
			this.setElement_id(H5.H5Fopen(this.getPath(), HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT));
        } catch (HDF5FileInterfaceException e) {
	    	try {
				this.setElement_id(H5.H5Fcreate(this.getPath(), HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
				//System.out.println("File - Name: " + this.getName() + " ID: " + this.getElement_id());
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
	
	/**
	 * Closes the file and all elements in this file.
	 * 
	 */
	
	public void close() {
        try {
            if (this.getElement_id() >= 0) {
            	Iterator<Hdf5DataSet<?>> iterDS = this.getDataSets().iterator();
	    		while (iterDS.hasNext()) {
	    			Hdf5DataSet<?> dataSet = iterDS.next();
	    			if (dataSet.getElement_id() >= 0) {
	    				dataSet.close();
	    			}
	    		}

            	Hdf5Attribute<?>[] attributes = this.listAttributes();
	    		if (attributes != null) {
	    			for (Hdf5Attribute<?> attribute: attributes) {
	    				if (attribute.getAttribute_id() >= 0) {
	    					attribute.close();
	    				}
	    			}
	    		}
	    		
	    		Iterator<Hdf5Group> iterG = this.getGroups().iterator();
	    		while (iterG.hasNext()) {
	    			Hdf5Group group = iterG.next();
	    			if (group.getElement_id() >= 0) {
	    				group.close();
	    			}
	    		}
	    		
                System.out.println("File " + this.getName() + " closed: " + H5.H5Fclose(this.getElement_id()));
                this.setElement_id(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

package org.knime.hdf5.lib;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5FileInterfaceException;

// TODO
// schauen, ob das die richtige Klasse f�r die Files ist
public class Hdf5File extends Hdf5Group {
	
	private long file_id = -1;
	private final String path;
	
	/**
	 * Open file using the default properties if it exists at the path.
	 * Otherwise create a new file using default properties.
	 * 
	 * @param path The path to the file. The separator is '/'.
	 */
	
	public Hdf5File(final String path) {
        this.path = path;
		try {
			//ausprobieren, ob es Fehler gibt, wenn man 2 Hdf5Files mit demselben Namen erstellt
			setFile_id(H5.H5Fopen(this.getPath(), HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT));
        } catch (HDF5FileInterfaceException e) {
        	try {
				setFile_id(H5.H5Fcreate(this.getPath(), HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
			} catch (Exception e2) {
				e2.printStackTrace();
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public long getFile_id() {
		return file_id;
	}

	public void setFile_id(long file_id) {
		this.file_id = file_id;
	}

	public String getPath() {
		return path;
	}
}

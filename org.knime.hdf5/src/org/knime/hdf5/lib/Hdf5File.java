package org.knime.hdf5.lib;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5FileInterfaceException;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5File extends Hdf5Group {
	
	private static final List<Hdf5File> ALL_FILES = new ArrayList<>();

	/* TODO when opening the file: make a backup of the file because sometimes there were some things wrong with datasets/groups in it
	 * it happened when ...
	 * - creating dataset/group with the same name directly after deleting it in HDFView (not always, only when there were (x is a name) x, x(1), x(2), x(3) and deleted and readded x(2))
	 * - TODO has to be checked if or when it also happens with the method getDataSet() in Hdf5Group
	 */
	private Hdf5File(final String filePath) throws HDF5LibraryException, NullPointerException, IllegalArgumentException {
		super(null, filePath, filePath.substring(filePath.lastIndexOf(File.separator) + 1), true);
		
		ALL_FILES.add(this);
        setPathFromFile("");
		open();
	}
	
	/**
	 * Creating an instance creates a new file or, if there's already a file in this path, opens it. <br>
	 * If the File.separator is not '/', the part of the path after the last File.separator
	 * may not contain '/'.
	 * 
	 * @param filePath The path to the file from the src directory.
	 */
	public static Hdf5File createFile(final String filePath) {
		Iterator<Hdf5File> iter = ALL_FILES.iterator();
		while (iter.hasNext()) {
			Hdf5File file = iter.next();
			if (file.getFilePath().equals(filePath)) {
				file.open();
				return file;
			}
		}
		
		Hdf5File file = null;
		
		try {
			file = new Hdf5File(filePath);
			
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
			NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
		}
		
		return file;
	}
	
	public static Hdf5File openFile(final String filePath) throws IOException {
		if (!new File(filePath).exists()) {
			throw new IOException("The file \"" + filePath + "\" does not exist");
		}
		return createFile(filePath);
	}
	
	/**
	 * 
	 */
	public void open() {
		try {
			if (!isOpen()) {
				setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDWR,
						HDF5Constants.H5P_DEFAULT));
				setOpen(true);
			}
		} catch (HDF5FileInterfaceException fie) {
	    	try {
				setElementId(H5.H5Fcreate(getFilePath(), HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                setOpen(true);
                
            } catch (Exception e) {
                NodeLogger.getLogger("Hdf5 Files").error("The file \"" + getFilePath() + "\" cannot be created", e);
			}
        } catch (Exception e) {
            NodeLogger.getLogger("Hdf5 Files").error("The file \"" + getFilePath() + "\" cannot be opened", e);
        }
	}
	
	public String whatIsOpen() {
        long count = -1;
        long openedObjects = -1;
        long[] objects;
        String[] objTypes = { "Unknown object", "File", "Group", "DataType", "DataSpace", "DataSet", "Attribute" };
        String opened = "";
		
        try {
        	if (isOpen()) {
        		count = H5.H5Fget_obj_count(getElementId(), HDF5Constants.H5F_OBJ_ALL);
			} else {
				return "(error: file already closed)";
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF5 Files").error("Number of opened objects in file could not be loaded", hle);
		}

        if (count <= 0) {
        	return "(error: couldn't find out number of opened objects)";
        }
        
        if (count == 1) {
        	return "0";
        }

        objects = new long[(int) count];
        opened += (count - 1) + " [";

        try {
			openedObjects = H5.H5Fget_obj_ids(getElementId(), HDF5Constants.H5F_OBJ_ALL, count, objects);

	        // i = 0 is just the file itself
	        if (openedObjects > 1) {
	    		String pathFromFile = H5.H5Iget_name(objects[1]);
	    		String objectType = objTypes[(int) H5.H5Iget_type(objects[1])];
	    		opened += "\"" + pathFromFile + "\" (" +  objectType + ")";
	        }
	        
	        for (int i = 2; i < openedObjects; i++) {
	        	String objectType = objTypes[(int) H5.H5Iget_type(objects[i])];
        		String pathFromFile = H5.H5Iget_name(objects[i]);
	    		opened += ", \"" + pathFromFile + "\" (" +  objectType + ")";
	        }
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Info of opened objects in file could not be loaded", hlnpe);
        }
        
        opened += "]";
        
        return opened;
	}

	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
	@Override
	public void close() {
		try {
            if (isOpen()) {
            	Iterator<Hdf5DataSet<?>> iterDss = getDataSets().iterator();
	    		while (iterDss.hasNext()) {
	    			iterDss.next().close();
	    		}

	    		Iterator<Hdf5Attribute<?>> iterAttrs = getAttributes().iterator();
	    		while (iterAttrs.hasNext()) {
	    			iterAttrs.next().close();
	    		}

	    		Iterator<Hdf5Group> iterGrps = getGroups().iterator();
	    		while (iterGrps.hasNext()) {
	    			iterGrps.next().close();
	    		}
	    		
	    		NodeLogger.getLogger("HDF5 Files").debug("Number of open objects in file \""
	    				+ getName() + "\": " + whatIsOpen());
				H5.H5Fclose(getElementId());
                setOpen(false);
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("File could not be closed", hle);
        }
	}
}

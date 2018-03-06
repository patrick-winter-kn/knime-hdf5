package org.knime.hdf5.lib;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5File extends Hdf5Group {

	public static final int READ_ONLY_ACCESS = 0;
	
	public static final int READ_WRITE_ACCESS = 1;
	
	private static final List<Hdf5File> ALL_FILES = new ArrayList<>();

	private int m_access = -1;

	private List<Long> m_accessors = new ArrayList<>();
	
	/* TODO when opening the file: make a backup of the file because sometimes there were some things wrong with dataSets/groups in it
	 * it happened when ...
	 * - creating dataSet/group with the same name directly after deleting it in HDFView (not always, only when there were (x is a name) x, x(1), x(2), x(3) and deleted and readded x(2))
	 * - TODO has to be checked if or when it also happens with the method Hdf5Group.getDataSet()
	 */
	private Hdf5File(final String filePath, final boolean create, final int access)
			throws HDF5LibraryException, NullPointerException, IllegalArgumentException {
		super(null, filePath, filePath.substring(filePath.lastIndexOf(File.separator) + 1), true);
		
		ALL_FILES.add(this);
        setPathFromFile("");
        
        if (create) {
        	create();
        } else {
    		open(access);
        }
	}
	
	/**
	 * Creating an instance creates a new file or, if there's already a file in this path, opens it. <br>
	 * If the File.separator is not '/', the part of the path after the last File.separator
	 * may not contain '/'.
	 * 
	 * @param filePath The path to the file from the src directory.
	 */
	public synchronized static Hdf5File createFile(final String filePath) throws IOException {
		if (new File(filePath).exists()) {
			throw new IOException("The file \"" + filePath + "\" does already exist");
		}
		
		Hdf5File file = null;
		try {
			file = new Hdf5File(filePath, true, READ_WRITE_ACCESS);
			
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
			NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
		}
		
		return file;
	}
	
	public static Hdf5File openFile(final String filePath, final int access) throws IOException {
		if (!new File(filePath).exists()) {
			throw new IOException("The file \"" + filePath + "\" does not exist");
		}
		
		Iterator<Hdf5File> iter = ALL_FILES.iterator();
		while (iter.hasNext()) {
			Hdf5File file = iter.next();
			if (file.getFilePath().equals(filePath)) {
				file.open(access);
				return file;
			}
		}

		Hdf5File file = null;
		try {
			file = new Hdf5File(filePath, false, access);
			
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
			NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
		}
		
		file.whatIsOpen();
		
		return file;
	}
	
	protected boolean isOpen() {
		// TODO find another way to use this id that an Hdf5File can have more than 1 id at the same time
		return m_accessors.contains(getElementId());
	}
			
	protected synchronized void setOpen(final boolean open, final int access) {
		if (open && !isOpen()) {
			if (m_accessors.isEmpty()) {
				m_access = access;
			}
			m_accessors.add(getElementId());
			
		} else if (!open && isOpen()) {
			m_accessors.remove(getElementId());
		}
	}
	
	private void create() {
		try {
			setElementId(H5.H5Fcreate(getFilePath(), HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
            setOpen(true, READ_WRITE_ACCESS);
            
        } catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("Hdf5 Files").error("The file \"" + getFilePath() + "\" cannot be created", hlnpe);
		}
	}
	
	public synchronized void open(final int access) {
		try {
			if (!isOpen()) {
				if (access == READ_ONLY_ACCESS) {
					while (m_access == READ_WRITE_ACCESS && !m_accessors.isEmpty()) {
						try {
							wait();
						} catch (InterruptedException ie) {}
					}
					
					setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDONLY,
							HDF5Constants.H5P_DEFAULT));
					
				} else if (access == READ_WRITE_ACCESS) {
					while (!m_accessors.isEmpty()) {
						try {
							wait();
						} catch (InterruptedException ie) {}
					}
					
					setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDWR,
							HDF5Constants.H5P_DEFAULT));
				}
				
				setOpen(true, access);
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("Hdf5 Files").error("The file \"" + getFilePath() + "\" cannot be opened", hlnpe);
        }
	}
	
	private String whatIsOpen() {
        long count = -1;
        long openObjects = -1;
        long[] objects;
        String[] objTypes = { "Unknown objectType", "File", "Group", "DataType", "DataSpace", "DataSet", "Attribute" };
        String opened = "";
		
        try {
        	if (isOpen()) {
        		int openInOtherThreads = m_accessors.size() - 1;
        		if (openInOtherThreads > 0) {
    		        return "(info: file is open in " + openInOtherThreads + " other threads)";
        		}
        		
        		count = H5.H5Fget_obj_count(getElementId(), HDF5Constants.H5F_OBJ_ALL);
        		
			} else {
				return "(error: file is already closed)";
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF5 Files").error("Number of open objects in file could not be loaded", hle);
		}

        if (count <= 0) {
        	return "(error: couldn't find out number of open objects)";
        }
        
        if (count == 1) {
        	return "0";
        }

        objects = new long[(int) count];
		opened += (count - 1);

        try {
			openObjects = H5.H5Fget_obj_ids(getElementId(), HDF5Constants.H5F_OBJ_ALL, count, objects);
			   
			// i = 0 is just the file itself
			int i = 1;
	        if (i < openObjects) {
	    		String pathFromFile = H5.H5Iget_name(objects[i]);
	    		String objectType = objTypes[(int) H5.H5Iget_type(objects[i])];
	    		opened += " [\"" + pathFromFile + "\" (" +  objectType + ")";
	        }
	        for (i = 2; i < openObjects; i++) {
	        	String objectType = objTypes[(int) H5.H5Iget_type(objects[i])];
        		String pathFromFile = H5.H5Iget_name(objects[i]);
	    		opened += ", \"" + pathFromFile + "\" (" +  objectType + ")";
	        }
	        
	        opened += "]";
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Info of open objects in file could not be loaded", hlnpe);
        }
        
        return opened;
	}

	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
	@Override
	public synchronized void close() {
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
                setOpen(false, m_access);
                
				if (m_accessors.isEmpty()) {
					if (m_access == READ_ONLY_ACCESS) {
						notify();
						
					} else if (m_access == READ_WRITE_ACCESS) {
						notifyAll();
					}
				}
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("File \"" + getName()
            		+ "\" could not be closed", hle);
        }
	}
}

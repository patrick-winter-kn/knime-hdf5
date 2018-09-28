package org.knime.hdf5.lib;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5File extends Hdf5Group {

	public static final int NOT_ACCESSED = -1;
	
	public static final int READ_ONLY_ACCESS = 0;
	
	public static final int READ_WRITE_ACCESS = 1;
	
	// private static final long START = System.nanoTime();
	
	private static final List<Hdf5File> ALL_FILES = new ArrayList<>();
	
	private final ReentrantReadWriteLock m_rwl = new ReentrantReadWriteLock(true);
	
	private final Lock m_r = m_rwl.readLock();
	
	private final Lock m_w = m_rwl.writeLock();
	
	private int m_access = NOT_ACCESSED;
	
	private Map<Thread, Integer> m_accessors = new HashMap<>();
	
	/* TODO check if there are problems when creating/deleting dataSets in HdfView at the same time as in the HdfWriter
	 * (e.g. there are x, x(1), x(2) and x(3); delete and create x(2) again)
	 */
	private Hdf5File(final String filePath) throws HDF5LibraryException, NullPointerException,
			IllegalArgumentException {
		super(filePath.substring(filePath.lastIndexOf(File.separator) + 1), filePath);
		
		ALL_FILES.add(this);
        setPathFromFile("");
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
			file = new Hdf5File(filePath);
			file.create();
			
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
			NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
		}
		
		return file;
	}
	
	public synchronized static Hdf5File openFile(final String filePath, final int access) throws IOException {
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
			file = new Hdf5File(filePath);
			file.open(access);
			
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
			NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
		}
		
		return file;
	}

	public synchronized static boolean existsHdfFile(final String filePath) {
		return hasHdf5FileEnding(filePath) && new File(filePath).exists();
	}
	
	public synchronized static boolean isHdfFileCreatable(final String filePath) {
		return hasHdf5FileEnding(filePath) && new File(getDirectoryPath(filePath)).isDirectory()
				&& !new File(filePath).exists();
	}
	
	public static boolean hasHdf5FileEnding(final String filePath) {
		return filePath.endsWith(".h5") || filePath.endsWith(".hdf5");
	}
	
	public static String getDirectoryPath(String filePath) {
		int dirPathLength = filePath.lastIndexOf(File.separator);
		return filePath.substring(0, dirPathLength >= 0 ? dirPathLength : 0);
	}
	
	protected boolean isOpen() {
		return m_accessors.containsKey(Thread.currentThread());
	}

	protected synchronized void setOpen(boolean open) {
		Thread curThread = Thread.currentThread();
		if (!isOpen() && open) {
			m_accessors.put(curThread, 1);
		} else if (isOpen()) {
			m_accessors.put(curThread, m_accessors.get(curThread) + (open ? 1 : -1));
		}
		// System.out.println(String.format("%,16d", // System.nanoTime() - START) + " " + curThread + ": \"" + getFilePath() + "\" is open " + m_accessors.get(curThread) + " times");
		if (m_accessors.get(curThread) == 0) {
			m_accessors.remove(curThread);
		}
	}
	
	private boolean isOpenExactlyOnceInThisThread() {
		return isOpen() && m_accessors.get(Thread.currentThread()) == 1;
	}
	
	private boolean isOpenOnlyInThisThread() {
		return isOpen() && m_accessors.size() == 1;
	}
	
	private boolean isOpenInAnyThread() {
		return !m_accessors.isEmpty();
	}
	
	private void create() {
		try {
			// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " LOCK WRITE end of \"" + getFilePath() + "\" ... ");
			m_w.lock();
			// System.out.println("successful");
			setElementId(H5.H5Fcreate(getFilePath(), HDF5Constants.H5F_ACC_TRUNC,
					HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
			m_access = READ_WRITE_ACCESS;
			setOpen(true);
            
        } catch (HDF5LibraryException | NullPointerException hlnpe) {
			// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " UNLOCK WRITE end of \"" + getFilePath() + "\" ... ");
            m_w.unlock();
			// System.out.println("successful");
            NodeLogger.getLogger("Hdf5 Files").error("The file \"" + getFilePath() + "\" cannot be created : " + hlnpe.getMessage(), hlnpe);
        }
	}
	
	public void open(final int access) {
		try {
			if (!isOpen()) {
				if (access == READ_ONLY_ACCESS) {
					// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " LOCK READ end of \"" + getFilePath() + "\" ... ");
					m_r.lock();
					// System.out.println("successful");
					synchronized (this) {
						if (!isOpenInAnyThread()) {
							setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDONLY,
									HDF5Constants.H5P_DEFAULT));
						}
						setOpen(true);
					}
					
				} else if (access == READ_WRITE_ACCESS) {
					// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " LOCK WRITE end of \"" + getFilePath() + "\" ... ");
					m_w.lock();
					// System.out.println("successful");
					synchronized (this) {
						if (!isOpenInAnyThread()) {
							setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDWR,
									HDF5Constants.H5P_DEFAULT));
						}
						setOpen(true);
					}
				}
				m_access = access;
			} else {
				setOpen(true);
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			if (access == READ_ONLY_ACCESS) {
				// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " UNLOCK READ end of \"" + getFilePath() + "\" ... ");
				m_r.unlock();
				// System.out.println("successful");
				
			} else if (access == READ_WRITE_ACCESS) {
				// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " UNLOCK WRITE end of \"" + getFilePath() + "\" ... ");
				m_w.unlock();
				// System.out.println("successful");
			}
            NodeLogger.getLogger("Hdf5 Files").error("The file \"" + getFilePath() + "\" cannot be opened: " + hlnpe.getMessage(), hlnpe);
        }
	}
	
	private String whatIsOpenInFile() {
        long count = -1;
        long openObjects = -1;
        long[] objects;
        String[] objTypes = { "Unknown objectType", "File", "Group", "DataType", "DataSpace", "DataSet", "Attribute" };
        String opened = "";
		
        try {
        	if (isOpen()) {
        		count = H5.H5Fget_obj_count(getElementId(), HDF5Constants.H5F_OBJ_ALL);
        		
			} else {
				return "(error: file is already closed)";
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger("HDF5 Files").debug("Number of open objects in file could not be loaded: " + hle.getMessage(), hle);
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
	    		opened += " [\"" + pathFromFile + "\"";
	    		if (H5.H5Iis_valid(objects[i])) {
		    		String objectType = objTypes[(int) H5.H5Iget_type(objects[i])];
		    		opened += " (" + objectType + ")";
	    		}
	        }
	        for (i = 2; i < openObjects; i++) {
        		String pathFromFile = H5.H5Iget_name(objects[i]);
	    		opened += ", \"" + pathFromFile + "\"";
	    		if (H5.H5Iis_valid(objects[i])) {
		        	String objectType = objTypes[(int) H5.H5Iget_type(objects[i])];
		    		opened += " (" + objectType + ")";
	    		}
	        }
	        
	        opened += "]";
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").debug("Info of open objects in file could not be loaded: " + hlnpe.getMessage(), hlnpe);
        }
        
        return opened;
	}

	private String whatIsOpen() {
		String opened = "";
		
		try { 
			int fileNum = 0;
			for (long id: H5.getOpenIDs()) {
				if (H5.H5Iis_valid(id) && H5.H5Iget_type(id) == HDF5Constants.H5I_FILE) {
					fileNum++;
				}
			}
			
			opened = H5.getOpenIDCount() + " objects, " + (fileNum - 1) + " other files " + H5.getOpenIDs();
			
		} catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").debug("Info of open objects in total could not be loaded: " + hle.getMessage(), hle);
		}
		
		return opened;
	}
	
	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
	@Override
	public boolean close() {
		try {
			if (isOpen()) {
				if (isOpenExactlyOnceInThisThread()) {
		    		synchronized (this) {
			    		if (isOpenOnlyInThisThread()) {
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
				    				+ getName() + "\": " + whatIsOpenInFile() + " (total number: " + whatIsOpen() + ")");
				    		
				    		// System.out.println(whatIsOpen());
				    		
							H5.H5Fclose(getElementId());
			    		}
			    		setOpen(false);
	  
						if (m_access == READ_ONLY_ACCESS) {
							// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " UNLOCK READ end of \"" + getFilePath() + "\" ... ");
							m_r.unlock();
							// System.out.println("successful");
						} else if (m_access == READ_WRITE_ACCESS) {
							// System.out.print(String.format("%,16d", // System.nanoTime() - START) + " " + Thread.currentThread() + " UNLOCK WRITE end of \"" + getFilePath() + "\" ... ");
							m_w.unlock();
							// System.out.println("successful");
						}
						
			    		if (!isOpenInAnyThread()) {
			    			m_access = NOT_ACCESSED;
						}
		    		}
	            } else {
	    			setOpen(false);
	            }
			}
            
            return true;
            
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("File \"" + getName() + "\" could not be closed: " + hle.getMessage(), hle);
        }
		
		return false;
	}
}

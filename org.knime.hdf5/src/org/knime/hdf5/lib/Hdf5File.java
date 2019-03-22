package org.knime.hdf5.lib;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Class for hdf files, i.e. are accessed through {@code H5F} in the hdf api.
 */
public class Hdf5File extends Hdf5Group {

	public static final int NOT_ACCESSED = -1;
	
	public static final int READ_ONLY_ACCESS = 0;
	
	public static final int READ_WRITE_ACCESS = 1;
	
	/**
	 * Lists all loaded file instances.
	 */
	private static final List<Hdf5File> ALL_FILES = new ArrayList<>();

	/**
	 * Manages the locks for opening/creating/deleting files on the machine.
	 */
	private static final ReentrantReadWriteLock GLOBAL_RWL = new ReentrantReadWriteLock(true);
	
	private static final Lock GLOBAL_R = GLOBAL_RWL.readLock();
	
	private static final Lock GLOBAL_W = GLOBAL_RWL.writeLock();
	
	private final String m_filePath;

	/**
	 * Manages the locks for reading/writing this file.
	 */
	private final ReentrantReadWriteLock m_rwl = new ReentrantReadWriteLock(true);
	
	private final Lock m_r = m_rwl.readLock();
	
	private final Lock m_w = m_rwl.writeLock();
	
	private int m_access = NOT_ACCESSED;
	
	/**
	 * Lists all threads that have access to this file and maps them to the
	 * number how many times they have opened this file.
	 */
	private Map<Thread, Integer> m_accessors = new HashMap<>();
	
	private Hdf5File(String filePath) throws HDF5LibraryException, NullPointerException,
			IllegalArgumentException {
		super(filePath.substring(filePath.lastIndexOf(File.separator) + 1));
		m_filePath = filePath;
		
		ALL_FILES.add(this);
	}
	
	/**
	 * Creates a new file with the input file path.
	 * The name of the file may not contain '/'.
	 * 
	 * @param filePath the path to the file including its name
	 * @throws IOException if the file already exists or an internal error occurred
	 */
	public static Hdf5File createFile(String filePath) throws IOException {
		try {
			GLOBAL_W.lock();
		
			if (new File(filePath).exists()) {
				throw new IOException("File does already exist");
			}
			
			Hdf5File file = new Hdf5File(filePath);
			file.create();
			
			return file;
			
		} catch (HDF5LibraryException | IOException | NullPointerException | IllegalArgumentException hlionpiae) {
			throw new IOException("The file \"" + filePath + "\" could not be created: " + hlionpiae.getMessage(), hlionpiae);
			
		} finally {
			GLOBAL_W.unlock();
		}
	}

	/**
	 * Opens the file with the input file path with either READ or WRITE access.
	 * The name of the file may not contain '/'.
	 * <br>
	 * <br>
	 * If the file is already open in this thread, increase the counter of
	 * accesses for this thread by 1.
	 * <br>
	 * If the file is open in different threads, it may happen that this
	 * method is locked until the other threads have closed the file.
	 * 
	 * @param filePath the path to the file including its name
	 * @param access {@code READ_ONLY_ACCESS} or {@code READ_WRITE_ACCESS}
	 * @throws IOException if the file does not exist or an internal error occurred
	 */
	public synchronized static Hdf5File openFile(String filePath, int access) throws IOException {
		try {
			GLOBAL_R.lock();
		
			if (!new File(filePath).exists()) {
				throw new IOException("File does not exist");
			}
			
			Hdf5File file = null;
			// check if the file has already been loaded
			for (Hdf5File f : ALL_FILES) {
				if (f.getFilePath().equals(filePath)) {
					file = f;
					file.open(access);
					break;
				}
			}
	
			if (file == null) {
				// load the file and add it to the list of loaded files
				file = new Hdf5File(filePath);
				file.open(access);
			}
			
			return file;
			
		} catch (HDF5LibraryException | IOException | NullPointerException | IllegalArgumentException hlionpiae) {
			throw new IOException("The file \"" + filePath + "\" could not be opened: " + hlionpiae.getMessage(), hlionpiae);
			
		} finally {
			GLOBAL_R.unlock();
		}
	}

	/**
	 * Returns the property list which sets the close degree to strong, i.e.
	 * it ensures that all elements in the file are also closed after
	 * closing the file.
	 * 
	 * @return the property list that specifies how the file will be closed
	 * @throws HDF5LibraryException if an error occurred in the hdf library
	 * @see Hdf5File#FILE_CLOSE_DEGREE
	 */
	private static long getAccessPropertyList() throws HDF5LibraryException {
		long pid = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);
		H5.H5Pset_fclose_degree(pid, HDF5Constants.H5F_CLOSE_STRONG);
		
		return pid;
	}
	
	/**
	 * @param filePath the file path
	 * @return if a file with the file path exists and has an hdf extension (.h5, .hdf5)
	 */
	public static boolean existsHdf5File(String filePath) {
		try {
			GLOBAL_R.lock();
			return hasHdf5FileEnding(filePath) && new File(filePath).exists();
			
		} finally {
			GLOBAL_R.unlock();
		}
	}
	
	/**
	 * @param filePath the file path
	 * @param overwriteHdfFile if an already existing file may be overwritten
	 * @return if a new file with the file path can be created, i.e. if the
	 * 	file path has an hdf extension (.h5, .hdf5), the parent directory
	 * 	exists and the file does not exist or may be overwritten
	 */
	public static boolean isHdf5FileCreatable(String filePath, boolean overwriteHdfFile) {
		try {
			GLOBAL_R.lock();
			return hasHdf5FileEnding(filePath) && new File(getDirectoryPath(filePath)).isDirectory()
					&& (!new File(filePath).exists() || overwriteHdfFile);
			
		} finally {
			GLOBAL_R.unlock();
		}
	}

	/**
	 * Checks if the file is writable.
	 * 
	 * @param filePath the file path
	 * @throws IOException if the file is not writable, i.e. it is not creatable
	 * 	if it does not exist or otherwise, if it is open somewhere on the machine
	 * @see Hdf5File#isHdf5FileCreatable(String, boolean)
	 * @see Hdf5File#isOpenAnywhere()
	 */
	public static void checkHdf5FileWritable(String filePath) throws IOException {
		try {
			if (existsHdf5File(filePath)) {
				Hdf5File file = Hdf5File.openFile(filePath, READ_WRITE_ACCESS);
				try {
					file.m_w.lock();
					file.close();
					if (file.isOpenAnywhere()) {
						throw new IOException("File is opened somewhere else.");
					}
				} finally {
					file.m_w.unlock();
				}
			} else if (!isHdf5FileCreatable(filePath, false)) {
				throw new IOException("File cannot be created");
			}
		} catch (Exception e) {
			throw new IOException("Hdf5File \"" + filePath + "\" is not writable: " + e.getMessage(), e);
		}
	}
	
	/**
	 * @param filePath the file path
	 * @return if the file path has an hdf extension (.h5, .hdf5)
	 */
	public static boolean hasHdf5FileEnding(String filePath) {
		return filePath.endsWith(".h5") || filePath.endsWith(".hdf5");
	}
	
	/**
	 * @param filePath the file path
	 * @return the file path of the parent directory
	 */
	public static String getDirectoryPath(String filePath) {
		int dirPathLength = filePath.lastIndexOf(File.separator);
		return filePath.substring(0, dirPathLength >= 0 ? dirPathLength : 0);
	}

	/**
	 * @param filePath the file path
	 * @return a file path with a constructed name from the input such that it
	 * 	is not contained in the parent directory
	 * @throws IOException if no parent directory exists for the file path
	 * @see Hdf5TreeElement#getUniqueName(List, String)
	 */
	public static String getUniqueFilePath(String filePath) throws IOException {
		File directory = new File(Hdf5File.getDirectoryPath(filePath));
		if (directory.isDirectory()) {
			String fileName = filePath.substring(filePath.lastIndexOf(File.separator) + 1);
			String fileExtension = fileName.lastIndexOf(".") >= 0 ? fileName.substring(fileName.lastIndexOf(".")) : "";
			String fileNameWithoutExtension = fileName.substring(0, fileName.length() - fileExtension.length());
			
			List<String> usedNames = new ArrayList<>();
			for (File file : directory.listFiles()) {
				if (file.isFile()) {
					String name = file.getName();
	    			String extension = name.lastIndexOf(".") >= 0 ? name.substring(name.lastIndexOf(".")) : "";
	    			if (extension.equals(fileExtension)) {
	    				usedNames.add(name.substring(0, name.length() - extension.length()));
	    			}
				}
			}
    		return directory.getPath() + File.separator + Hdf5TreeElement.getUniqueName(usedNames, fileNameWithoutExtension) + fileExtension;
    		
		} else {
			throw new IOException("Directory \"" + directory.getPath() + "\" for new file does not exist");
		}
	}
	
	/**
	 * @return the absolute file path of this file
	 */
	public String getFilePath() {
		return new File(m_filePath).getAbsolutePath();
	}
	
	@Override
	protected boolean isOpen() {
		return isOpenInThisThread();
	}

	@Override
	public boolean exists() {
		return Hdf5File.existsHdf5File(getFilePath());
	}
	
	private boolean isOpenInThisThread() {
		synchronized (m_accessors) {
			return m_accessors.containsKey(Thread.currentThread());
		}
	}

	private void setOpenInThisThread(boolean open) {
		synchronized (m_accessors) {
			Thread curThread = Thread.currentThread();
			if (!isOpenInThisThread() && open) {
				// add the thread as new accessor
				m_accessors.put(curThread, 1);
			} else if (isOpenInThisThread()) {
				// update the number of accesses to the file
				m_accessors.put(curThread, m_accessors.get(curThread) + (open ? 1 : -1));
			}
			
			/*
			 * delete the thread from the accessor list if it does not access
			 * the file anymore
			 */
			if (m_accessors.get(curThread) == 0) {
				m_accessors.remove(curThread);
			}
		}
	}
	
	private boolean isOpenExactlyOnceInThisThread() {
		synchronized (m_accessors) {
			return isOpenInThisThread() && m_accessors.get(Thread.currentThread()) == 1;
		}
	}
	
	/**
	 * @return if this file is open in this thread, but not in any other thread
	 */
	private boolean isOpenOnlyInThisThread() {
		return isOpenInThisThread() && m_accessors.size() == 1;
	}
	
	private boolean isOpenInAnyThread() {
		return !m_accessors.isEmpty();
	}
	
	/**
	 * <b>Note:</b> This method is platform-dependent. It only works on Windows
	 * and POSIX-compliant Linux so far.
	 * 
	 * @return if the file is open somewhere on the machine
	 * @see Hdf5File.PlatformOS#isFileOpen(File)
	 */
	private boolean isOpenAnywhere() {
		return PlatformOS.get().isFileOpen(new File(getFilePath()));
	}
	
	private static enum PlatformOS {
		WINDOWS, LINUX, OTHER;
		
		private static PlatformOS get() {
			String os = Platform.getOS();
			if (os.equals(Platform.OS_WIN32)) {
				return WINDOWS;
			} else if (os.equals(Platform.OS_LINUX)) {
				return LINUX;
			} else {
				return OTHER;
			}
		}
		
		/**
		 * @param file the file
		 * @return if the file is open on the machine
		 */
		private boolean isFileOpen(File file) {
		    try {
				switch (this) {
				case WINDOWS:
					return !file.renameTo(new File(file.getPath()));
					
				case LINUX:
					Process process = null;
			        try {
			        	// using function 'lsof' instead of 'fuser' needs too long
				    	if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
				    		// execute the command 'fuser absoluteFilePath'
					        process = new ProcessBuilder(new String[] {"fuser", file.getAbsolutePath()}).start();
				    	} else {
				    		throw new UnsupportedOperationException("Function 'fuser' is only supported on POSIX-compliant OS");
				    	}
				        BufferedInputStream in = new BufferedInputStream(process.getInputStream());
				        if (in.read(new byte[1]) != -1) {
				        	// the result of the command was not empty
				            return true;
				        }
				    } finally {
				    	if (process != null) {
						    process.destroy();
				    	}
				    }
				    
				    return false;
				    
				default:
					return false;
				}
		    } catch (Exception e) {
				NodeLogger.getLogger(getClass()).warn("Could not check if file is open somewhere else: " + e.getMessage(), e);
				return false;
		    }
		}
	}
	
	/**
	 * Creates the hdf file from this instance.
	 * 
	 * @throws IOException if the file already exists or an internal error occurred
	 */
	private void create() throws IOException {
		try {
			m_w.lock();
			
			try {
				lockWriteOpen();
				setElementId(H5.H5Fcreate(getFilePath(), HDF5Constants.H5F_ACC_EXCL,
						HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
				m_access = READ_WRITE_ACCESS;
				setOpenInThisThread(true);
				
			} finally {
				unlockWriteOpen();
			}
        } catch (HDF5LibraryException | NullPointerException hlnpe) {
            m_w.unlock();

			throw new IOException("The file \"" + getFilePath() + "\" cannot be created : " + hlnpe.getMessage(), hlnpe);
        }
	}
	
	/**
	 * Opens this file with either READ or WRITE access.
	 * <br>
	 * <br>
	 * If the file is already open in this thread, increase the counter of
	 * accesses for this thread by 1.
	 * <br>
	 * If the file is open in different threads, it may happen that this
	 * method is locked until the other threads have closed the file.
	 * 
	 * @param access {@code READ_ONLY_ACCESS} or {@code READ_WRITE_ACCESS}
	 * @throws IOException if this file does not exist or an internal error occurred
	 */
	public void open(int access) throws IOException {
		try {
			// acquire a read or write lock if this file is not open in this thread
			if (!isOpenInThisThread()) {
    			long pid = Hdf5File.getAccessPropertyList();

				if (access == READ_ONLY_ACCESS) {
					m_r.lock();
					
					try {
						lockWriteOpen();
						// open this file only if this instance is not already open
						if (!isOpenInAnyThread()) {
							setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDONLY, pid));
						}
						m_access = access;
						setOpenInThisThread(true);
						
					} finally {
						unlockWriteOpen();
					}
				} else if (access == READ_WRITE_ACCESS) {
					m_w.lock();

					try {
						lockWriteOpen();
						// open this file only if this instance is not already open
						if (!isOpenInAnyThread()) {
							setElementId(H5.H5Fopen(getFilePath(), HDF5Constants.H5F_ACC_RDWR, pid));
						}
						m_access = access;
						setOpenInThisThread(true);
						
					} finally {
						unlockWriteOpen();
					}
				}
				
    			H5.H5Pclose(pid);
    			
			} else {
				setOpenInThisThread(true);
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			if (access == READ_ONLY_ACCESS) {
				m_r.unlock();
				
			} else if (access == READ_WRITE_ACCESS) {
				m_w.unlock();
			}

			throw new IOException("The file \"" + getFilePath() + "\" cannot be opened: " + hlnpe.getMessage(), hlnpe);
        }
	}
	
	/**
	 * @param prefix the prefix for the name like "temp_"
	 * @return the instance for the newly created copy of this hdf file
	 * @throws IOException if an error occurred in the hdf library while creating
	 * @throws IllegalArgumentException if the prefix contains '/'
	 */
	@Override
	public Hdf5File createBackup(String prefix) throws IOException, IllegalArgumentException {
		if (prefix.contains("/")) {
			throw new IllegalArgumentException("Prefix for backup file cannot contain '/'");
		}
		
		return copyFile(getUniqueFilePath(getDirectoryPath(getFilePath()) + File.separator + prefix + getName()));
	}
	
	/**
	 * Copies this file to {@code newPath} and opens the new file.
	 * 
	 * @param newPath the path for the new file
	 * @return the new file
	 * @throws IOException if a file in {@code newPath} already exists or the
	 * 	new file could not be opened
	 */
	public Hdf5File copyFile(String newPath) throws IOException {
		GLOBAL_W.lock();
		
		if (new File(newPath).exists()) {
			throw new IOException("File could not be copied: File already exists");
		}
		
		Path backupPath = Files.copy(Paths.get(getFilePath()), Paths.get(newPath), StandardCopyOption.COPY_ATTRIBUTES);
		Hdf5File file = openFile(backupPath.toString(), READ_ONLY_ACCESS);
		
		GLOBAL_W.unlock();
		
		return file;
	}

	/**
	 * <b>Note:</b> Be careful that this instance is not usable anymore after
	 * deletion.
	 * 
	 * @return if the deletion from the hdf file was successful
	 * @throws IOException if this file does not exist, is open somewhere on
	 * the machine or an internal error occurred
	 * @throws IllegalStateException if this file is not in write access
	 * @see Hdf5File#isOpenAnywhere()
	 */
	public boolean deleteFile() throws IOException, IllegalStateException {
		// TODO check if some other threads are still waiting for this file
		if (m_access != READ_WRITE_ACCESS) {
			throw new IllegalStateException("Write access is needed to delete a file.");
		}
		
		GLOBAL_W.lock();
		
		File file = new File(getFilePath());
		if (file == null || !file.exists()) {
			throw new IOException("File cannot be deleted: it does not exist");
		}
		
		close();
		if (isOpenAnywhere()) {
			throw new IOException("File cannot be deleted: it is still opened somewhere");
		}
		
		boolean success = file.delete();
		if (success) {
			ALL_FILES.remove(this);
		}
		
		GLOBAL_W.unlock();
		
		return success;
	}
	
	/**
	 * @return information about what elements in this file are still open
	 */
	private String whatisOpenInFile() {
        long count = -1;
        long openObjects = -1;
        long[] objects;
        String[] objTypes = { "Unknown objectType", "File", "Group", "DataType", "DataSpace", "DataSet", "Attribute" };
        String opened = "";
		
        try {
        	if (isOpenInThisThread()) {
        		count = H5.H5Fget_obj_count(getElementId(), HDF5Constants.H5F_OBJ_ALL);
        		
			} else {
				return "(error: file is already closed)";
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger(getClass()).debug("Number of open objects in file could not be loaded: " + hle.getMessage(), hle);
		}

        if (count <= 0) {
        	return "(error: couldn't find out number of open objects)";
        }

		// only the file itself is open
        if (count == 1) {
        	return "0";
        }

        objects = new long[(int) count];
		opened += count - 1;

        try {
			openObjects = H5.H5Fget_obj_ids(getElementId(), HDF5Constants.H5F_OBJ_ALL, count, objects);
			   
			// i == 0 is just the file itself
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
	        
		} catch (Exception e) {
            NodeLogger.getLogger(getClass()).debug("Info of open objects in file could not be loaded: " + e.getMessage(), e);
        }
        
        return opened;
	}
	
	/**
	 * Closes this group and all elements in this group.
	 * 
	 * @throws IOException 
	 */

	/**
	 * Closes this file and all elements in this file if it is not accessed by
	 * any threads anymore.
	 * <br>
	 * <br>
	 * If the file is already closed in this thread, the method does nothing.
	 * <br>
	 * If the file is still open in this thread, decrease the counter of
	 * accesses for this thread by 1.
	 * 
	 * @param access {@code READ_ONLY_ACCESS} or {@code READ_WRITE_ACCESS}
	 * @throws IOException if this file does not exist or an internal error occurred
	 */
	@Override
	public boolean close() throws IOException {
		try {
			lockWriteOpen();
			checkExists();
			
			boolean success = true;
			if (isOpenInThisThread()) {
				if (isOpenExactlyOnceInThisThread()) {
	    			if (isOpenOnlyInThisThread()) {
	    	    		for (Hdf5DataSet<?> ds : getDataSets()) {
	            			success &= ds.close();
	            		}

	    	    		for (Hdf5Attribute<?> attr : getAttributes()) {
	            			success &= attr.close();
	            		}
	    	    		
	    	    		for (Hdf5Group group : getGroups()) {
	            			success &= group.close();
	            		}
			    		
			    		NodeLogger.getLogger(getClass()).debug("Number of open objects in file \""
			    				+ getName() + "\": " + whatisOpenInFile());
			    		
			    		success &= H5.H5Fclose(getElementId()) >= 0;
			    		if (success) {
			    			setElementId(-1);
			    		}
		    		}
	    			
	    			if (success) {
			    		setOpenInThisThread(false);
			    		
			    		/*
			    		 * unlock the read or write lock since the file is not
			    		 * open anymore in this thread
			    		 */
						if (m_access == READ_ONLY_ACCESS) {
							m_r.unlock();
						} else if (m_access == READ_WRITE_ACCESS) {
							m_w.unlock();
						}
						
			    		if (!isOpenInAnyThread()) {
			    			m_access = NOT_ACCESSED;
						}
	    			}
	            } else {
	    			setOpenInThisThread(false);
	            }
			}
            
            return success;
            
        } catch (HDF5LibraryException hle) {
        	throw new IOException("File \"" + getFilePath() + "\" could not be closed: " + hle.getMessage(), hle);
        	
        } finally {
			unlockWriteOpen();
		}
	}
	
	@Override
	public String toString() {
		return "{ filePath=" + getFilePath() + ",open=" + isOpen() + ",access= "
				+ (m_access == READ_ONLY_ACCESS ? "READ" : m_access == READ_WRITE_ACCESS ? "WRITE" : "NONE") + " }";
	}
}

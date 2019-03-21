package org.knime.hdf5.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.DataSetNodeEdit;
import org.knime.hdf5.nodes.writer.edit.EditDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Class for hdf groups, i.e. are accessed through {@code H5G} in the hdf api.
 */
public class Hdf5Group extends Hdf5TreeElement {
	
	public static final int OBJECT_NOT_EXISTS = 0;
	
	private final List<Hdf5Group> m_groups = new ArrayList<>();
	
	private final List<Hdf5DataSet<?>> m_dataSets = new ArrayList<>();

	protected Hdf5Group(final String name)
			throws NullPointerException, IllegalArgumentException {
		super(name);
	}
	
	private static Hdf5Group getInstance(final Hdf5Group parent, final String name) throws IllegalArgumentException, IllegalStateException {
		if (parent == null) {
			throw new IllegalArgumentException("Parent of group \"" + name + "\" cannot be null");
			
		} else if (!parent.isOpen()) {
			throw new IllegalStateException("Parent group \"" + parent.getName() + "\" is not open!");
		}
		
		return new Hdf5Group(name);
	}
	
	private static Hdf5Group createGroup(final Hdf5Group parent, final String name) throws IOException {
		Hdf5Group group = null;
		
		try {
			group = getInstance(parent, name);

			/*
			 * parent.lockReadOpen() is not needed here because write access
			 * for the whole file is already needed for creating the group,
			 * so no other readers or writers can access parent anyway
			 */
			group.setElementId(H5.H5Gcreate(parent.getElementId(), group.getName(), 
					HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
					HDF5Constants.H5P_DEFAULT));
			parent.addGroup(group);
    		
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException | IllegalStateException hlnpiaise) {
			throw new IOException("Group could not be created: " + hlnpiaise.getMessage(), hlnpiaise);
        }
		
		return group;
	}

	private static Hdf5Group openGroup(final Hdf5Group parent, final String name) throws IOException {
		Hdf5Group group = null;
		
		try {
			group = getInstance(parent, name);
			parent.addGroup(group);
			
			group.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
        	throw new IOException("Group could not be opened: " + npiaise.getMessage(), npiaise);
        }
		
		return group;
	}

	/**
	 * @return the child groups of this group
	 */
	public Hdf5Group[] getGroups() {
		synchronized (m_groups) {
			return m_groups.toArray(new Hdf5Group[m_groups.size()]);
		}
	}
	
	/**
	 * @return the child dataSets of this group
	 */
	public Hdf5DataSet<?>[] getDataSets() {
		synchronized (m_dataSets) {
			return m_dataSets.toArray(new Hdf5DataSet<?>[m_dataSets.size()]);
		}
	}
	
	/**
	 * Creates a new child group with the name {@code name}.
	 * The name may not contain '/'.
	 * 
	 * @param name the name for the new group
	 * @return the new group 
	 * @throws IOException if there already exists a child <b>treeElement</b>
	 * 	with the same name in this group
	 */
	public Hdf5Group createGroup(final String name) throws IOException {
		int objectType = getObjectTypeByName(name);
		if (objectType == OBJECT_NOT_EXISTS) {
			return createGroup(this, name);
			
		} else {
			throw new IOException("There is already "
					+ (objectType == HDF5Constants.H5I_GROUP ? "a group" 
						: (objectType == HDF5Constants.H5I_DATASET ? "a dataSet" : "an object"))
					+ " with the name \"" + name + "\" in the group \"" + getPathFromFileWithName() + "\"");
		}
	}
	
	/**
	 * Creates a new child group with the name {@code name}.
	 * The name may not contain '/'. <br>
	 * See the {@code H5P} api for more information about {@code compressionLevel}
	 * and {@code chunkRowSize}.
	 * 
	 * @param name the name for the new dataSet
	 * @param dimensions the dimensions for the new dataSet
	 * @param compressionLevel the compression level (from 0 to 9) for the new dataSet
	 * @param chunkRowSize the size of the row chunks to store the dataSet
	 * 	(may not be larger than the number of rows or 2^32-1)
	 * @param type the data type for the new dataSet
	 * @return the new dataSet
	 * @throws IOException if there already exists a child <b>treeElement</b>
	 * 	with the same name in this group
	 */
	public Hdf5DataSet<?> createDataSet(final String name, long[] dimensions,
			int compressionLevel, long chunkRowSize, Hdf5DataType type) throws IOException {
		int objectType = getObjectTypeByName(name);	
		if (objectType == OBJECT_NOT_EXISTS) {
			return Hdf5DataSet.createDataSet(this, name, dimensions, compressionLevel, chunkRowSize, type);
			
		} else {
			throw new IOException("There is already "
					+ (objectType == HDF5Constants.H5I_DATASET ? "a dataSet" 
						: (objectType == HDF5Constants.H5I_GROUP ? "a group" : "an object"))
					+ " with the name \"" + name + "\" in the group \"" + getPathFromFileWithName() + "\"");
		}
	}
	
	/**
	 * Moves the input object from its original place to this group.
	 * <br>
	 * <br>
	 * <b>Note:</b> Be careful that the instance of the old object is not usable
	 * anymore since the object does not exist anymore in the hdf file.
	 * 
	 * @param oldObject the object to move
	 * @param newName the new name for the moved object
	 * @return the new instance of the moved object
	 * @throws IOException if {@code oldObject} does not exist, an object with
	 * 	the new name already exists in this group or an internal error occurred
	 */
	public Hdf5TreeElement moveObject(Hdf5TreeElement oldObject, String newName) throws IOException {
		Hdf5TreeElement newObject = null;
		
		try {
			if (getObjectTypeByName(newName) != OBJECT_NOT_EXISTS) {
				throw new IOException("Object already exists");
			}

			Hdf5Group oldParent = oldObject.getParent();
			int objectType = oldParent.getObjectTypeByName(oldObject.getName());
			
			// move object
			oldObject.close();
			H5.H5Lmove(oldParent.getElementId(), oldObject.getName(), getElementId(), newName, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
			
			// update list of children
			if (!oldObject.exists() && getObjectTypeByName(newName) == objectType) {
				if (objectType == HDF5Constants.H5I_GROUP) {
					removeGroup((Hdf5Group) oldObject);
					newObject = getGroup(newName);
				} else if (objectType == HDF5Constants.H5I_DATASET) {
					removeDataSet((Hdf5DataSet<?>) oldObject);
					newObject = getDataSet(newName);
				}
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Object \"" + oldObject.getPathFromFileWithName()
					+ "\" could not be moved to group \"" + getPathFromFileWithName(true) + "\" with new name \""
					+ newName + "\": " + hlionpe.getMessage(), hlionpe);
		}
		
		return newObject;
	}

	/**
	 * Copies the input object to this group.
	 * 
	 * @param oldObject the object to copy
	 * @param newName the new name for the new object
	 * @return the new instance of the new object
	 * @throws IOException if {@code oldObject} does not exist, an object with
	 * 	the new name already exists in this group or an internal error occurred
	 */
	public Hdf5TreeElement copyObject(Hdf5TreeElement oldObject, String newName) throws IOException {
		Hdf5TreeElement newObject = null;
		
		try {
			Hdf5Group oldParent = oldObject.getParent();
			int objectType = oldParent.getObjectTypeByName(oldObject.getName());
			H5.H5Ocopy(oldParent.getElementId(), oldObject.getName(), getElementId(), newName, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
			if (getObjectTypeByName(newName) == objectType) {
				newObject = objectType == HDF5Constants.H5I_GROUP ? getGroup(newName) : getDataSet(newName);
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Object \"" + oldObject.getPathFromFileWithName()
					+ "\" could not be copied to group \"" + getPathFromFileWithName(true) + "\" with new name \""
					+ newName + "\": " + hlionpe.getMessage(), hlionpe);
		}
		
		return newObject;
	}
	
	/**
	 * @param name the name of the child group
	 * @return the open group with the input name if it exists
	 * @throws IOException if it does not exist or an internal error occurred
	 */
	public Hdf5Group getGroup(final String name) throws IOException {
		Hdf5Group group = null;
		
		synchronized (m_groups) {
			// check if the group has already been loaded
			for (Hdf5Group grp : getGroups()) {
				if (grp.getName().equals(name)) {
					group = grp;
					group.open();
					break;
				}
			}
			
			if (group == null) {
				// load the group and add it to the list of child groups
				if (getObjectTypeByName(name) == HDF5Constants.H5I_GROUP) {
					group = openGroup(this, name);
				
				} else {
					throw new IOException("Group \"" + getPathFromFileWithName() + name + "\" does not exist");
				}
			}
		}
		
		return group;
	}
	
	/**
	 * @param path the path with '/' as separator and this group as root
	 * @return the group with this path
	 * @throws IOException if no group with this path exists or an
	 * 	internal error occurred
	 */
	public Hdf5Group getGroupByPath(String path) throws IOException {
		Hdf5Group group = null;
		String name = path.split("/")[0];
		
		if (path.contains("/")) {
			Hdf5Group grp = getGroup(name);
			
			if (grp != null) {
				group = grp.getGroupByPath(path.substring(name.length() + 1));
			}
		} else if (!path.isEmpty()) {
			group = getGroup(name);
			
		} else {
			group = this;
		}
		
		return group;
	}

	/**
	 * @param name the name of the child dataSet
	 * @return the open dataSet with the input name if it exists
	 * @throws IOException if it does not exist or an internal error occurred
	 */
	public Hdf5DataSet<?> getDataSet(final String name) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		
		synchronized (m_dataSets) {
			// check if the dataSet has already been loaded and update it if necessary
			for (Hdf5DataSet<?> ds : getDataSets()) {
				if (ds.getName().equals(name)) {
					dataSet = ds;
					dataSet.updateDataSet();
					dataSet.open();
					break;
				}
			}
			
			if (dataSet == null) {
				// load the dataSet and add it to the list of child dataSets
				if (getObjectTypeByName(name) == HDF5Constants.H5I_DATASET) {
					dataSet = Hdf5DataSet.openDataSet(this, name);
					
				} else {
					throw new IOException("DataSet \"" + getPathFromFileWithName() + name + "\" does not exist");
				}
			}
		}
	
		return dataSet;
	}
	
	/**
	 * @param path the path with '/' as separator and this group as root
	 * @return the dataSet with this path
	 * @throws IOException if no dataSet with this path exists or an
	 * 	internal error occurred
	 */
	public Hdf5DataSet<?> getDataSetByPath(String path) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		String name = path.split("/")[0];
		
		if (path.contains("/")) {
			Hdf5Group group = getGroup(name);
			
			if (group != null) {
				dataSet = group.getDataSetByPath(path.substring(name.length() + 1));
			}
		} else {
			dataSet = getDataSet(name);
		}
		
		return dataSet;
	}
	
	/**
	 * Returns the type id of the child object with the input name.
	 * <br>
	 * The type id can be {@code HDF5Constants.H5I_DATASET} (== 5) for a dataSet,
	 * {@code HDF5Constants.H5I_GROUP} (== 2) for a group,
	 * {@code Hdf5Group.OBJECT_NOT_EXISTS} (== 0) if no object exists
	 * or differently for other (unsupported) objects.
	 * 
	 * @param name the name of the child object
	 * @return the type id of the object
	 * @throws IOException if the existence could not be checked
	 */
	public int getObjectTypeByName(String name) throws IOException {
		try {
			lockReadOpen();
			checkOpen();
			
			if (H5.H5Lexists(getElementId(), name, HDF5Constants.H5P_DEFAULT)
					&& H5.H5Oexists_by_name(getElementId(), name, HDF5Constants.H5P_DEFAULT)) {
				long elementId = H5.H5Oopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				int typeId = H5.H5Iget_type(elementId); 
				H5.H5Oclose(elementId);
				
				return typeId;
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Existence of object \"" + name + "\" in group \""
					+ getPathFromFileWithName() + "\" could not be checked: " + hlionpe.getMessage(), hlionpe);
			
		} finally {
			unlockReadOpen();
		}
		
		return OBJECT_NOT_EXISTS;
	}

	/**
	 * <b>Note:</b> Be careful that the instance of the deleted object is
	 * not usable anymore.
	 * 
	 * @param name the name of the object to delete
	 * @return {@code true} if the deletion form the hdf file was successful
	 * @throws IOException if the object does not exist or an internal error occurred
	 */
	public boolean deleteObject(final String name) throws IOException {
		boolean success = false;
		
		try {
			int objectType = getObjectTypeByName(name);
			if (objectType == HDF5Constants.H5I_GROUP || objectType == HDF5Constants.H5I_DATASET) {
				Hdf5TreeElement object = objectType == HDF5Constants.H5I_GROUP ? getGroup(name) : getDataSet(name);
				
				// delete children
				if (objectType == HDF5Constants.H5I_GROUP) {
					Hdf5Group group = (Hdf5Group) object;
					for (String childName : group.loadObjectNames()) {
						group.deleteObject(childName);
					}
				}
				for (String childName : object.loadAttributeNames()) {
					object.deleteAttribute(childName);
				}

				// delete object with the input name
				object.close();
				if (!object.isOpen()) {
					H5.H5Ldelete(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				}
				
				// remove the object from list of children
				int objectTypeAfterDeletion = getObjectTypeByName(name);
				if (objectTypeAfterDeletion == OBJECT_NOT_EXISTS) {
					success = true;
					if (object instanceof Hdf5Group) {
						removeGroup((Hdf5Group) object);
					} else {
						removeDataSet((Hdf5DataSet<?>) object);
					}
				}
			} else if (objectType == OBJECT_NOT_EXISTS){
				throw new IOException("Cannot delete object which does not exist");
				
			} else {
				throw new IOException("Cannot delete object which is no group or dataSet");
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Object \"" + getPathFromFileWithName(true) + name
					+ "\" could not be deleted from disk: " + hlionpe.getMessage(), hlionpe);
		}
		
		return success;
	}
	
	private void addGroup(Hdf5Group group) {
		synchronized (m_groups) {
			m_groups.add(group);
			group.setPathFromFile(getPathFromFileWithName(true));
			group.setParent(this);
		}
	}

	void addDataSet(Hdf5DataSet<?> dataSet) {
		synchronized (m_dataSets) {
			m_dataSets.add(dataSet);
			dataSet.setPathFromFile(getPathFromFileWithName(true));
			dataSet.setParent(this);
		}
	}

	private boolean removeGroup(Hdf5Group group) {
		synchronized (m_groups) {
			group.setParent(null);
			group.setPathFromFile("");
			group.setElementId(-1);
			return m_groups.remove(group);
		}
	}

	private boolean removeDataSet(Hdf5DataSet<?> dataSet) {
		synchronized (m_dataSets) {
			dataSet.setParent(null);
			dataSet.setPathFromFile("");
			dataSet.setElementId(-1);
			return m_dataSets.remove(dataSet);
		}
	}

	private String[] loadObjectNames(int objectId) throws IOException {
		List<String> names = new ArrayList<>();
		Hdf5Group group = isFile() ? this : getParent();
		String name = isFile() ? "/" : getName();
		
		try {
			group.lockReadOpen();
			group.checkOpen();
			
			// get number of child objects in this group
			int count = (int) H5.H5Gn_members(group.getElementId(), name);
			
			if (count > 0) {
				// get the information of the child objects
				String[] oname = new String[count];
				int[] otype = new int[count];
				H5.H5Gget_obj_info_all(group.getElementId(), name, oname, otype, new int[count], new long[count], HDF5Constants.H5_INDEX_NAME);
				
				// filter the child objects on the objectId
				boolean returnAllObjects = objectId == HDF5Constants.H5O_TYPE_UNKNOWN;
				for (int i = 0; i < otype.length; i++) {
					if (returnAllObjects || objectId == otype[i]) {
						names.add(oname[i]);
					}
				}
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlnpe) {
           throw new IOException("List of objects could not be loaded: " + hlnpe.getMessage(), hlnpe);
           
        } finally {
			group.unlockReadOpen();
		}
		
		return names.toArray(new String[names.size()]);
	}
	
	/**
	 * @return the names of all child objects in this group
	 * 	(also of the not supported ones)
	 * @throws IOException if this group is not open or an internal error occurred
	 */
	public String[] loadObjectNames() throws IOException {
		return loadObjectNames(HDF5Constants.H5O_TYPE_UNKNOWN);
	}

	/**
	 * @return the names of all child groups in this group
	 * @throws IOException if this group is not open or an internal error occurred
	 */
	public String[] loadGroupNames() throws IOException {
		return loadObjectNames(HDF5Constants.H5O_TYPE_GROUP);
	}

	/**
	 * @return the names of all child dataSets in this group
	 * @throws IOException if this group is not open or an internal error occurred
	 */
	public String[] loadDataSetNames() throws IOException {
		return loadObjectNames(HDF5Constants.H5O_TYPE_DATASET);
	}

	/**
	 * @return all descendant dataSets mapped to their data type
	 * @throws IllegalStateException if this group is not open or an internal error occurred
	 */
	public Map<String, Hdf5DataType> getAllDataSetsInfo() throws IOException {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		String path = getPathFromFileWithName(true);
		
		try {
			lockReadOpen();
			checkOpen();
			
			for (String name : loadDataSetNames()) {
				// there might exist some dataSets with unsupported dataTypes which should be ignored
				try {
					Hdf5DataType dataType = findDataSetType(name);
					paths.put(path + name, dataType);

				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger(getClass()).warn("DataSet \"" + path + name
							+ "\" could not be loaded: " + udte.getMessage());
					
				} catch (IOException | NullPointerException ionpe) {
					NodeLogger.getLogger(getClass()).error("DataSet \"" + path + name
							+ "\" could not be loaded: " + ionpe.getMessage(), ionpe);
				} 
			}
			
			for (String name : loadGroupNames()) {
				try {
					Hdf5Group group = getGroup(name);
					paths.putAll(group.getAllDataSetsInfo());
					
				} catch (IOException | NullPointerException ionpe) {
					NodeLogger.getLogger(getClass()).error("Group \"" + path + name
							+ "\" could not be loaded: " + ionpe.getMessage(), ionpe);
				}
			}
		} finally {
			unlockReadOpen();
		}
		
		return paths;
	}
	
	/**
	 * Creates a new attribute as a child of this treeElement using an edit specification.
	 * 
	 * @param edit information about name and data type for the dataSet creation
	 * @return the new dataSet
	 * @throws IOException if an internal error occurred while creating
	 */
	public Hdf5DataSet<?> createDataSetFromEdit(DataSetNodeEdit edit) throws IOException {
		EditDataType editDataType = edit.getEditDataType();
		Hdf5DataType dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(editDataType.getOutputType(), editDataType.getEndian()),
				Hdf5KnimeDataType.getKnimeDataType(editDataType.getOutputType(), true), false, true, editDataType.getStringLength());
		long[] dims = edit.getNumberOfDimensions() == 1 ? new long[] { edit.getInputRowCount() }
				: new long[] { edit.getInputRowCount(), edit.getColumnInputTypes().length };
		
		return createDataSet(edit.getName(), dims, edit.getCompressionLevel(), edit.getChunkRowSize(), dataType);
	}

	/**
	 * @return the specification of all descendant dataSets
	 * @throws IllegalStateException if a descendant is not open
	 */
	public DataTableSpec createSpecOfDataSets() throws IllegalStateException {
		return createSpecOfObjects(HDF5Constants.H5I_DATASET);
	}

	/**
	 * @param name name of the child dataSet of this treeElement
	 * @throws UnsupportedDataTypeException if the data type is not supported
	 * 	(for {@code HDF5Constants.H5T_VLEN, HDF5Constants.H5T_REFERENCE, HDF5Constants.H5T_COMPOUND})
	 * @throws IOException if an internal error occurred
	 */
	Hdf5DataType findDataSetType(String name) throws UnsupportedDataTypeException, IOException {
		Hdf5DataType dataType = null;
		
		if (getObjectTypeByName(name) == HDF5Constants.H5I_DATASET) {
			long dataSetId = -1;
			
			try {
				dataSetId = H5.H5Dopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				dataType = Hdf5DataType.openDataType(dataSetId);
				
			} catch (UnsupportedDataTypeException udte) {
				throw udte;
				
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				throw new IOException("DataType for dataSet \"" + name + "\" in group \""
						+ getPathFromFileWithName() + "\" could not be created: " + hlnpe.getMessage(), hlnpe);
				
			} finally {
				try {
					if (dataSetId >= 0) {
						H5.H5Dclose(dataSetId);
					}
				} catch (HDF5LibraryException hle) {
					NodeLogger.getLogger(getClass()).error("DataSet \"" + name + "\" in group \""
							+ getPathFromFileWithName() + "\" could not be closed");
				}
			}

			return dataType;
		} else {
			throw new IllegalArgumentException("DataSet \"" + name + "\" in group \""
					+ getPathFromFileWithName() + "\" does not exist");
		}
	}

	@Override
	public boolean open() throws IOException, IllegalStateException {
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		
		try {
			lockWriteOpen();
			
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}

				checkExists();
				
				setElementId(H5.H5Gopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT));
			}
			
            return true;
            
		} catch (HDF5LibraryException | IOException | NullPointerException hlnpe) {
			throw new IOException("Group \"" + getPathFromFileWithName()
					+ "\" could not be opened: " + hlnpe.getMessage(), hlnpe);
			
        } finally {
        	unlockWriteOpen();
        }
	}

	@Override
	public boolean close() throws IOException {
		try {
			lockWriteOpen();
			checkExists();
			
			boolean success = true;
			
            if (isOpen()) {
	    		for (Hdf5DataSet<?> ds : getDataSets()) {
        			success &= ds.close();
        		}

	    		for (Hdf5Attribute<?> attr : getAttributes()) {
        			success &= attr.close();
        		}
	    		
	    		for (Hdf5Group group : getGroups()) {
        			success &= group.close();
        		}
	    		
	    		success &= H5.H5Gclose(getElementId()) >= 0;
	    		if (success) {
	    			setElementId(-1);
	    		}
            }
            
            return success;
            
        } catch (HDF5LibraryException | IOException hlioe) {
        	throw new IOException("Group \"" + getPathFromFileWithName()
					+ "\" could not be closed: " + hlioe.getMessage(), hlioe);
        	
        } finally {
        	unlockWriteOpen();
        }
	}
}

package org.knime.hdf5.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.DataSetNodeEdit;
import org.knime.hdf5.nodes.writer.edit.EditDataType;
import org.knime.hdf5.nodes.writer.edit.GroupNodeEdit;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Group extends Hdf5TreeElement {
	
	public static final int OBJECT_NOT_EXISTS = 0;
	
	private final List<Hdf5Group> m_groups = new ArrayList<>();
	
	private final List<Hdf5DataSet<?>> m_dataSets = new ArrayList<>();

	protected Hdf5Group(final String name)
			throws NullPointerException, IllegalArgumentException {
		super(name);
	}
	
	private static Hdf5Group getInstance(final Hdf5Group parent, final String name) throws IllegalStateException {
		if (parent == null) {
			throw new IllegalArgumentException("Parent of group \"" + name + "\" cannot be null");
			
		} else if (!parent.isOpen()) {
			throw new IllegalStateException("Parent group \"" + parent.getName() + "\" is not open!");
		}
		
		return new Hdf5Group(name);
	}
	
	private static Hdf5Group createGroup(final Hdf5Group parent, final String name) {
		Hdf5Group group = null;
		
		try {
			group = getInstance(parent, name);

			group.setElementId(H5.H5Gcreate(parent.getElementId(), group.getName(), 
					HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
					HDF5Constants.H5P_DEFAULT));
			parent.addGroup(group);
			group.setOpen(true);
    		
		} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException | IllegalStateException hlnpiaise) {
            NodeLogger.getLogger("HDF5 Files").error("Group could not be created: " + hlnpiaise.getMessage(), hlnpiaise);
			/* group stays null */
        }
		
		return group;
	}

	private static Hdf5Group openGroup(final Hdf5Group parent, final String name) {
		Hdf5Group group = null;
		
		try {
			group = getInstance(parent, name);
			parent.addGroup(group);
			
			group.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
            NodeLogger.getLogger("HDF5 Files").error("Group could not be opened: " + npiaise.getMessage(), npiaise);
			/* group stays null */
        }
		
		return group;
	}

	/**
	 * @return a list of all children groups of this group
	 */
	public List<Hdf5Group> getGroups() {
		return m_groups;
	}
	
	/**
	 * @return a list of all dataSets of this group
	 */
	public List<Hdf5DataSet<?>> getDataSets() {
		return m_dataSets;
	}
	
	/**
	 * Creates a new child group with the name {@code name}.
	 * If a group with this name already physically exists in the .h5 file,
	 * the method will do nothing and return {@code null}. <br>
	 * The name may not contain '/'.
	 * 
	 * @param name the name of the new group
	 * 
	 * @return the new group 
	 * @throws IOException 
	 */
	public synchronized Hdf5Group createGroup(final String name) throws IOException {
		Hdf5Group group = null;
		
		int objectType = -1;
		try {
			objectType = getObjectTypeByName(name);
			
			if (objectType == OBJECT_NOT_EXISTS) {
				group = createGroup(this, name);
				
			} else if (objectType == HDF5Constants.H5I_GROUP || objectType == HDF5Constants.H5I_DATASET) {
				throw new IOException("There is already "
						+ (objectType == HDF5Constants.H5I_GROUP ? "a group" 
							: (objectType == HDF5Constants.H5I_DATASET ? "a dataSet" : "an object"))
						+ " with the name \"" + name + "\" in the group \"" + getPathFromFileWithName() + "\"");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger(getClass()).error(hlnpe.getMessage(), hlnpe);
			/* group stays null */
		}
		
		return group;
	}
	
	public synchronized Hdf5DataSet<?> createDataSet(final String name, long[] dimensions,
			int compressionLevel, long chunkRowSize, Hdf5DataType type) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		
		int objectType = -1;
		try {
			objectType = getObjectTypeByName(name);
			
			if (objectType == OBJECT_NOT_EXISTS) {
				dataSet = Hdf5DataSet.createDataSet(this, name, dimensions, compressionLevel, chunkRowSize, type);
				
			} else if (objectType == HDF5Constants.H5I_DATASET || objectType == HDF5Constants.H5I_GROUP) {
				throw new IOException("There is already "
						+ (objectType == HDF5Constants.H5I_DATASET ? "a dataSet" 
							: (objectType == HDF5Constants.H5I_GROUP ? "a group" : "an object"))
						+ " with the name \"" + name + "\" in the group \"" + getPathFromFileWithName() + "\"");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger(getClass()).error(hlnpe.getMessage(), hlnpe);
			/* dataSet stays null */
		}
		
		return dataSet;
	}
	
	public synchronized Hdf5TreeElement moveObject(final String oldName, Hdf5Group newParent, String newName) {
		Hdf5TreeElement newObject = null;
		
		try {
			int objectType = getObjectTypeByName(oldName);
			Hdf5TreeElement oldObject = objectType == HDF5Constants.H5I_GROUP ? getGroup(oldName) : getDataSet(oldName);
			H5.H5Lmove(getElementId(), oldName, newParent.getElementId(), newName, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
			if (getObjectTypeByName(oldName) == OBJECT_NOT_EXISTS && newParent.getObjectTypeByName(newName) == objectType) {
				if (oldObject instanceof Hdf5Group) {
					removeGroup((Hdf5Group) oldObject);
				} else {
					removeDataSet((Hdf5DataSet<?>) oldObject);
				}
				newObject = objectType == HDF5Constants.H5I_GROUP ? getGroup(newName) : getDataSet(newName);
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			NodeLogger.getLogger(getClass()).error("Object \"" + getPathFromFileWithName(true) + oldName
					+ "\" could not be moved to group \"" + newParent.getPathFromFileWithName(true) + "\" with new name \""
					+ newName + "\": " + hlionpe.getMessage(), hlionpe);
		}
		
		return newObject;
	}

	public synchronized Hdf5TreeElement copyObject(final String oldName, Hdf5Group newParent, String newName) {
		Hdf5TreeElement newObject = null;
		
		try {
			int objectType = getObjectTypeByName(oldName);
			H5.H5Ocopy(getElementId(), oldName, newParent.getElementId(), newName, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
			if (newParent.getObjectTypeByName(newName) == objectType) {
				newObject = objectType == HDF5Constants.H5I_GROUP ? getGroup(newName) : getDataSet(newName);
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			NodeLogger.getLogger(getClass()).error("Object \"" + getPathFromFileWithName(true) + oldName
					+ "\" could not be copied to group \"" + newParent.getPathFromFileWithName(true) + "\" with new name \""
					+ newName + "\": " + hlionpe.getMessage(), hlionpe);
		}
		
		return newObject;
	}
	
	public synchronized Hdf5Group getGroup(final String name) throws IOException {
		Hdf5Group group = null;
		
		Iterator<Hdf5Group> iter = getGroups().iterator();
		boolean found = false;
		while (!found && iter.hasNext()) {
			Hdf5Group grp = iter.next();
			if (grp.getName().equals(name)) {
				group = grp;
				group.open();
				found = true;
			}
		}
		
		if (!found) {
			int objectType = -1;
			try {
				objectType = getObjectTypeByName(name);
				
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				NodeLogger.getLogger(getClass()).error(hlnpe.getMessage(), hlnpe);
				/* dataSet stays null */
			}
			
			if (objectType == HDF5Constants.H5I_GROUP) {
				group = openGroup(this, name);
			
			} else {
				throw new IOException("Group \"" + getPathFromFileWithName() + name + "\" does not exist");
			}
		}
		
		return group;
	}
	
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
	
	public synchronized Hdf5DataSet<?> getDataSet(final String name) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		Iterator<Hdf5DataSet<?>> iter = getDataSets().iterator();
		boolean found = false;
		
		while (!found && iter.hasNext()) {
			Hdf5DataSet<?> ds = iter.next();
			if (ds.getName().equals(name)) {
				dataSet = ds;
				dataSet.open();
				found = true;
			}
		}
		
		if (!found) {
			int objectType = -1;
			try {
				objectType = getObjectTypeByName(name);
				
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				NodeLogger.getLogger(getClass()).error(hlnpe.getMessage(), hlnpe);
				/* dataSet stays null */
			}
			
			if (objectType == HDF5Constants.H5I_DATASET) {
				dataSet = Hdf5DataSet.openDataSet(this, name);
				
			} else {
				throw new IOException("DataSet \"" + getPathFromFileWithName() + name + "\" does not exist");
			}
		}
	
		return dataSet;
	}
	
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
	
	public Hdf5DataSet<?> updateDataSet(final String name) throws IOException {
		for (Hdf5DataSet<?> dataSet : getDataSets()) {
			if (dataSet.getName().equals(name)) {
				removeDataSet(dataSet);
				break;
			}
		}
		
		return getDataSet(name);
	}
	
	private int getObjectTypeByName(final String name) throws HDF5LibraryException, NullPointerException {
		try {
			if (H5.H5Lexists(getElementId(), name, HDF5Constants.H5P_DEFAULT)
					&& H5.H5Oexists_by_name(getElementId(), name, HDF5Constants.H5P_DEFAULT)) {
				long elementId = H5.H5Oopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				int typeId = H5.H5Iget_type(elementId); 
				H5.H5Oclose(elementId);
				
				return typeId;
			}
		} catch (HDF5LibraryException hle) {
			HDF5LibraryException newHle = new HDF5LibraryException("Existence of object could not be checked (" + hle.getMessage() + ")");
			newHle.setStackTrace(hle.getStackTrace());
			throw newHle;
		}
		
		return OBJECT_NOT_EXISTS;
	}
	
	public synchronized int deleteObject(final String name) throws IOException {
		int success = -1;
		
		try {
			int objectType = getObjectTypeByName(name);
			if (objectType == HDF5Constants.H5I_GROUP || objectType == HDF5Constants.H5I_DATASET) {
				Hdf5TreeElement object = objectType == HDF5Constants.H5I_GROUP ? getGroup(name) : getDataSet(name);
				if (objectType == HDF5Constants.H5I_GROUP) {
					Hdf5Group group = (Hdf5Group) object;
					for (String childName : group.loadObjectNames()) {
						group.deleteObject(childName);
					}
				}
				for (String childName : object.loadAttributeNames()) {
					object.deleteAttribute(childName);
				}

				object.close();
				if (!object.isOpen()) {
					H5.H5Ldelete(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				}
				
				if (getObjectTypeByName(name) == OBJECT_NOT_EXISTS) {
					if (object instanceof Hdf5Group) {
						success = removeGroup((Hdf5Group) object) ? 1 : 0;
					} else {
						success = removeDataSet((Hdf5DataSet<?>) object) ? 1 : 0;
					}
				}
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
		m_groups.add(group);
		group.setPathFromFile(getPathFromFileWithName(true));
		group.setParent(this);
	}

	void addDataSet(Hdf5DataSet<?> dataSet) {
		m_dataSets.add(dataSet);
		dataSet.setPathFromFile(getPathFromFileWithName(true));
		dataSet.setParent(this);
	}

	private boolean removeGroup(Hdf5Group group) {
		group.setParent(null);
		group.setPathFromFile("");
		group.setElementId(-1);
		return m_groups.remove(group);
	}

	private boolean removeDataSet(Hdf5DataSet<?> dataSet) {
		dataSet.setParent(null);
		dataSet.setPathFromFile("");
		dataSet.setElementId(-1);
		return m_dataSets.remove(dataSet);
	}

	private List<String> loadObjectNames(int objectId) throws IllegalStateException {
		List<String> names = new ArrayList<>();
		Hdf5Group group = isFile() ? this : getParent();
		String name = isFile() ? "/" : getName();
		
		try {
			if (group.isOpen()) {
				int count = (int) H5.H5Gn_members(group.getElementId(), name);
				
				if (count > 0) {
					String[] oname = new String[count];
					int[] otype = new int[count];
					H5.H5Gget_obj_info_all(group.getElementId(), name, oname, otype, new int[count], new long[count], HDF5Constants.H5_INDEX_NAME);
					
					// Get type of the object and add it to the list
					for (int i = 0; i < otype.length; i++) {
						if (objectId == HDF5Constants.H5O_TYPE_UNKNOWN || objectId == otype[i]) {
							names.add(oname[i]);
						}
					}
				}
			} else {
				throw new IllegalStateException("The parent group \"" + group.getPathFromFileWithName() + "\" is not open!");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("List of objects could not be loaded: " + hlnpe.getMessage(), hlnpe);
        }
		
		return names;
	}
	
	public List<String> loadObjectNames() throws IllegalStateException {
		return loadObjectNames(HDF5Constants.H5O_TYPE_UNKNOWN);
	}
	
	public List<String> loadGroupNames() throws IllegalStateException {
		return loadObjectNames(HDF5Constants.H5O_TYPE_GROUP);
	}
	
	public List<String> loadDataSetNames() throws IllegalStateException {
		return loadObjectNames(HDF5Constants.H5O_TYPE_DATASET);
	}
	
	public Map<String, Hdf5DataType> getAllDataSetsInfo() throws IllegalStateException {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		
		if (isOpen()) {
			String path = getPathFromFileWithName(true);
			
			Iterator<String> iterDS = loadDataSetNames().iterator();
			while (iterDS.hasNext()) {
				String name = iterDS.next();
				
				try {
					Hdf5DataType dataType = findDataSetType(name);
					paths.put(path + name, dataType);

				} catch (IllegalArgumentException iae) {
					NodeLogger.getLogger("HDF5 Files").error(iae.getMessage(), iae);
					
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
				}
			}
			
			Iterator<String> iterG = loadGroupNames().iterator();
			while (iterG.hasNext()) {
				try {
					Hdf5Group group = getGroup(iterG.next());
					paths.putAll(group.getAllDataSetsInfo());
					
				} catch (IOException | NullPointerException ionpe) {
					NodeLogger.getLogger("HDF5 Files").error(ionpe.getMessage(), ionpe);
				}
			}
		} else {
			throw new IllegalStateException("Group \"" + getPathFromFileWithName() + "\" is not open");
		}
		
		return paths;
	}

	public Hdf5Group createGroupFromEdit(GroupNodeEdit edit) throws IOException {
		Hdf5Group group = null;
		
		try {
			String name = edit.getName();
			int objectType = getObjectTypeByName(name);
			if (objectType == OBJECT_NOT_EXISTS) {
				group = createGroup(name);
			} else if (objectType == HDF5Constants.H5I_GROUP && edit.isOverwrite()) {
				if (deleteObject(name) >= 0) {
					group = createGroup(name);
				}
			} else {
				throw new IOException("Object with the same name \""
						+ name + "\" already exists in group \"" + getPathFromFileWithName() + "\"");
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger(getClass()).warn(hle.getMessage(), hle);
		}
		
		return group;
	}
	
	public Hdf5DataSet<?> createDataSetFromEdit(DataSetNodeEdit edit) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		
		EditDataType editDataType = edit.getEditDataType();
		Hdf5DataType dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(editDataType.getOutputType(), editDataType.getEndian()),
				Hdf5KnimeDataType.getKnimeDataType(editDataType.getOutputType(), true), false, true, editDataType.getStringLength());
		long[] dims = edit.getNumberOfDimensions() == 1 ? new long[] { edit.getInputRowCount() }
				: new long[] { edit.getInputRowCount(), edit.getColumnInputTypes().length };
		
		try {
			String name = edit.getName();
			int compressionLevel = edit.getCompressionLevel();
			long chunkRowSize = edit.getChunkRowSize();
			
			int objectType = getObjectTypeByName(name);
			if (objectType == OBJECT_NOT_EXISTS) {
				dataSet = createDataSet(name, dims, compressionLevel, chunkRowSize, dataType);
			} else if (objectType == HDF5Constants.H5I_DATASET && edit.isOverwrite()) {
				dataSet = getDataSet(name);
				if (dataType.isSimilarTo(dataSet.getType()) && dims.equals(dataSet.getDimensions())
						&& compressionLevel == dataSet.getCompressionLevel() && chunkRowSize == dataSet.getChunkRowSize()
						&& dataSet.clearData()) {
					// nothing to do; only return dataSet at the end
				} else if (deleteObject(name) >= 0) {
					dataSet = createDataSet(name, dims, compressionLevel, chunkRowSize, dataType);
				}
			} else {
				throw new IOException("Object with the same name \""
						+ name + "\" already exists in group \"" + getPathFromFileWithName() + "\"");
			}
		} catch (HDF5LibraryException hle) {
			NodeLogger.getLogger(getClass()).warn(hle.getMessage(), hle);
		}
		
		return dataSet;
	}

	public DataTableSpec createSpecOfDataSets() throws IllegalStateException {
		return createSpecOfObjects(HDF5Constants.H5I_DATASET);
	}

	/**
	 * 
	 * @param name name of the attribute in this treeElement
	 * @return dataType of the attribute (TODO doesn't work for {@code H5T_VLEN} and {@code H5T_REFERENCE} at the moment)
	 * @throws UnsupportedDataTypeException 
	 */
	Hdf5DataType findDataSetType(String name) throws UnsupportedDataTypeException, IllegalArgumentException {
		Hdf5DataType dataType = null;
		
		int objectType = -1;
		try {
			objectType = getObjectTypeByName(name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger(getClass()).error(hlnpe.getMessage(), hlnpe);
		}
		
		if (objectType == HDF5Constants.H5I_DATASET) {
			long dataSetId = -1;
			long classId = -1;
			int size = 0;
			Endian endian = null;
			boolean unsigned = false;
			boolean vlen = false;
			
			try {
				dataSetId = H5.H5Dopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				long typeId = H5.H5Dget_type(dataSetId);
				classId = H5.H5Tget_class(typeId);
				size = (int) H5.H5Tget_size(typeId);
				endian = H5.H5Tget_order(typeId) == HDF5Constants.H5T_ORDER_LE ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN;
				vlen = classId == HDF5Constants.H5T_VLEN || H5.H5Tis_variable_str(typeId);
				if (classId == HDF5Constants.H5T_INTEGER) {
					unsigned = HDF5Constants.H5T_SGN_NONE == H5.H5Tget_sign(typeId);
				}
				H5.H5Tclose(typeId);
				
				// unsupported dataSets
				if (classId == HDF5Constants.H5T_VLEN || classId == HDF5Constants.H5T_REFERENCE || classId == HDF5Constants.H5T_COMPOUND) {
					H5.H5Dclose(dataSetId);
					throw new UnsupportedDataTypeException("DataType " + H5.H5Tget_class_name(classId) + " of dataSet \""
							+ getPathFromFileWithName(true) + name + "\" is not supported");
				}
				
				dataType = Hdf5DataType.openDataType(dataSetId, classId, size, endian, unsigned, vlen);
				
				H5.H5Dclose(dataSetId);
				
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				NodeLogger.getLogger("Hdf5 Files").error("DataType for \"" + name + "\" could not be created", hlnpe);	
			}

			return dataType;
		} else {
			throw new IllegalArgumentException("There isn't a dataSet \"" + name + "\" in group \""
					+ getPathFromFile() + getName() + "\"");
		}
	}

	@Override
	public boolean open() {
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		try {
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				setElementId(H5.H5Gopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT));
				setOpen(true);
				
                return true;
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Group could not be opened", hlnpe);
        }

        return false;
	}

	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
	@Override
	public boolean close() {
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
	    		
	    		H5.H5Gclose(getElementId());
		        setOpen(false);
		        
		        // TODO should only be true if all descendants are closed
                return true;
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("Group could not be closed", hle);
        }
		
        return false;
	}
}

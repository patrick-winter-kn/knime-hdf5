package org.knime.hdf5.lib;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Group extends Hdf5TreeElement {
	
	public static final int OBJECT_NOT_EXISTS = 0;
	
	private final List<Hdf5Group> m_groups = new ArrayList<>();
	
	private final List<Hdf5DataSet<?>> m_dataSets = new ArrayList<>();

	protected Hdf5Group(final Hdf5Group parent, final String filePath, final String name, boolean create)
			throws HDF5LibraryException, NullPointerException, IllegalArgumentException {
		super(name, filePath);
		
		if (!(this instanceof Hdf5File)) {
			if (parent == null) {
				throw new NullPointerException("Parent of group \"" + name + "\" cannot be null");
				
			} else if (!parent.isOpen()) {
				throw new IllegalStateException("Parent group \"" + parent.getName() + "\" is not open!");
				
			} else if (!create) {
				parent.addGroup(this);
				open();
			} else {
				setElementId(H5.H5Gcreate(parent.getElementId(), getName(), 
						HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
						HDF5Constants.H5P_DEFAULT));
				parent.addGroup(this);
				setOpen(true);
			}
		}
	}

	/**
	 * @return a list of all children groups of this group
	 */
	protected List<Hdf5Group> getGroups() {
		return m_groups;
	}
	
	/**
	 * @return a list of all dataSets of this group
	 */
	protected List<Hdf5DataSet<?>> getDataSets() {
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
	 */
	public Hdf5Group createGroup(final String name, Hdf5OverwritePolicy policy) {
		Hdf5Group group = null;
		
		int objectType = -1;
		try {
			objectType = getObjectTypeByName(name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of object could not be checked", hlnpe);
			/* group stays null */
		}
		
		if (objectType == HDF5Constants.H5I_DATASET) {
			NodeLogger.getLogger("HDF5 Files").error("There is already a dataSet with the name \"" + name + "\"", new IllegalArgumentException());
			/* group stays null */
			
		} else if (objectType == OBJECT_NOT_EXISTS
				|| (policy == Hdf5OverwritePolicy.OVERWRITE && objectType == HDF5Constants.H5I_GROUP)) {
			try {
				group = new Hdf5Group(this, getFilePath(), name, true);
				
			} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
				NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
				/* group stays null */
			}
			
		} else if (policy == Hdf5OverwritePolicy.ABORT && objectType == HDF5Constants.H5I_GROUP) {
			group = getGroup(name);
			
		} else if (policy == Hdf5OverwritePolicy.KEEP_BOTH && objectType == HDF5Constants.H5I_GROUP) {
			String oldName = name;
			int i = 1;
			
			if (oldName.matches(".*\\([1-9][0-9]*\\)")) {
				oldName = oldName.substring(0, oldName.lastIndexOf("("));
				i = Integer.parseInt(oldName.substring(oldName.lastIndexOf("(") + 1, oldName.lastIndexOf(")")));
			}
			
			String newName;
			List<String> groupNames = loadGroupNames();
			do {
				 newName = oldName + "(" + i + ")";
				 i++;
			} while (groupNames.contains(newName));
			
			try {
				group = new Hdf5Group(this, getFilePath(), newName, true);
				
			} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
				NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
				/* group stays null */
			}
		}
		
		return group;
	}
	
	public Hdf5DataSet<?> createDataSet(final String name, long[] dimensions, 
			Hdf5DataType type, Hdf5OverwritePolicy policy) {
		Hdf5DataSet<?> dataSet = null;
		
		int objectType = -1;
		try {
			objectType = getObjectTypeByName(name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of object could not be checked", hlnpe);
			/* dataSet stays null */
		}
		
		if (objectType == HDF5Constants.H5I_GROUP) {
			NodeLogger.getLogger("HDF5 Files").error("There is already a group with the name \"" + name + "\"", new IllegalArgumentException());
			/* dataSet stays null */
			
		} else if (objectType == OBJECT_NOT_EXISTS) {
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, type, true);
			
		} else if (policy == Hdf5OverwritePolicy.OVERWRITE && objectType == HDF5Constants.H5I_DATASET) {
			// TODO implement possibility to overwrite if necessary
			NodeLogger.getLogger("HDF5 Files").error("A dataSet cannot be overwritten", new UnsupportedOperationException());
			/* dataSet stays null */
			
		} else if (policy == Hdf5OverwritePolicy.ABORT && objectType == HDF5Constants.H5I_DATASET) {
			dataSet = getDataSet(name);
			
		} else if (policy == Hdf5OverwritePolicy.KEEP_BOTH && objectType == HDF5Constants.H5I_DATASET) {
			String oldName = name;
			int i = 1;
			
			if (oldName.matches(".*\\([1-9][0-9]*\\)")) {
				oldName = oldName.substring(0, oldName.lastIndexOf("("));
				i = Integer.parseInt(oldName.substring(oldName.lastIndexOf("(") + 1, oldName.lastIndexOf(")")));
			}
			
			String newName;
			List<String> dataSetNames = loadDataSetNames();
			do {
				 newName = oldName + "(" + i + ")";
				 i++;
			} while (dataSetNames.contains(newName));
			
			dataSet = Hdf5DataSet.getInstance(this, newName, dimensions, type, true);
		}
		
		return dataSet;
	}
	
	public Hdf5Group getGroup(final String name) {
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
			try {
				group = new Hdf5Group(this, getFilePath(), name, false);
			} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
				NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
				/* group stays null */
			}
		}
		
		return group;
	}
	
	public Hdf5Group getGroupByPath(String path) {
		Hdf5Group group = null;
		String name = path.split("/")[0];
		
		if (path.contains("/")) {
			Hdf5Group grp = getGroup(name);
			
			if (grp != null) {
				group = grp.getGroupByPath(path.substring(name.length() + 1));
			}
		} else {
			group = getGroup(name);
		}
		
		return group;
	}
	
	public Hdf5DataSet<?> getDataSet(final String name) {
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
		
		int objectType = -1;
		try {
			objectType = getObjectTypeByName(name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of object could not be checked", hlnpe);
			/* dataSet stays null */
		}
		
		if (!found && objectType == HDF5Constants.H5I_DATASET) {
			long dataSetId = -1;
			long dataspaceId = -1;
			long[] dimensions = null;
			
			try {
				Hdf5DataType type = findDataSetType(name);
				dataSetId = H5.H5Dopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				dataspaceId = H5.H5Dget_space(dataSetId);

				int ndims = H5.H5Sget_simple_extent_ndims(dataspaceId);
				long[] dims = new long[ndims];
				H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
				
                H5.H5Sclose(dataspaceId);
                H5.H5Dclose(dataSetId);
				
				dimensions = dims;
				
				dataSet = Hdf5DataSet.getInstance(this, name, dimensions, type, false);
				
			} catch (HDF5LibraryException | NullPointerException | IllegalArgumentException hlnpiae) {
				NodeLogger.getLogger("HDF5 Files").error(hlnpiae.getMessage(), hlnpiae);
				
			} catch (UnsupportedDataTypeException udte) {
				NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
			}
			
		}
	
		return dataSet;
	}
	
	public Hdf5DataSet<?> getDataSetByPath(String path) {
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
	
	private int getObjectTypeByName(final String name) throws HDF5LibraryException, NullPointerException {
		if (H5.H5Oexists_by_name(getElementId(), name, HDF5Constants.H5P_DEFAULT)) {
			long elementId = H5.H5Oopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
			int typeId = H5.H5Iget_type(elementId); 
			H5.H5Oclose(elementId);
			
			return typeId;
		}
		
		return OBJECT_NOT_EXISTS;
	}
	
	private void addGroup(Hdf5Group group) {
		getGroups().add(group);
		group.setPathFromFile(getPathFromFileWithName(false));
		group.setParent(this);
	}

	void addDataSet(Hdf5DataSet<?> dataSet) {
		getDataSets().add(dataSet);
		dataSet.setPathFromFile(getPathFromFileWithName(false));
		dataSet.setParent(this);
	}

	private List<String> loadObjectNames(int objectId) {
		List<String> names = new ArrayList<>();
		Hdf5Group group = this instanceof Hdf5File ? this : getParent();
		String name = this instanceof Hdf5File ? "/" : getName();
		
		try {
			if (group.isOpen()) {
				int count = (int) H5.H5Gn_members(group.getElementId(), name);
				
				if (count > 0) {
					String[] oname = new String[count];
					int[] otype = new int[count];
					H5.H5Gget_obj_info_all(group.getElementId(), name, oname, otype, new int[count], new long[count], HDF5Constants.H5_INDEX_NAME);
					
					// Get type of the object and add it to the list
					for (int i = 0; i < otype.length; i++) {
						if (otype[i] == objectId) {
							names.add(oname[i]);
						}
					}
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("The parent \"" + group.getPathFromFile() + group.getName() + "\" is not open!", new IllegalStateException());
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("List of objects could not be loaded", hlnpe);
        }
		
		return names;
	}
	
	public List<String> loadGroupNames() {
		return loadObjectNames(HDF5Constants.H5O_TYPE_GROUP);
	}
	
	public List<String> loadDataSetNames() {
		return loadObjectNames(HDF5Constants.H5O_TYPE_DATASET);
	}
	
	public List<String> getAllDataSetPaths() {
		List<String> paths = new ArrayList<>();
		String path = getPathFromFileWithName(false);
		
		Iterator<String> iterDS = loadDataSetNames().iterator();
		while (iterDS.hasNext()) {
			paths.add(path + iterDS.next());
		}
		
		Iterator<String> iterG = loadGroupNames().iterator();
		while (iterG.hasNext()) {
			Hdf5Group group = getGroup(iterG.next());
			paths.addAll(group.getAllDataSetPaths());
		}
		
		return paths;
	}

	/**
	 *
	 * @param object only useful for HDF5Constants.ATTR and HDF5Constants.DATASET
	 * @return
	 */
	private Map<String, Hdf5DataType> getAllObjectsInfoOfType(int objectId) throws IllegalStateException {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		
		if (isOpen()) {
			String path = getPathFromFileWithName(false);
			
			if (objectId == HDF5Constants.H5I_ATTR) {
				Iterator<String> iterAttr = loadAttributeNames().iterator();
				while (iterAttr.hasNext()) {
					String name = iterAttr.next();
					
					try {
						Hdf5DataType dataType = findAttributeType(name);
						paths.put(path + name, dataType);
						
					} catch (IllegalArgumentException iae) {
						NodeLogger.getLogger("HDF5 Files").error(iae.getMessage(), iae);
						
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
					}
				}
				
				Iterator<String> iterDS = loadDataSetNames().iterator();
				while (iterDS.hasNext()) {
					Hdf5DataSet<?> dataSet = getDataSet(iterDS.next());
					if (dataSet != null) {
						paths.putAll(dataSet.getDirectAttributesInfo());
					}
				}
			} else if (objectId == HDF5Constants.H5I_DATASET) {
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
			}
			
			Iterator<String> iterG = loadGroupNames().iterator();
			while (iterG.hasNext()) {
				Hdf5Group group = getGroup(iterG.next());
				paths.putAll(group.getAllObjectsInfoOfType(objectId));
			}
		} else {
			throw new IllegalStateException("\"" + getPathFromFileWithName() + "\" is not open");
		}
		
		return paths;
	}
	
	public Map<String, Hdf5DataType> getAllDataSetsInfo() {
		return getAllObjectsInfoOfType(HDF5Constants.H5I_DATASET);
	}
	
	public Map<String, Hdf5DataType> getAllAttributesInfo() {
		return getAllObjectsInfoOfType(HDF5Constants.H5I_ATTR);
	}
	
	/**
	 *
	 * @param object only ATTRIBUTE and DATASET allowed
	 * @return
	 */
	private DataTableSpec createSpecOfObjects(int objectId) {
		List<DataColumnSpec> colSpecList = new ArrayList<>();
		
		Map<String, Hdf5DataType> objInfo = getAllObjectsInfoOfType(objectId);
		Iterator<String> iter = objInfo.keySet().iterator();
		while (iter.hasNext()) {
			String objPath = iter.next();
			Hdf5DataType dataType = objInfo.get(objPath);
			if (dataType != null) {
				try {
					DataType objType = dataType.getKnimeType().getColumnDataType();
					colSpecList.add(new DataColumnSpecCreator(objPath, objType).createSpec());
					
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
				}
			}	
		}
		
		return new DataTableSpec(colSpecList.toArray(new DataColumnSpec[] {}));
	}

	public DataTableSpec createSpecOfDataSets() {
		return createSpecOfObjects(HDF5Constants.H5I_DATASET);
	}
	
	public DataTableSpec createSpecOfAttributes() {
		return createSpecOfObjects(HDF5Constants.H5I_ATTR);
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
			NodeLogger.getLogger("HDF5 Files").error("Existence of object could not be checked", hlnpe);
		}
		
		if (objectType == HDF5Constants.H5I_DATASET) {
			long dataSetId = -1;
			long classId = -1;
			int size = 0;
			boolean unsigned = false;
			boolean vlen = false;
			
			try {
				dataSetId = H5.H5Dopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				long typeId = H5.H5Dget_type(dataSetId);
				classId = H5.H5Tget_class(typeId);
				size = (int) H5.H5Tget_size(typeId);
				vlen = classId == HDF5Constants.H5T_VLEN || H5.H5Tis_variable_str(typeId);
				if (classId == HDF5Constants.H5T_INTEGER /*TODO || classId == HDF5Constants.H5T_CHAR*/) {
					unsigned = HDF5Constants.H5T_SGN_NONE == H5.H5Tget_sign(typeId);
				}
				H5.H5Tclose(typeId);
				
				if (classId == HDF5Constants.H5T_VLEN) {
					// TODO find correct real dataType
					H5.H5Dclose(dataSetId);
					throw new UnsupportedDataTypeException("DataType H5T_VLEN of dataSet \""
							+ getPathFromFileWithName() + name + "\" is not supported");
				}
				
				if (classId == HDF5Constants.H5T_REFERENCE) {
					H5.H5Dclose(dataSetId);
					throw new UnsupportedDataTypeException("DataType H5T_REFERENCE of dataSet \""
							+ getPathFromFileWithName() + name + "\" is not supported");
				}
				
				dataType = new Hdf5DataType(classId, size, unsigned, vlen, true);
				if (classId == HDF5Constants.H5T_STRING) {
					dataType.getHdfType().initInstanceString(dataSetId);
				}
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
	
	public void open() {
		if (this instanceof Hdf5File) {
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
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Group could not be opened", hlnpe);
        }
	}
	
	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
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
	    		
	    		H5.H5Gclose(getElementId());
		        setOpen(false);
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("Group could not be closed", hle);
        }
	}
}

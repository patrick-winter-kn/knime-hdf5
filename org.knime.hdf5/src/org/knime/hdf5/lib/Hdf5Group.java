package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

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
	
	private final List<Hdf5Group> m_groups = new LinkedList<>();
	
	private final List<Hdf5DataSet<?>> m_dataSets = new LinkedList<>();

	protected Hdf5Group(final Hdf5Group parent, final String filePath, final String name, boolean create)
			throws NullPointerException, IllegalArgumentException {
		super(name, filePath);
		if (!(this instanceof Hdf5File)) {
			if (parent == null) {
				NodeLogger.getLogger("HDF5 Files").error("parent of group " + name + " cannot be null",
						new NullPointerException());
			} else if (!parent.isOpen()) {
				NodeLogger.getLogger("HDF5 Files").error("parent group " + parent.getName() + " is not open!",
						new IllegalStateException());
			} else if (!create) {
				parent.addGroup(this);
				open();
			} else {
				try {
					setElementId(H5.H5Gcreate(parent.getElementId(), getName(), 
							HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
							HDF5Constants.H5P_DEFAULT));
					parent.addGroup(this);
					setOpen(true);
				} catch (HDF5LibraryException | NullPointerException e) {
					e.printStackTrace();
				}
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
		
		if (!existsDataSet(name)) {
			if (!existsGroup(name) || policy == Hdf5OverwritePolicy.OVERWRITE) {
				try {
					group = new Hdf5Group(this, getFilePath(), name, true);
				} catch (NullPointerException | IllegalArgumentException npiae) {
					group = null;
				}
				
			} else if (policy == Hdf5OverwritePolicy.ABORT) {
				group = getGroup(name);
				
			} else if (policy == Hdf5OverwritePolicy.KEEP_BOTH) {
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
					 System.out.print(newName + ", ");
				} while (groupNames.contains(newName));
	
				group = new Hdf5Group(this, getFilePath(), newName, true);
			}
		} else {
			NodeLogger.getLogger("HDF5 Files").error("There is already a dataSet with the name " + name, new IllegalArgumentException());
		}
		
		return group;
	}
	
	public Hdf5DataSet<?> createDataSet(final String name, long[] dimensions, 
			long stringLength, Hdf5DataType type, Hdf5OverwritePolicy policy) {
		Hdf5DataSet<?> dataSet = null;
		
		if (!existsGroup(name)) {
			if (!existsDataSet(name)) {
				dataSet = Hdf5DataSet.getInstance(this, name, dimensions, stringLength, type, true);
			} else if (policy == Hdf5OverwritePolicy.OVERWRITE) {
				NodeLogger.getLogger("HDF5 Files").error("A dataSet cannot be overwritten", new IllegalArgumentException());
				
			} else if (policy == Hdf5OverwritePolicy.ABORT) {
				dataSet = getDataSet(name);
				
			} else if (policy == Hdf5OverwritePolicy.KEEP_BOTH) {
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
					 System.out.print(newName + ", ");
				} while (dataSetNames.contains(newName));
				
				dataSet = Hdf5DataSet.getInstance(this, newName, dimensions, stringLength, type, true);
			}
		} else {
			NodeLogger.getLogger("HDF5 Files").error("There is already a group with the name " + name, new IllegalArgumentException());
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
			} catch (NullPointerException | IllegalArgumentException npiae) {
				group = null;
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
		
		if (!found && existsDataSet(name)) {
			long dataSetId = -1;
			long dataspaceId = -1;
			long[] dimensions = null;
			Hdf5DataType type = findDataSetType(name);
			
			if (type == null) {
				return null;
			}
			
			try {
				dataSetId = H5.H5Dopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				dataspaceId = H5.H5Dget_space(dataSetId);

				int ndims = -1;
				if (dataspaceId >= 0) {
					ndims = H5.H5Sget_simple_extent_ndims(dataspaceId);
				}
				
				long[] dims = new long[ndims];
				if (dataspaceId >= 0) {
					H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
				}
				
                // Terminate access to the data space.
                if (dataspaceId >= 0) {
                	H5.H5Sclose(dataspaceId);
                }

                // Terminate access to the datset.
                H5.H5Dclose(dataSetId);
				
				dimensions = dims;
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
			
			long stringLength = type.getHdfType().getStringLength();
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, stringLength, type, false);
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
	
	public boolean existsGroup(final String name) {
		List<String> groupNames = loadGroupNames();
		
		if (groupNames != null) {
			for (String n: groupNames) {
				if (n.equals(name)) {
					return true;
				}
			}
		}
		
		return false;
	}
	
	public boolean existsDataSet(final String name) {
		List<String> dataSetNames = loadDataSetNames();
		
		if (dataSetNames != null) {
			for (String n: dataSetNames) {
				if (n.equals(name)) {
					return true;
				}
			}
		}
		
		return false;
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

	/**
	 *
	 * @param object only GROUP and DATASET allowed
	 * @return
	 */
	private String[] loadObjectNames(Hdf5Object object) {
		if (object != Hdf5Object.GROUP && object != Hdf5Object.DATASET) {
			NodeLogger.getLogger("HDF5 Files").error("Inappropriate object type " + object, new InvalidTargetObjectTypeException());
		}
		
		String[] names = null;
		int index = 0;
		Hdf5Group group = this instanceof Hdf5File ? this : getParent();
		String name = this instanceof Hdf5File ? "/" : getName();
		
		// Begin iteration.
		try {
			if (group.isOpen()) {
				long count = H5.H5Gn_members(group.getElementId(), name);
				
				if (count > 0) {
					String[] oname = new String[(int) count];
					int[] otype = new int[(int) count];
					long[] orefs = new long[(int) count];
					H5.H5Gget_obj_info_all(group.getElementId(), name, oname, otype, new int[(int) count], orefs, HDF5Constants.H5_INDEX_NAME);
					
					// Get type of the object and add it to the array
					names = new String[(int) count];
					for (int i = 0; i < otype.length; i++) {
						if (Hdf5Object.get(otype[i]) == object) {
							names[index] = oname[i];
							index++;
						}
					}
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("The parent " + group.getPathFromFile() + group.getName() + " is not open!", new IllegalStateException());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return names != null ? Arrays.copyOf(names, index) : null;
	}
	
	public List<String> loadGroupNames() {
		List<String> names = new LinkedList<>();
		String[] groupNames = loadObjectNames(Hdf5Object.GROUP);
		if (groupNames != null) {
			Collections.addAll(names, groupNames);
		}
		return names;
	}
	
	public List<String> loadDataSetNames() {
		List<String> names = new LinkedList<>();
		String[] dataSetNames = loadObjectNames(Hdf5Object.DATASET);
		if (dataSetNames != null) {
			Collections.addAll(names, dataSetNames);
		}
		return names;
	}
	
	public List<String> getAllDataSetPaths() {
		List<String> paths = new LinkedList<>();
		String path = getPathFromFileWithName(false);
		
		Iterator<String> iterDS = loadDataSetNames().iterator();
		while (iterDS.hasNext()) {
			paths.add(path + iterDS.next());
		}
		
		Iterator<String> iterG = loadGroupNames().iterator();
		while (iterG.hasNext()) {
			Hdf5Group group = getGroup(iterG.next());
			paths.addAll(group.getAllDataSetPaths());
			group.close();
		}
		
		return paths;
	}

	/**
	 *
	 * @param object only ATTRIBUTE and DATASET allowed
	 * @return
	 */
	private Map<String, Hdf5DataType> getAllObjectsInfoOfType(Hdf5Object object) {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		String path = getPathFromFileWithName(false);
		
		if (object != Hdf5Object.DATASET && object != Hdf5Object.ATTRIBUTE) {
			NodeLogger.getLogger("HDF5 Files").error("Inappropriate object type " + object, new InvalidTargetObjectTypeException());
		}
		
		if (object == Hdf5Object.ATTRIBUTE) {
			Iterator<String> iterAttr = loadAttributeNames().iterator();
			while (iterAttr.hasNext()) {
				String name = iterAttr.next();
				Hdf5DataType dataType = findAttributeType(name);
				if (dataType != null) {
					paths.put(path + name, dataType);
				}
			}
			
			Iterator<String> iterDS = loadDataSetNames().iterator();
			while (iterDS.hasNext()) {
				Hdf5DataSet<?> dataSet = getDataSet(iterDS.next());
				if (dataSet != null) {
					paths.putAll(dataSet.getDirectAttributesInfo());
					dataSet.close();
				}
			}
		} else {
			Iterator<String> iterDS = loadDataSetNames().iterator();
			while (iterDS.hasNext()) {
				String name = iterDS.next();
				Hdf5DataType dataType = findDataSetType(name);
				if (dataType != null) {
					paths.put(path + name, dataType);
				}
			}
		}
		
		Iterator<String> iterG = loadGroupNames().iterator();
		while (iterG.hasNext()) {
			Hdf5Group group = getGroup(iterG.next());
			paths.putAll(group.getAllObjectsInfoOfType(object));
			group.close();
		}
		
		return paths;
	}
	
	public Map<String, Hdf5DataType> getAllDataSetsInfo() {
		return getAllObjectsInfoOfType(Hdf5Object.DATASET);
	}
	
	public Map<String, Hdf5DataType> getAllAttributesInfo() {
		return getAllObjectsInfoOfType(Hdf5Object.ATTRIBUTE);
	}
	
	/**
	 *
	 * @param object only ATTRIBUTE and DATASET allowed
	 * @return
	 */
	private DataTableSpec createSpecOfObjects(Hdf5Object object) {
		if (object != Hdf5Object.ATTRIBUTE && object != Hdf5Object.DATASET) {
			NodeLogger.getLogger("HDF5 Files").error("Inappropriate object type " + object, new InvalidTargetObjectTypeException());
		}
		
		List<DataColumnSpec> colSpecList = new LinkedList<>();
		
		Map<String, Hdf5DataType> objInfo = getAllObjectsInfoOfType(object);
		Iterator<String> iter = objInfo.keySet().iterator();
		while (iter.hasNext()) {
			String objPath = iter.next();
			Hdf5DataType dataType = objInfo.get(objPath);
			if (dataType != null) {
				DataType objType = dataType.getKnimeType().getColumnType();
				
				colSpecList.add(new DataColumnSpecCreator(objPath, objType).createSpec());
			}	
		}
		
		return new DataTableSpec(colSpecList.toArray(new DataColumnSpec[] {}));
	}

	public DataTableSpec createSpecOfDataSets() {
		return createSpecOfObjects(Hdf5Object.DATASET);
	}
	
	public DataTableSpec createSpecOfAttributes() {
		return createSpecOfObjects(Hdf5Object.ATTRIBUTE);
	}

	/**
	 * 
	 * @param name name of the attribute in this treeElement
	 * @return dataType of the attribute (TODO doesn't work for {@code H5T_VLEN} and {@code H5T_REFERENCE} at the moment)
	 */
	Hdf5DataType findDataSetType(String name) {
		Hdf5DataType dataType = null;
		
		if (existsDataSet(name)) {
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
					NodeLogger.getLogger("HDF5 Files").warn("DataType H5T_VLEN of dataSet \""
							+ getPathFromFileWithName() + name + "\" is not supported");
					H5.H5Dclose(dataSetId);
					return null;
				}
				
				if (classId == HDF5Constants.H5T_REFERENCE) {
					NodeLogger.getLogger("HDF5 Files").warn("DataType H5T_REFERENCE of dataSet \""
							+ getPathFromFileWithName() + name + "\" is not supported");
					H5.H5Dclose(dataSetId);
					return null;
				}
				
				dataType = new Hdf5DataType(classId, size, unsigned, vlen, true);
				if (classId == HDF5Constants.H5T_STRING) {
					dataType.getHdfType().initInstanceString(dataSetId);
				}
				H5.H5Dclose(dataSetId);
				
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}

			return dataType;
		} else {
			NodeLogger.getLogger("HDF5 Files").error("There isn't a dataSet \"" + name + "\" in group"
					+ getPathFromFile() + getName(), new IllegalArgumentException());
			return null;
		}
	}
	
	public void open() {
		try {
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				setElementId(H5.H5Gopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT));
				setOpen(true);
			}
		} catch (HDF5LibraryException | NullPointerException e) {
			e.printStackTrace();
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
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

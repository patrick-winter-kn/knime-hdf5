package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

// TODO when creating a group, it should also check for dataSets with the same name!
// TODO same vice versa

public class Hdf5Group extends Hdf5TreeElement {
	
	private final List<Hdf5Group> m_groups = new LinkedList<>();
	
	private final List<Hdf5DataSet<?>> m_dataSets = new LinkedList<>();

	protected Hdf5Group(final Hdf5Group parent, final String filePath, final String name, boolean create) {
		super(name, filePath);
		if (!(this instanceof Hdf5File)) {
			if (parent == null) {
				NodeLogger.getLogger("HDF5 Files").error("parent cannot be null",
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
					NodeLogger.getLogger("HDF5 Files").info("Group " + getName() + " created: " + getElementId());
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
	 */
	// TODO add another argument for the mode (overwrite, abort, nameChange)
	public Hdf5Group createGroup(final String name, Hdf5OverwritePolicy policy) {
		Hdf5Group group = null;
		
		if (!existsGroup(name) || policy == Hdf5OverwritePolicy.OVERWRITE) {
			group = new Hdf5Group(this, getFilePath(), name, true);
			
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

			System.out.println("\nName of the new group: " + newName);
			group = new Hdf5Group(this, getFilePath(), newName, true);
		}
		
		return group;
	}
	
	public Hdf5DataSet<?> createDataSet(final String name, long[] dimensions, 
			long stringLength, Hdf5DataType type, Hdf5OverwritePolicy policy) {
		Hdf5DataSet<?> dataSet = null;
		
		if (!existsDataSet(name)) {
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, stringLength, type, true);
		} else if (policy == Hdf5OverwritePolicy.OVERWRITE) {
			// TODO Exception
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
			
			System.out.println("\nName of the new dataSet: " + newName);
			dataSet = Hdf5DataSet.getInstance(this, newName, dimensions, stringLength, type, true);
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
			group = new Hdf5Group(this, getFilePath(), name, false);
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
			long stringLength = -1;
			Hdf5DataType type = findDataSetType(name);
			
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
		        
				if (type.isString()) {
					long filetypeId = -1;
					long memtypeId = -1;
					
		    		// Get the datatype and its size.
		    		if (isOpen()) {
	    				filetypeId = H5.H5Dget_type(dataSetId);
	    			}
	    			if (filetypeId >= 0) {
	    				stringLength = H5.H5Tget_size(filetypeId);
	    				// (+1) for: Make room for null terminator
	    			}
		    		
		    		// Create the memory datatype.
		    		memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
	    			if (memtypeId >= 0) {
	    				H5.H5Tset_size(memtypeId, stringLength + 1);
	    			}
		    		
					type.getConstants()[0] = filetypeId;
					type.getConstants()[1] = memtypeId;
					
        			// Terminate access to the file and mem type.
					H5.H5Tclose(H5.H5Dget_type(dataSetId));
					H5.H5Tclose(H5.H5Tcopy(HDF5Constants.H5T_C_S1));
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
			
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, stringLength, type, false);
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
		group.setPathFromFile(getPathFromFile() + getName() + "/");
		group.setParent(this);
	}

	void addDataSet(Hdf5DataSet<?> dataSet) {
		getDataSets().add(dataSet);
		dataSet.setPathFromFile(getPathFromFile() + getName());
		dataSet.setParent(this);
	}
	
	private String[] loadObjectNames(Hdf5Object object) {
		String[] names = null;
		int index = 0;
		Hdf5Group group = this instanceof Hdf5File ? this : getParent();
		String path = this instanceof Hdf5File ? "/" : getName();
		
		// Begin iteration.
		try {
			if (group.isOpen()) {
				long count = H5.H5Gn_members(group.getElementId(), path);
				
				if (count > 0) {
					String[] oname = new String[(int) count];
					int[] otype = new int[(int) count];
					long[] orefs = new long[(int) count];
					H5.H5Gget_obj_info_all(group.getElementId(), path, oname, otype, new int[otype.length], orefs, HDF5Constants.H5_INDEX_NAME);
					
					// Get type of the object and add it to the array
					System.out.println((object == Hdf5Object.GROUP) ? "Group: " : "DataSets: ");
					names = new String[(int) count];
					for (int i = 0; i < otype.length; i++) {
						if (Hdf5Object.get(otype[i]) == object) {
							names[index] = oname[i];
							System.out.print(names[index] + ", ");
							index++;
						}
					}
					System.out.print("\nEND\n");
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("The parent "
						+ group.getName() + " of " + path + " is not open!", new IllegalStateException());
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
	
	public Hdf5DataType findDataSetType(String name) {
		if (loadDataSetNames().contains(name)) {
			long dataSetId = -1;
			String dataType = "";
			int size = 0;
			try {
				System.out.println("GroupId: " + getElementId() + ", open: " + isOpen());
				dataSetId = H5.H5Dopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				dataType = H5.H5Tget_class_name(H5.H5Tget_class(H5.H5Dget_type(dataSetId)));
				size = (int) H5.H5Tget_size(H5.H5Dget_type(dataSetId));
				H5.H5Dclose(dataSetId);
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
			System.out.println("Size: " + size);
			if (dataType.equals("H5T_INTEGER") && size == 4) {
				return Hdf5DataType.INTEGER;
			} else if (dataType.equals("H5T_INTEGER") && size == 8) {
				return Hdf5DataType.LONG;
			} else if (dataType.equals("H5T_FLOAT") && size == 8) {
				return Hdf5DataType.DOUBLE;
			} else if (dataType.equals("H5T_STRING")) {
				return Hdf5DataType.STRING;
			} else {
				NodeLogger.getLogger("HDF5 Files").error("Datatype is not supported", new IllegalArgumentException());
				return Hdf5DataType.UNKNOWN;
			}
		} else {
			NodeLogger.getLogger("HDF5 Files").error("There isn't a dataSet with this name in this group!",
					new IllegalArgumentException());
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
				NodeLogger.getLogger("HDF5 Files").info("Group " + getName() + " opened: " + getElementId());
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
	    		
	    		NodeLogger.getLogger("HDF5 Files").info("Group " + getName() + " closed: " + H5.H5Gclose(getElementId()));
		        setOpen(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

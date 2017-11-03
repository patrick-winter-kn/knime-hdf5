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
				this.open();
			} else {
				try {
					this.setElementId(H5.H5Gcreate(parent.getElementId(), this.getName(), 
							HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
							HDF5Constants.H5P_DEFAULT));
					NodeLogger.getLogger("HDF5 Files").info("Group " + this.getName() + " created: " + this.getElementId());
					parent.addGroup(this);
					this.setOpen(true);
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
		
		if (!this.existsGroup(name) || policy == Hdf5OverwritePolicy.OVERWRITE) {
			group = new Hdf5Group(this, this.getFilePath(), name, true);
		} else if (policy == Hdf5OverwritePolicy.ABORT) {
			Iterator<Hdf5Group> iter = this.getGroups().iterator();
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
				group = new Hdf5Group(this, this.getFilePath(), name, false);
			}
		} else if (policy == Hdf5OverwritePolicy.KEEP_BOTH) {
			String oldName = name;
			int i = 1;
			
			if (oldName.matches(".*\\([1-9][0-9]*\\)")) {
				oldName = oldName.substring(0, oldName.lastIndexOf("("));
				i = Integer.parseInt(oldName.substring(oldName.lastIndexOf("(") + 1, oldName.lastIndexOf(")")));
			}
			
			String newName;
			List<String> groupNames = this.loadGroupNames();
			do {
				 newName = oldName + "(" + i + ")";
				 i++;
				 System.out.print(newName + ", ");
			} while (groupNames.contains(newName));

			System.out.println("\nName of the new group: " + newName);
			group = new Hdf5Group(this, this.getFilePath(), newName, true);
		}
		
		return group;
	}

	public Hdf5DataSet<?> createDataSet(final String name,
			long[] dimensions, Hdf5DataType type, Hdf5OverwritePolicy policy) {
		Hdf5DataSet<?> dataSet = null;
		
		if (!this.existsDataSet(name)) {
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, type, true);
		} else if (policy == Hdf5OverwritePolicy.OVERWRITE) {
			
		} else if (policy == Hdf5OverwritePolicy.ABORT) {
			dataSet = this.getDataSet(name);
			
		} else if (policy == Hdf5OverwritePolicy.KEEP_BOTH) {
			String oldName = name;
			int i = 1;
			
			if (oldName.matches(".*\\([1-9][0-9]*\\)")) {
				oldName = oldName.substring(0, oldName.lastIndexOf("("));
				i = Integer.parseInt(oldName.substring(oldName.lastIndexOf("(") + 1, oldName.lastIndexOf(")")));
			}
			
			String newName;
			List<String> dataSetNames = this.loadDataSetNames();
			do {
				 newName = oldName + "(" + i + ")";
				 i++;
				 System.out.print(newName + ", ");
			} while (dataSetNames.contains(newName));
			
			System.out.println("\nName of the new dataSet: " + newName);
			dataSet = Hdf5DataSet.getInstance(this, newName, dimensions, type, true);
		}
		
		return dataSet;
	}
	
	public Hdf5DataSet<?> getDataSet(final String name) {
		Hdf5DataSet<?> dataSet = null;
		Iterator<Hdf5DataSet<?>> iter = this.getDataSets().iterator();
		boolean found = false;
		
		while (!found && iter.hasNext()) {
			Hdf5DataSet<?> ds = iter.next();
			if (ds.getName().equals(name)) {
				dataSet = ds;
				dataSet.open();
				found = true;
			}
		}
		
		if (!found && this.existsDataSet(name)) {
			long dataSetId = -1;
			long dataspaceId = -1;
			long[] dimensions = null;
			Hdf5DataType type = this.findDataSetType(name);
			
			try {
				dataSetId = H5.H5Dopen(this.getElementId(), name, HDF5Constants.H5P_DEFAULT);
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
					long stringLength = -1;
					
		    		// Get the datatype and its size.
		    		if (this.isOpen()) {
	    				filetypeId = H5.H5Dget_type(dataSetId);
	    			}
	    			if (filetypeId >= 0) {
	    				stringLength = H5.H5Tget_size(filetypeId);
	    				dims = Arrays.copyOf(dims, ndims + 1);
	    		        dims[dims.length - 1] = stringLength + 1;
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
			
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, type, false);
		}
	
		return dataSet;
	}
	
	private boolean existsGroup(final String name) {
		List<String> groupNames = this.loadGroupNames();
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
		List<String> dataSetNames = this.loadDataSetNames();
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
		this.getGroups().add(group);
		group.setPathFromFile(this.getPathFromFile() + this.getName() + "/");
		group.setParent(this);
	}

	void addDataSet(Hdf5DataSet<?> dataSet) {
		this.getDataSets().add(dataSet);
		dataSet.setPathFromFile(this.getPathFromFile() + this.getName());
		dataSet.setParent(this);
	}
	
	private String[] loadObjectNames(Hdf5Object object) {
		String[] names = null;
		int index = 0;
		Hdf5Group group = this instanceof Hdf5File ? this : this.getParent();
		String path = this instanceof Hdf5File ? "/" : this.getName();
		
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
		String[] groupNames = this.loadObjectNames(Hdf5Object.GROUP);
		if (groupNames != null) {
			Collections.addAll(names, groupNames);
		}
		return names;
	}
	
	public List<String> loadDataSetNames() {
		List<String> names = new LinkedList<>();
		String[] dataSetNames = this.loadObjectNames(Hdf5Object.DATASET);
		if (dataSetNames != null) {
			Collections.addAll(names, dataSetNames);
		}
		return names;
	}
	
	public Hdf5DataType findDataSetType(String name) {
		if (this.loadDataSetNames().contains(name)) {
			long dataSetId = -1;
			String dataType = "";
			int size = 0;
			try {
				System.out.println("GroupId: " + this.getElementId() + ", open: " + this.isOpen());
				dataSetId = H5.H5Dopen(this.getElementId(), name, HDF5Constants.H5P_DEFAULT);
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
			if (!this.isOpen()) {
				this.setElementId(H5.H5Gopen(this.getParent().getElementId(), this.getName(),
						HDF5Constants.H5P_DEFAULT));
				NodeLogger.getLogger("HDF5 Files").info("Group " + this.getName() + " opened: " + this.getElementId());
				this.setOpen(true);
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
            if (this.isOpen()) {
            	Iterator<Hdf5DataSet<?>> iterDss = this.getDataSets().iterator();
	    		while (iterDss.hasNext()) {
	    			iterDss.next().close();
	    		}

	    		Iterator<Hdf5Attribute<?>> iterAttrs = this.getAttributes().iterator();
	    		while (iterAttrs.hasNext()) {
	    			iterAttrs.next().close();
	    		}
	    		
	    		Iterator<Hdf5Group> iterGrps = this.getGroups().iterator();
	    		while (iterGrps.hasNext()) {
	    			iterGrps.next().close();
	    		}
	    		
	    		NodeLogger.getLogger("HDF5 Files").info("Group " + this.getName() + " closed: " + H5.H5Gclose(this.getElementId()));
		        this.setOpen(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

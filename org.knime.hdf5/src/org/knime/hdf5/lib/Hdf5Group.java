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
	public Hdf5Group createGroup(final String name) {
		if (!this.existsGroup(name)) {
			return new Hdf5Group(this, this.getFilePath(), name, true);
		}
		return null;
	}

	public Hdf5DataSet<?> createDataSet(final String name,
			long[] dimensions, Hdf5DataType type) {
		if (!this.existsDataSet(name)) {		
			return Hdf5DataSet.getInstance(this, name, dimensions, type, true);
		}
		return null;
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
	
	private boolean existsDataSet(final String name) {
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
	
	/**
	 * Finds the child group with the name {@code name}.
	 * If the group doesn't exist in the list {@code m_groups}, but physically
	 * exists in the .h5 file, a new instance of the group will be created
	 * and added to the list {@code m_groups}.
	 * 
	 * @param name
	 * @return child group
	 */
	public Hdf5Group getGroup(final String name) {
		Iterator<Hdf5Group> iter = this.getGroups().iterator();
		Hdf5Group group = null;
		boolean found = false;
		
		while (!found && iter.hasNext()) {
			Hdf5Group grp = iter.next();
			if (grp.getName().equals(name)) {
				group = grp;
				found = true;
			}
		}
		
		if (!found && this.existsGroup(name)) {
			group = new Hdf5Group(this, this.getFilePath(), name, false);
		}
		
		return group;
	}
	
	/**
	 * Finds the dataSet with the name {@code name}.
	 * If the dataSet doesn't exist in the list {@code m_dataSets}, but physically
	 * exists in the .h5 file, a new instance of the dataSet will be created
	 * and added to the list {@code m_dataSets}.
	 * 
	 * @param name
	 * @return dataSet
	 */
	// TODO here try to do it that the arguments only are group and name, recognize the rest
	public Hdf5DataSet<?> getDataSet(final String name, long[] dimensions, Hdf5DataType type) {
		Iterator<Hdf5DataSet<?>> iter = this.getDataSets().iterator();
		Hdf5DataSet<?> dataSet = null;
		boolean found = false;
		
		while (!found && iter.hasNext()) {
			Hdf5DataSet<?> ds = iter.next();
			if (ds.getName().equals(name)) {
				dataSet = ds;
				found = true;
			}
		}
		
		if (!found && this.existsDataSet(name)) {
			dataSet = Hdf5DataSet.getInstance(this, name, dimensions, type, false);
		}
		
		return dataSet;
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
					names = new String[(int) count];
					for (int i = 0; i < otype.length; i++) {
						if (Hdf5Object.get(otype[i]) == object) {
							names[index] = oname[i];
							index++;
						}
					}
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("The parent "
						+ group.getName() + " of " + this.getName() + " is not open!", new IllegalStateException());
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

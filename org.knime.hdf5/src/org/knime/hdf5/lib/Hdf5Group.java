package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.Hdf5DataSet.Hdf5DataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Group extends Hdf5TreeElement {
	
	protected static final List<Hdf5Group> ALL_GROUPS = new LinkedList<>();
	
	private final List<Hdf5Group> m_groups = new LinkedList<>();
	
	private final List<Hdf5DataSet<?>> m_dataSets = new LinkedList<>();

	private enum Hdf5Object {
		UNKNOWN(-1),		// Unknown object type
		GROUP(0),			// Object is a group
		DATASET(1),			// Object is a dataset
		TYPE(2),			// Object is a named data type
		LINK(3),			// Object is a symbolic link
		UDLINK(4),			// Object is a user-defined link
		RESERVED_5(5), 		// Reserved for future use
		RESERVED_6(6),		// Reserved for future use
		RESERVED_7(7);		// Reserved for future use
		private static final Map<Integer, Hdf5Object> lookup = new HashMap<>();

		static {
			for (Hdf5Object o : EnumSet.allOf(Hdf5Object.class)) {
				lookup.put(o.getType(), o);
			}
		}

		private int m_type;

		Hdf5Object(int type) {
			m_type = type;
		}

		public int getType() {
			return m_type;
		}

		public static Hdf5Object get(int type) {
			return lookup.get(type);
		}
	}
	
	protected Hdf5Group(final Hdf5Group group, final String filePath, final String name, boolean create) {
		super(name, filePath);
		if (!(this instanceof Hdf5File)) {
			ALL_GROUPS.add(this);
		}
		if (create) {
			if (group != null) {
				this.addToGroup(group);
			}
		} else {
			this.setParent(group);
			this.open();
			this.setPathFromFile(group.getPathFromFile() + this.getName() + "/");
			group.addGroup(this);
		}
	}
	
	/**
	 * Creates a new instance of a group with the name {@code name} and
	 * creates it physically in the group {@code group}. <br>
	 * The method does may create the group physically on the file system.
	 * If you do not want that, use getInstance(). <br>
	 * If in {@code group} already exists a group with the same name,
	 * the group will be opened and added to the list {@code ALL_GROUPS}. <br>
	 * If it's already in this list, the group in this list with the same
	 * name and location will be returned. <br>
	 * The name may not contain '/'. <br>
	 * 
	 * @param group the parent group
	 * @param name the name of the new group
	 * 
	 * @return the new group 
	 */
	public static Hdf5Group createInstance(final Hdf5Group group, final String name) {
		Iterator<Hdf5Group> iter = ALL_GROUPS.iterator();
		while (iter.hasNext()) {
			Hdf5Group grp = iter.next();
			if (grp.getFilePath().equals(group.getFilePath())
					&& grp.getPathFromFile().equals(group.getPathFromFile() + name + "/")) {
				grp.open();
				return grp;
			}
		}
		
		Hdf5Group[] groups = group.loadGroups();
		if (groups != null) {
			for (Hdf5Group grp: groups) {
				if (grp.getName().equals(name)) {
					return grp;
				}
			}
		}
		
		return new Hdf5Group(group, group.getFilePath(), name, true);
	}
	
	/**
	 * Creates a new instance of a group with the name {@code name} and
	 * relates it in the group {@code group}. <br>
	 * The method does not create the group physically on the file system.
	 * Use createInstance(). <br>
	 * The group will be opened and added to the list ALL_GROUPS if that
	 * did not already happen. <br>
	 * If it's already in this list, the group in this list with the same
	 * name and location will be returned. <br>
	 * The name may not contain '/'. <br>
	 * 
	 * @param group the parent group
	 * @param name the name of this group
	 * 
	 * @return 
	 */
	public static Hdf5Group getInstance(final Hdf5Group group, final String name) {
		Iterator<Hdf5Group> iter = ALL_GROUPS.iterator();
		while (iter.hasNext()) {
			Hdf5Group grp = iter.next();
			if (grp.getFilePath().equals(group.getFilePath())
					&& grp.getPathFromFile().equals(group.getPathFromFile() + name + "/")) {
				grp.open();
				return grp;
			}
		}
		return new Hdf5Group(group, group.getFilePath(), name, false);
	}
	
	/**
	 * @return a list of all direct subGroups which were either already loaded by loadGroups() or
	 * created in the same run.
	 */
	public List<Hdf5Group> getGroups() {
		return m_groups;
	}

	/**
	 * @return a list of all datasets which were either already loaded by loadDataSets() or
	 * created in the same run.
	 */
	public List<Hdf5DataSet<?>> getDataSets() {
		return m_dataSets;
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
	
	public String[] getGroupNames() {
		List<String> names = new LinkedList<>();
		names.add(" ");
		Collections.addAll(names, this.loadObjectNames(Hdf5Object.GROUP));
		return names.toArray(new String[0]);
	}
	
	/**
	 * Loads all direct subGroups which are accessible from this group. <br>
	 * All subGroups will also be opened and the list {@code groups} will be updated.
	 * 
	 * @return the array of all accessible subGroups
	 */
	public Hdf5Group[] loadGroups() {
		String[] names = this.loadObjectNames(Hdf5Object.GROUP);
		if (names != null) {
			Hdf5Group[] groups = new Hdf5Group[names.length];
			for (int i = 0; i < names.length; i++) {
				// the object list groups will be updated in here
				groups[i] = Hdf5Group.getInstance(this, names[i]);
			}
			return groups;
		}
		return null;
	}
	
	/**
	 * Find the subGroup with the name {@code name} using the returned list of the method getGroups(). <br>
	 * The list was updated at the latest call of loadGroups().
	 * 
	 * @param name
	 * @return
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
		
		return group;
	}
	
	/**
	 * Adds the group to the list if it is not already there. You might have to call the method
	 * loadGroup() to update the list which is used in this method.
	 * 
	 * @param dataSet
	 */
	public void addGroup(Hdf5Group group) {
		if (this.getGroup(group.getName()) == null) {
			this.getGroups().add(group);
		}
	}
	
	public String[] getDataSetNames() {
		List<String> names = new LinkedList<>();
		names.add(" ");
		Collections.addAll(names, this.loadObjectNames(Hdf5Object.DATASET));
		return names.toArray(new String[0]);
	}
	
	/**
	 * Loads all datasets which are accessible from this group with the respective type. <br>
	 * All datasets will also be opened and the list {@code dataSets} will be updated.
	 * 
	 * @return the array of all accessible datasets
	 */
	public Hdf5DataSet<?>[] loadDataSets(Hdf5DataType type) {
		String[] names = this.loadObjectNames(Hdf5Object.DATASET);
		if (names != null) {
			Hdf5DataSet<?>[] dataSets = new Hdf5DataSet[names.length];
			for (int i = 0; i < names.length; i++) {
				dataSets[i] = Hdf5DataSet.getInstance(this, names[i], null, type);
			}
			return dataSets;
		}
		return null;
	}
	
	/**
	 * Loads all datasets which are accessible from this group. <br>
	 * All datasets will also be opened and the list {@code dataSets} will be updated.
	 * 
	 * @return the array of all accessible datasets
	 */
	public Hdf5DataSet<?>[] loadDataSets() {
		List<Hdf5DataSet<?>> dataSets = new LinkedList<>();
		Collections.addAll(dataSets, this.loadDataSets(Hdf5DataType.INTEGER));
		Collections.addAll(dataSets, this.loadDataSets(Hdf5DataType.LONG));
		Collections.addAll(dataSets, this.loadDataSets(Hdf5DataType.DOUBLE));
		Collections.addAll(dataSets, this.loadDataSets(Hdf5DataType.STRING));
		return (Hdf5DataSet<?>[]) dataSets.toArray();
	}
	
	/**
	 * Find the dataset with the name {@code name} using the returned list of the method getDataSets(). <br>
	 * The list was updated at the latest call of loadDataSets().
	 * 
	 * @param name
	 * @return
	 */
	public Hdf5DataSet<?> getDataSet(final String name) {
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
		
		return dataSet;
	}
	
	/**
	 * Adds the dataset to the list if it is not already there. You might have to call the method
	 * loadDataSets() to update the list which is used in this method.
	 * 
	 * @param dataSet
	 */
	public void addDataSet(Hdf5DataSet<?> dataSet) {
		if (this.getDataSet(dataSet.getName()) == null) {
			this.getDataSets().add(dataSet);
		}
	}
	
	/**
	 * Adds this (sub)group to a group which has already been added to another group
	 * or is a file. <br>
	 * If there is already another (sub)group with the same name in the group,
	 * the (sub)group will just be opened.
	 * 
	 * @param group the destination group
	 * @return {@code true} if the (sub)group is successfully added to the group
	 */
	private boolean addToGroup(Hdf5Group group) {
		if (group.isOpen()) {
			try {
				this.setElementId(H5.H5Gcreate(group.getElementId(), this.getName(), 
						HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
						HDF5Constants.H5P_DEFAULT));
				NodeLogger.getLogger("HDF5 Files").info("Group " + this.getName() + " created: " + this.getElementId());
				this.setPathFromFile(group.getPathFromFile() + this.getName() + "/");
				this.setParent(group);
				group.addGroup(this);
				this.setOpen(true);
				return true;
			} catch (HDF5LibraryException | NullPointerException e) {
				e.printStackTrace();
			}
		} else {
			NodeLogger.getLogger("HDF5 Files").error("This group " + this.getName() + " is not open!",
					new IllegalStateException());
			System.err.println();
		}
		return false;
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
	
	public void close() {
        try {
        	if (this.isOpen()) {
				NodeLogger.getLogger("HDF5 Files").info("Group " + this.getName() + " closed: " + H5.H5Gclose(this.getElementId()));
		        this.setOpen(false);
        	}
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
	public void closeAll() {
		try {
            if (this.isOpen()) {
            	Iterator<Hdf5DataSet<?>> iterDS = this.getDataSets().iterator();
	    		while (iterDS.hasNext()) {
	    			iterDS.next().close();
	    		}

            	this.closeAttributes();
	    		
	    		Iterator<Hdf5Group> iterG = this.getGroups().iterator();
	    		while (iterG.hasNext()) {
	    			iterG.next().closeAll();
	    		}
	    		
	    		this.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

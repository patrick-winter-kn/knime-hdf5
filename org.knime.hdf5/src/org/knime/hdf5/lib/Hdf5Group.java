package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Group extends Hdf5TreeElement {
	
	private final List<Hdf5Group> groups = new LinkedList<>();
	private final List<Hdf5DataSet<?>> dataSets = new LinkedList<>();

	enum Hdf5Object {
		UNKNOWN(-1),		// Unknown object type
		GROUP(0),			// Object is a group
		DATASET(1),			// Object is a dataset
		TYPE(2),			// Object is a named data type
		LINK(3),			// Object is a symbolic link
		UDLINK(4),			// Object is a user-defined link
		RESERVED_5(5), 		// Reserved for future use
		RESERVED_6(6),		// Reserved for future use
		RESERVED_7(7);		// Reserved for future use
		private static final Map<Integer, Hdf5Object> lookup = new HashMap<Integer, Hdf5Object>();

		static {
			for (Hdf5Object o : EnumSet.allOf(Hdf5Object.class)) {
				lookup.put(o.getType(), o);
			}
		}

		private int type;

		Hdf5Object(int type) {
			this.type = type;
		}

		public int getType() {
			return this.type;
		}

		public static Hdf5Object get(int type) {
			return lookup.get(type);
		}
	}
	
	/**
	 * Creates a group with the name {@code name}. <br>
	 * The name may not contain '/'. <br>
	 * The group has not been added to another group yet. Use the method addToGroup() for that. 
	 * 
	 * @param name
	 */
	
	public Hdf5Group(final String name) {
		super(name);
	}

	/**
	 * @return a list of all direct subGroups which were either already loaded by loadGroups() or
	 * created in the same run.
	 */
	
	public List<Hdf5Group> getGroups() {
		return groups;
	}

	/**
	 * @return a list of all datasets which were either already loaded by loadDataSets() or
	 * created in the same run.
	 */
	
	public List<Hdf5DataSet<?>> getDataSets() {
		return dataSets;
	}
	
	/*
	public static Hdf5Group addToFile(Hdf5File file, String path) {
		Hdf5Group group = null; 
		if (file.getElement_id() >= 0) {
			if (!path.equals("")) {
		    	group = Hdf5Group.getLastGroup(path);
		    	group.addGroupToFile(file, Hdf5Group.getGroupPath(path));
	    	}
    	}
    	return group;
	}
	
	public static Hdf5Group getLastGroup(String path) {
		int lastIndex = path.lastIndexOf("/");
		String name = (lastIndex != -1) ? path.substring(lastIndex + 1) : path;
		return !path.equals("") ? new Hdf5Group(name) : null;
	}
	
	public static String getGroupPath(String path) {
		int lastIndex = path.lastIndexOf("/");
		return (lastIndex != -1) ? path.substring(0, lastIndex) : "";
	}
	*/
	
	private String[] loadObjectNames(Hdf5Object object) {
		String[] names = null;
		int index = 0;
		
		// Begin iteration.
		//System.out.println("\nObjects in group " + this.getName() + " with ID " + this.getElement_id() + " in path " + this.getPathFromFile() + ":");
		try {
			if (this.getElement_id() >= 0) {
				Hdf5Group group = this instanceof Hdf5File ? this : this.getGroupAbove();
				String path = this instanceof Hdf5File ? "/" : this.getName();
				
				long count = H5.H5Gn_members(group.getElement_id(), path);
				String[] oname = new String[(int) count];
				int[] otype = new int[(int) count];
				long[] orefs = new long[(int) count];
				H5.H5Gget_obj_info_all(group.getElement_id(), path, oname, otype, new int[otype.length], orefs, HDF5Constants.H5_INDEX_NAME);
				//System.out.println("Success: " + H5.H5Gget_obj_info_all(group.getElement_id(), path, oname, otype, new int[otype.length], orefs, HDF5Constants.H5_INDEX_NAME) + " / " + count);
				
				names = new String[(int) count];
				
				// Get type of the object and add it to the array
				for (int i = 0; i < otype.length; i++) {
					if (Hdf5Object.get(otype[i]) == object) {
						names[index] = oname[i];
						index++;
						//System.out.println("Found " + object.name() + ": " + oname[i]);
					}
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return names != null ? Arrays.copyOf(names, index) : null;
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
			Hdf5Group group;
			this.getGroups().clear();
			for (int i = 0; i < names.length; i++) {
				group = new Hdf5Group(names[i]);
				group.setPathFromFile(this.getPathFromFile() + group.getName() + "/");
				group.setGroupAbove(this);
				try {
					group.setElement_id(H5.H5Gopen(this.getElement_id(), group.getName(), HDF5Constants.H5P_DEFAULT));
				} catch (HDF5LibraryException | NullPointerException e) {
					e.printStackTrace();
				}
				groups[i] = group;
				this.addGroup(group);
			}
			return groups;
		}
		return null;
	}
	
	public Hdf5Group getGroup(final String name) {
		for (Hdf5Group group: this.loadGroups()) {
			if (group.getName().equals(name)) {
				return group;
			}
		}
		return null;
	}
	
	public void addGroup(Hdf5Group group) {
		if (this.getGroup(group.getName()) == null) {
			this.getGroups().add(group);
		}
	}
	
	/**
	 * Loads all datasets which are accessible from this group. <br>
	 * All datasets will also be opened and the list {@code dataSets} will be updated.
	 * 
	 * @return the array of all accessible datasets
	 */
	
	public Hdf5DataSet<Byte>[] loadDataSets() {
		String[] names = this.loadObjectNames(Hdf5Object.DATASET);
		if (names != null) {
			Hdf5DataSet<Byte>[] dataSets = new Hdf5DataSet[names.length];
			Hdf5DataSet<Byte> dataSet;
			this.getDataSets().clear();
			long[] dims2D = { 2, 2, 2, 7+1 };
			for (int i = 0; i < names.length; i++) {
				// in case it is String
				dataSet = new Hdf5DataSet<Byte>(names[i], dims2D, new Byte((byte) 0));
				dataSet.setPathFromFile(this.getPathFromFile() + dataSet.getName());
				dataSet.setGroupAbove(this);
				try {
					dataSet.setElement_id(H5.H5Dopen(this.getElement_id(), dataSet.getName(),
	                		HDF5Constants.H5P_DEFAULT));
					dataSet.updateDimensions();
				} catch (HDF5LibraryException | NullPointerException e) {
					e.printStackTrace();
				}
				dataSets[i] = dataSet;
				this.addDataSet(dataSet);
			}
			return dataSets;
		}
		return null;
	}
	
	public Hdf5DataSet<?> getDataSet(final String name) {
		for (Hdf5DataSet<?> dataSet: this.loadDataSets()) {
			if (dataSet.getName().equals(name)) {
				return dataSet;
			}
		}
		return null;
	}
	
	public void addDataSet(Hdf5DataSet<?> dataSet) {
		if (this.getDataSet(dataSet.getName()) == null) {
			this.getDataSets().add(dataSet);
		}
	}
	
	public boolean addToGroup(Hdf5Group group) {
		if (this instanceof Hdf5File) {
			System.err.println("Cannot add file to group!");
		} else {
			if (!group.getPathFromFile().contains("/")) {
				System.err.println("Cannot add group to a group which is not added to a group!");
			} else {
				if (group.getElement_id() >= 0 && group.getGroup(this.getName()) == null) {
					try {
						this.setElement_id(H5.H5Gcreate(group.getElement_id(), this.getName(), 
								HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
								HDF5Constants.H5P_DEFAULT));
						this.setPathFromFile(group.getPathFromFile() + this.getName() + "/");
						this.setGroupAbove(group);
						group.addGroup(this);
						return true;
					} catch (HDF5LibraryException | NullPointerException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return false;
		
		/*
		if (!(this instanceof Hdf5File)) {
			if (!(group instanceof Hdf5File)) {
				String path = group.getPathFromFile();
				if (!path.equals(null) && file.getElement_id() >= 0 && group.getElement_id() >= 0) {
		    		try {
						this.setElement_id(H5.H5Gopen(group.getElement_id(), this.getName(), 
								HDF5Constants.H5P_DEFAULT));
						//group.addGroup(this);
						this.setPathFromFile(path);
					} catch (HDF5LibraryException | NullPointerException e) {
						try {
							this.setElement_id(H5.H5Gcreate(group.getElement_id(), this.getName(), 
									HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
									HDF5Constants.H5P_DEFAULT));
							//group.addGroup(this);
							this.setPathFromFile(path);
						} catch (HDF5LibraryException | NullPointerException e2) {
							e2.printStackTrace();
						}
					} 
				}
			} else {
				this.addGroupToFile(file, "");
			}
		} else {
			System.err.println("Cannot add file to file!");
		}
		*/
	}
	
	/*
	private void addGroupToFile(Hdf5File file, String path) {
		if (!(this instanceof Hdf5File)) {
			if (file.getElement_id() >= 0) {
				Hdf5Group group = file;
				if (!path.equals("")) {
					group = Hdf5Group.getLastGroup(path);
			    	group.addGroupToFile(file, Hdf5Group.getGroupPath(path));             
				} 
				if (group.getElement_id() >= 0) {
		    		try {
						this.setElement_id(H5.H5Gopen(group.getElement_id(), this.getName(), 
								HDF5Constants.H5P_DEFAULT));
						//group.addGroup(this);
						this.setPathFromFile(path);
					} catch (HDF5LibraryException | NullPointerException e) {
						try {
							this.setElement_id(H5.H5Gcreate(group.getElement_id(), this.getName(), 
									HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
									HDF5Constants.H5P_DEFAULT));
							//group.addGroup(this);
							this.setPathFromFile(path);
						} catch (HDF5LibraryException | NullPointerException e2) {
							e.printStackTrace();
						}
					}
				} 
	        }
		} else {
			System.err.println("Cannot add file to file!");
		}
	}
	*/
	
	/**
	 * Closes the group and all elements in this group.
	 * 
	 */
	
	public void close() {
		try {
            if (this.getElement_id() >= 0) {
            	Iterator<Hdf5DataSet<?>> iterDS = this.getDataSets().iterator();
	    		while (iterDS.hasNext()) {
	    			Hdf5DataSet<?> dataSet = iterDS.next();
	    			if (dataSet.getElement_id() >= 0) {
	    				dataSet.close();
	    			}
	    		}

            	Hdf5Attribute<?>[] attributes = this.listAttributes();
	    		if (attributes != null) {
	    			for (Hdf5Attribute<?> attribute: attributes) {
	    				if (attribute.getAttribute_id() >= 0) {
	    					attribute.close();
	    				}
	    			}
	    		}
	    		
	    		Iterator<Hdf5Group> iterG = this.getGroups().iterator();
	    		while (iterG.hasNext()) {
	    			Hdf5Group group = iterG.next();
	    			if (group.getElement_id() >= 0) {
	    				group.close();
	    			}
	    		}
	    		
                System.out.println("Group " + this.getName() + " closed: " + H5.H5Gclose(this.getElement_id()));
                this.setElement_id(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

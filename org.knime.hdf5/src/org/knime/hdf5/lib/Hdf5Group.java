package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5Group extends Hdf5TreeElement {
	
	private final List<Hdf5Group> groups = new LinkedList<>();
	private final List<Hdf5DataSet<?>> dataSets = new LinkedList<>();
	private String pathFromFile = null;
	
	public Hdf5Group(final String name) {
		super(name);
	}
	
	/**
	 * 
	 * @param file
	 * @param path
	 * @return null if the file_id = -1
	 */
	
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
	
	public String getName() {
		return name;
	}
	
	public List<Hdf5Group> getGroups() {
		return groups;
	}

	public List<Hdf5DataSet<?>> getDataSets() {
		return dataSets;
	}

	public String getPathFromFile() {
		return pathFromFile;
	}

	public void setPathFromFile(String pathFromFile) {
		this.pathFromFile = pathFromFile;
	}

	public Hdf5Group[] listGroups() {
		Hdf5Group[] groups = new Hdf5Group[this.getGroups().size()];
		Iterator<Hdf5Group> iter = this.getGroups().iterator();
		int i = 0;
		while (iter.hasNext()) {
			groups[i] = iter.next();
			i++;
		}
		return groups;
	}
	
	public Hdf5Group getGroup(final String name) {
		for (Hdf5Group group: this.listGroups()) {
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
	
	public Hdf5DataSet<?>[] listDataSets() {
		Hdf5DataSet<?>[] dataSets = new Hdf5DataSet<?>[this.getDataSets().size()];
		Iterator<Hdf5DataSet<?>> iter = this.getDataSets().iterator();
		int i = 0;
		while (iter.hasNext()) {
			dataSets[i] = iter.next();
			i++;
		}
		return dataSets;
	}
	
	public Hdf5DataSet<?> getDataSet(final String name) {
		for (Hdf5DataSet<?> dataSet: this.listDataSets()) {
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
	
	public void addToGroupInFile(Hdf5Group group, Hdf5File file) {
		if (!(this instanceof Hdf5File)) {
			if (!(group instanceof Hdf5File)) {
				String path = group.getPathFromFile();
				if (!path.equals(null) && file.getElement_id() >= 0 && group.getElement_id() >= 0) {
		    		try {
						this.setElement_id(H5.H5Gopen(group.getElement_id(), this.getName(), 
								HDF5Constants.H5P_DEFAULT));
						group.addGroup(this);
						this.setPathFromFile(path);
					} catch (HDF5LibraryException | NullPointerException e) {
						try {
							this.setElement_id(H5.H5Gcreate(group.getElement_id(), this.getName(), 
									HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
									HDF5Constants.H5P_DEFAULT));
							group.addGroup(this);
							this.setPathFromFile(path);
						} catch (HDF5LibraryException | NullPointerException e2) {
							e.printStackTrace();
						}
					} 
				}
			} else {
				this.addGroupToFile(file, "");
			}
		} else {
			System.err.println("Cannot add file to file!");
		}
	}
	
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
						group.addGroup(this);
						this.setPathFromFile(path);
					} catch (HDF5LibraryException | NullPointerException e) {
						try {
							this.setElement_id(H5.H5Gcreate(group.getElement_id(), this.getName(), 
									HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
									HDF5Constants.H5P_DEFAULT));
							group.addGroup(this);
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
	
	public void close() {
		try {
            if (this.getElement_id() >= 0) {
                H5.H5Gclose(this.getElement_id());
                this.setElement_id(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void closeAllBelow() {
		Iterator<Hdf5Group> iter = this.getGroups().iterator();
		while(iter.hasNext()) {
			Hdf5Group group = iter.next();
			group.closeAllBelow();
			group.close();
		}
	}
}

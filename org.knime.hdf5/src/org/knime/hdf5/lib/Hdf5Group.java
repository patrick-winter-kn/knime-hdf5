package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

//TODO
public class Hdf5Group extends Hdf5TreeElement {
	
	private final String name;
	private final List<Hdf5Group> groups = new LinkedList<>();
	private final List<Hdf5DataSet<?>> dataSets = new LinkedList<>();
	private long group_id = -1;
	
	/**
	 * 
	 * @param file
	 * @param path
	 * @return null if the file_id = -1
	 */
	
	public Hdf5Group(final String name) {
		this.name = name;
	}
	
	public static Hdf5Group addToFile(Hdf5File file, String path) {
		Hdf5Group group = null; 
		if (file.getFile_id() >= 0) {
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

	public long getGroup_id() {
		return group_id;
	}

	public void setGroup_id(long group_id) {
		this.group_id = group_id;
	}

	// TODO this method is similar to Hdf5TreeElement.listAttributes(), maybe put it together
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
		Iterator<Hdf5Group> iter = this.getGroups().iterator();
		while (iter.hasNext()) {
			Hdf5Group group = iter.next();
			if (group.getName() == name) {
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
		Iterator<Hdf5DataSet<?>> iter = this.getDataSets().iterator();
		while (iter.hasNext()) {
			Hdf5DataSet<?> dataSet = iter.next();
			if (dataSet.getName() == name) {
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
	
	// TODO test that our group isn't a file, because a file shouldn't add files
	private void addGroupToFile(Hdf5File file, String path) {
		if (file.getFile_id() >= 0) {
			Hdf5Group group = file;
			if (!path.equals("")) {
				group = Hdf5Group.getLastGroup(path);
		    	group.addGroupToFile(file, Hdf5Group.getGroupPath(path));             
			} 
			group.addGroup(this);
			if (group.getGroup_id() >= 0) {
	    		try {
					this.setGroup_id(H5.H5Gopen(group.getGroup_id(), this.getName(), 
							HDF5Constants.H5P_DEFAULT));
				} catch (HDF5LibraryException | NullPointerException e) {
					try {
						this.setGroup_id(H5.H5Gcreate(group.getGroup_id(), this.getName(), 
								HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT,
								HDF5Constants.H5P_DEFAULT));
					} catch (HDF5LibraryException | NullPointerException e2) {
						e.printStackTrace();
					}
				}
			} 
        }
	}
	
	public void close() {
		try {
            if (this.getGroup_id() >= 0) {
                H5.H5Gclose(this.getGroup_id());
                this.setGroup_id(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void closeAll() {
		Iterator<Hdf5Group> iter = this.getGroups().iterator();
		while(iter.hasNext()) {
			Hdf5Group group = iter.next();
			group.closeAll();
			group.close();
		}
		this.close();
	}
}

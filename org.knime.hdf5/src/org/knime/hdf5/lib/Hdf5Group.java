package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

//TODO
// hier fehlt noch der Teil mit dem Filehandling
public class Hdf5Group extends Hdf5TreeElement {
	
	private final String name;
	private final List<Hdf5Group> groups = new LinkedList<>();
	private final List<Hdf5DataSet<?>> dataSets = new LinkedList<>();
	
	public Hdf5Group(final String name) {
		this.name = name;
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

	// da es der Methode Hdf5TreeElement.listAttributes() aehnelt, evtl. mgl., beide zu einer zusammenzufassen
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
}

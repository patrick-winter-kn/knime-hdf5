package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

abstract public class Hdf5TreeElement {

	private final String name;
	private final List<Hdf5Attribute<?>> attributes = new LinkedList<>();
	private long element_id = -1;
	private boolean opened;
	private final String filePath;
	private String pathFromFile = "";
	private Hdf5Group groupAbove;
	
	/**
	 * Creates a treeElement with the name {@code name}. <br>
	 * The name may not contain '/'.
	 * 
	 * @param name
	 */
	
	public Hdf5TreeElement(final String name, final String filePath) {
		if (name.contains("/")) {
			System.err.println("Name " + name + " contains '/'");
			this.name = null;
		} else {
			this.name = name;
		}
		this.filePath = filePath;
	}
	
	public String getName() {
		return name;
	}

	private List<Hdf5Attribute<?>> getAttributes() {
		return attributes;
	}

	public long getElement_id() {
		return element_id;
	}

	public void setElement_id(long element_id) {
		this.element_id = element_id;
	}

	public boolean isOpened() {
		return opened;
	}

	public void setOpened(boolean opened) {
		this.opened = opened;
	}

	public String getFilePath() {
		return filePath;
	}

	public String getPathFromFile() {
		return pathFromFile;
	}

	public void setPathFromFile(String pathFromFile) {
		this.pathFromFile = pathFromFile;
	}

	public Hdf5Group getGroupAbove() {
		return groupAbove;
	}

	public void setGroupAbove(Hdf5Group groupAbove) {
		this.groupAbove = groupAbove;
	}
	
	public Hdf5Attribute<?>[] listAttributes() {
		Hdf5Attribute<?>[] attributes = new Hdf5Attribute<?>[this.getAttributes().size()];
		Iterator<Hdf5Attribute<?>> iter = this.getAttributes().iterator();
		int i = 0;
		while (iter.hasNext()) {
			attributes[i] = iter.next();
			i++;
		}
		return attributes;
	}

	public Hdf5Attribute<?> getAttribute(final String name) {
		for (Hdf5Attribute<?> attr: this.listAttributes()) {
			if (attr.getName().equals(name)) {
				return attr;
			}
		}
		return null;
	}

	public void addAttribute(Hdf5Attribute<?> attribute) {
		if (this.getAttribute(attribute.getName()) == null) {
			this.getAttributes().add(attribute);
		}
	}
	
	public void closeAttributes() {
		Hdf5Attribute<?>[] attributes = this.listAttributes();
		if (attributes != null) {
			for (Hdf5Attribute<?> attribute: attributes) {
				attribute.close();
			}
		}
	}
}

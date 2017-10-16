package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.node.NodeLogger;

abstract public class Hdf5TreeElement {

	private final String m_name;
	
	private final String m_filePath;
	
	private final List<Hdf5Attribute<?>> m_attributes = new LinkedList<>();
	
	private long m_elementId = -1;
	
	private boolean m_open;
	
	private String m_pathFromFile = "";
	
	// TODO maybe not necessary anymore
	private Hdf5Group m_parent;
	
	/**
	 * Creates a treeElement with the name {@code name}. <br>
	 * The name may not contain '/'.
	 * 
	 * @param name
	 */
	protected Hdf5TreeElement(final String name, final String filePath) {
		if (name.equals("")) {
			NodeLogger.getLogger("HDF5 Files").error("Name of " + filePath +  " may not be the empty String!" ,
					new IllegalArgumentException());
			// this is necessary because m_name is final
			m_name = null;
		} else if (name.contains("/")) {
			NodeLogger.getLogger("HDF5 Files").error("Name " + name + " contains '/'",
					new IllegalArgumentException());
			m_name = null;
		} else {
			m_name = name;
		}
		m_filePath = filePath;
	}
	
	public String getName() {
		return m_name;
	}

	protected String getFilePath() {
		return m_filePath;
	}

	protected List<Hdf5Attribute<?>> getAttributes() {
		return m_attributes;
	}

	protected long getElementId() {
		return m_elementId;
	}

	protected void setElementId(long elementId) {
		m_elementId = elementId;
	}

	protected boolean isOpen() {
		return m_open;
	}

	protected void setOpen(boolean open) {
		m_open = open;
	}

	public String getPathFromFile() {
		return m_pathFromFile;
	}

	protected void setPathFromFile(String pathFromFile) {
		m_pathFromFile = pathFromFile;
	}

	protected Hdf5Group getParent() {
		return m_parent;
	}

	protected void setParent(Hdf5Group parent) {
		m_parent = parent;
	}

	public Hdf5Attribute<?> getAttribute(final String name) {
		Iterator<Hdf5Attribute<?>> iter = this.getAttributes().iterator();
		Hdf5Attribute<?> attribute = null;
		boolean found = false;
		
		while (!found && iter.hasNext()) {
			Hdf5Attribute<?> attr = iter.next();
			if (attr.getName().equals(name)) {
				attribute = attr;
				found = true;
			}
		}
		
		return attribute;
	}

	public void addAttribute(Hdf5Attribute<?> attribute) {
		if (this.getAttribute(attribute.getName()) == null) {
			this.getAttributes().add(attribute);
		}
	}
	
	public void closeAttributes() {
		Iterator<Hdf5Attribute<?>> iter = this.getAttributes().iterator();
		while (iter.hasNext()) {
			iter.next().close();
		}
	}
}

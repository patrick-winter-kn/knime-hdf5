package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.node.NodeLogger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

abstract public class Hdf5TreeElement {

	private final String m_name;
	
	private final String m_filePath;
	
	private final List<Hdf5Attribute<?>> m_attributes = new LinkedList<>();
	
	private long m_elementId = -1;
	
	private boolean m_open;
	
	private String m_pathFromFile = "";
	
	private Hdf5Group m_parent;
	
	/**
	 * Creates a treeElement with the name {@code name}. <br>
	 * The name may not contain '/'.
	 * 
	 * @param name
	 */
	protected Hdf5TreeElement(final String name, final String filePath) {
		if (name == null) {
			NodeLogger.getLogger("HDF5 Files").error("name is null",
					new NullPointerException());
			// this is necessary because m_name is final
			m_name = null;
		} else if (name.equals("")) {
			NodeLogger.getLogger("HDF5 Files").error("name may not be the empty String!",
					new IllegalArgumentException());
			m_name = null;
		} else if (name.contains("/")) {
			NodeLogger.getLogger("HDF5 Files").error("name " + name + " contains '/'",
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

	public String getFilePath() {
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

	public Hdf5Group getParent() {
		return m_parent;
	}

	protected void setParent(Hdf5Group parent) {
		m_parent = parent;
	}
	
	public String getPathWithoutFileName() {
		if (this instanceof Hdf5File) {
			return "";
		}
		
		final String ext = ".h5/";
		String path = getPathFromFile() + (this instanceof Hdf5DataSet<?> ? "/" : "") + getName() + (this instanceof Hdf5Group ? "/" : "");
		
		if (path.contains(ext)) {
			path = path.substring(path.indexOf(ext) + ext.length());
		}
		
		return path;
	}

	public Hdf5Attribute<?> getAttribute(final String name) {
		Iterator<Hdf5Attribute<?>> iter = getAttributes().iterator();
		Hdf5Attribute<?> attribute = null;
		boolean found = false;
		
		while (!found && iter.hasNext()) {
			Hdf5Attribute<?> attr = iter.next();
			if (attr.getName().equals(name)) {
				attribute = attr;
				found = true;
			}
		}
		
		if (!found && existsAttribute(name)) {
			attribute = Hdf5Attribute.getInstance(this, name);
		}
		
		return attribute;
	}
	
	public boolean existsAttribute(final String name) {
		List<String> attrNames = loadAttributeNames();
		if (attrNames != null) {
			for (String n: attrNames) {
				if (n.equals(name)) {
					return true;
				}
			}
		}
		return false;
	}
	
	// TODO it maybe makes more sense if it's only possible for dataSets
	// if it's also possible for parent groups, it should be done recursively (add the attributes from parent groups to dataSets)
	public List<String> loadAttributeNames() {
		List<String> attrNames = new LinkedList<>();
		long numAttrs = -1;
		
		try {
			numAttrs = H5.H5Oget_info(getElementId()).num_attrs;
			for (int i = 0; i < numAttrs; i++) {
				attrNames.add(H5.H5Aget_name_by_idx((this instanceof Hdf5File ? this : getParent()).getElementId(),
						getName(),HDF5Constants.H5_INDEX_NAME,
						HDF5Constants.H5_ITER_INC, i, HDF5Constants.H5P_DEFAULT));
			}
		} catch (HDF5LibraryException | NullPointerException lnpe) {
			lnpe.printStackTrace();
		}
		
		return attrNames;
	}
	
	Hdf5DataType findAttributeType(String name) {
		if (loadAttributeNames().contains(name)) {
			long attributeId = -1;
			String dataType = "";
			int size = 0;
			
			try {
				attributeId = H5.H5Aopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				long filetypeId = H5.H5Aget_type(attributeId);
				dataType = H5.H5Tget_class_name(H5.H5Tget_class(filetypeId));
				size = (int) H5.H5Tget_size(filetypeId);
				H5.H5Tclose(filetypeId);
				H5.H5Aclose(attributeId);
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
			
			return Hdf5DataType.getInstance(dataType, size, false);
		} else {
			NodeLogger.getLogger("HDF5 Files").error("There isn't an attribute with this name in this treeElement!",
					new IllegalArgumentException());
			return null;
		}
	}
	
	public void addAttribute(Hdf5Attribute<?> attribute) {
		try {
            if (getElementId() >= 0) {
            	attribute.setAttributeId(H5.H5Aopen(getElementId(), attribute.getName(), HDF5Constants.H5P_DEFAULT));
                NodeLogger.getLogger("HDF5 Files").info("Attribute " + attribute.getName() + " opened: " + attribute.getAttributeId());
        		getAttributes().add(attribute);
        		attribute.updateDimension();
            	attribute.setOpen(true);
            }
        } catch (HDF5LibraryException le) {
        	if (attribute.getType().isString()) {
				long filetypeId = -1;
				long memtypeId = -1;
				
	        	// Create file and memory datatypes. For this example we will save
	    		// the strings as FORTRAN strings, therefore they do not need space
	    		// for the null terminator in the file.
	    		try {
	    			filetypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1);
	    			if (filetypeId >= 0) {
	    				H5.H5Tset_size(filetypeId, attribute.getDimension() - 1);
	    			}
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    		}
	    		
	    		try {
	    			memtypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
	    			if (memtypeId >= 0) {
	    				H5.H5Tset_size(memtypeId, attribute.getDimension());
	    			}
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    		}
	    		
	    		attribute.getType().getConstants()[0] = filetypeId;
	    		attribute.getType().getConstants()[1] = memtypeId;
        	}
        	
        	// Create the data space for the attribute.
        	// the array length can only be 1 for Strings
        	long[] dims = { attribute.getType().isString() ? 1 : attribute.getDimension() };
            try {
            	attribute.setDataspaceId(H5.H5Screate_simple(1, dims, null));
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Create a dataset attribute.
            try {
                if (getElementId() >= 0 && attribute.getDataspaceId() >= 0 && attribute.getType().getConstants()[0] >= 0) {
                	attribute.setAttributeId(H5.H5Acreate(getElementId(), attribute.getName(),
                    		attribute.getType().getConstants()[0], attribute.getDataspaceId(),
                            HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
                    NodeLogger.getLogger("HDF5 Files").info("Attribute " + attribute.getName() + " created: " + attribute.getAttributeId());
                	attribute.setOpen(true);
                    
                    if (attribute.getAttributeId() >= 0 && attribute.getType().getConstants()[1] >= 0) {
                        H5.H5Awrite(attribute.getAttributeId(), attribute.getType().getConstants()[1], attribute.getValue());
                		getAttributes().add(attribute);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
	}
}

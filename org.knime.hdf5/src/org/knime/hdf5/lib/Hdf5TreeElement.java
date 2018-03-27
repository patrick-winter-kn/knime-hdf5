package org.knime.hdf5.lib;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

abstract public class Hdf5TreeElement {

	private final String m_name;
	
	private final String m_filePath;
	
	private final List<Hdf5Attribute<?>> m_attributes = new ArrayList<>();
	
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
	protected Hdf5TreeElement(final String name, final String filePath)
			throws NullPointerException, IllegalArgumentException {
		if (name == null) {
			throw new NullPointerException("name cannot be null");
			
		} else if (name.equals("")) {
			throw new IllegalArgumentException("name cannot be the Empty String");
			
		} else if (name.contains("/")) {
			throw new IllegalArgumentException("name \"" + name + "\" cannot contain '/'");
		}
		
		m_name = name;
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
		if (this instanceof Hdf5File) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		return m_open;
	}

	protected void setOpen(boolean open) {
		if (this instanceof Hdf5File) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
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

	/**
	 * 
	 * @param endSlash (only relevant if treeElement is an {@code Hdf5File})
	 * set to {@code true} if an {@code '/'} should be added to the path. <br>
	 * If the treeElement is not an {@code Hdf5File}, the {@code '/'} will always be
	 * added (even if {@code endSlash == false}).
	 * @return
	 */
	public String getPathFromFileWithName(boolean endSlash) {
		return this instanceof Hdf5File ? (endSlash ? "/" : "") : getPathFromFile() + getName() + "/";
	}
	
	public String getPathFromFileWithName() {
		return getPathFromFileWithName(true);
	}
	
	public void createAttribute(Hdf5Attribute<?> attribute) {
		try {
        	// Create the data space for the attribute.
        	long[] dims = { attribute.getDimension() };
        	attribute.setDataspaceId(H5.H5Screate_simple(1, dims, null));

            // Create the attribute and write the data of the Hdf5Attribute into it.
            if (getElementId() >= 0 && attribute.getDataspaceId() >= 0 && attribute.getType().getConstants()[0] >= 0) {
            	attribute.setAttributeId(H5.H5Acreate(getElementId(), attribute.getName(),
                		attribute.getType().getConstants()[0], attribute.getDataspaceId(),
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
            	attribute.setOpen(true);
                
                if (attribute.getAttributeId() >= 0 && attribute.getType().getConstants()[1] >= 0) {
                    H5.H5Awrite(attribute.getAttributeId(), attribute.getType().getConstants()[1], attribute.getValue());
            		getAttributes().add(attribute);
                }
            }
        } catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Attribute could not be created", hnpe);
        }
	}
	
	public synchronized Hdf5Attribute<?> getAttribute(final String name) {
		Hdf5Attribute<?> attribute = null;
		
		Iterator<Hdf5Attribute<?>> iter = getAttributes().iterator();
		boolean found = false;
		while (!found && iter.hasNext()) {
			Hdf5Attribute<?> attr = iter.next();
			if (attr.getName().equals(name)) {
				attribute = attr;
				found = true;
			}
		}
		
		try {
			if (!found && existsAttribute(name)) {
				try {
					Hdf5DataType type = findAttributeType(name);
					
					attribute = Hdf5Attribute.getInstance(this, name, /* TODO dimension 0L */ 0L, type, false);
						
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
				}
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of attribute could not be checked", hlnpe);
			/* attribute stays null */
		}
		
		return attribute;
	}
	
	public boolean existsAttribute(final String name) throws HDF5LibraryException, NullPointerException {
		return H5.H5Aexists(getElementId(), name);
	}
	
	public Hdf5Attribute<?> getAttributeByPath(final String path) {
		Hdf5Attribute<?> attribute = null;
		
		if (path.contains("/")) {
			String name = path.substring(path.lastIndexOf("/") + 1);
			if (this instanceof Hdf5Group) {
				assert (this instanceof Hdf5Group);
				String pathWithoutName = path.substring(0, path.length() - name.length() - 1);
				Hdf5DataSet<?> dataSet = ((Hdf5Group) this).getDataSetByPath(pathWithoutName);
				if (dataSet != null) {
					attribute = dataSet.getAttribute(name);
				} else {
					Hdf5Group group = ((Hdf5Group) this).getGroupByPath(pathWithoutName);
					attribute = group.getAttribute(name);
				}
			}
		} else {
			attribute = getAttribute(path);
		}
		
		return attribute;
	}
	
	void addAttribute(Hdf5Attribute<?> attribute) {
		m_attributes.add(attribute);
		attribute.setPathFromFile(getPathFromFileWithName(false));
		attribute.setParent(this);
	}
	
	public List<String> loadAttributeNames() {
		List<String> attrNames = new ArrayList<>();
		long numAttrs = -1;
		Hdf5TreeElement treeElement = this instanceof Hdf5File ? this : getParent();
		String path = this instanceof Hdf5File ? "/" : getName();
		
		try {
			if (treeElement.isOpen()) {
				numAttrs = H5.H5Oget_info(getElementId()).num_attrs;
				for (int i = 0; i < numAttrs; i++) {
					attrNames.add(H5.H5Aget_name_by_idx(treeElement.getElementId(), path,
							HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC, i,
							HDF5Constants.H5P_DEFAULT));
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("The parent \"" + treeElement.getName()
						+ "\" of \"" + path + "\" is not open!", new IllegalStateException());
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("List of attributes could not be loaded", hlnpe);
		}
		
		return attrNames;
	}
	
	Map<String, Hdf5DataType> getDirectAttributesInfo() {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		String path = getPathFromFileWithName(false);
		
		Iterator<String> iterAttr = loadAttributeNames().iterator();
		while (iterAttr.hasNext()) {
			String name = iterAttr.next();
			
			try {
				Hdf5DataType dataType = findAttributeType(name);
				paths.put(path + name, dataType);
				
			} catch (IllegalArgumentException iae) {
				NodeLogger.getLogger("HDF5 Files").error(iae.getMessage(), iae);
				
			} catch (UnsupportedDataTypeException udte) {
				NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
			}
		}
		
		return paths;
	}
	
	/**
	 * 
	 * @param name name of the attribute in this treeElement
	 * @return dataType of the attribute (TODO doesn't work for {@code H5T_VLEN} and {@code H5T_REFERENCE} at the moment)
	 * @throws UnsupportedDataTypeException 
	 */
	Hdf5DataType findAttributeType(String name) throws UnsupportedDataTypeException, IllegalArgumentException {
		Hdf5DataType dataType = null;
		
		if (loadAttributeNames().contains(name)) {
			long attributeId = -1;
			long dataspaceId = -1;
			long classId = -1;
			int size = 0;
			boolean unsigned = false;
			boolean vlen = false;

			try {
				attributeId = H5.H5Aopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);

				if (attributeId >= 0) {
					dataspaceId = H5.H5Aget_space(attributeId);
				}
				
				if (dataspaceId >= 0) {
					int rank = H5.H5Sget_simple_extent_ndims(dataspaceId);
					if (rank > 0) {
						long[] dims = new long[rank];
						H5.H5Sget_simple_extent_dims(dataspaceId, dims, null);
	                    for (int j = 0; j < dims.length; j++) {
	                    	if (dims[j] == 0) {
		                    	H5.H5Aclose(attributeId);
	    			            H5.H5Sclose(dataspaceId);
	    			            throw new UnsupportedDataTypeException("Array of Attribute \"" 
	    								+ getPathFromFileWithName(true) + name + "\" has length 0");
		    				}
	                    }
	                }
		            H5.H5Sclose(dataspaceId);
				}
				
				long typeId = H5.H5Aget_type(attributeId);
				classId = H5.H5Tget_class(typeId);
				size = (int) H5.H5Tget_size(typeId);
				vlen = classId == HDF5Constants.H5T_VLEN || H5.H5Tis_variable_str(typeId);
				if (classId == HDF5Constants.H5T_INTEGER /*TODO || classId == HDF5Constants.H5T_CHAR*/) {
					unsigned = HDF5Constants.H5T_SGN_NONE == H5.H5Tget_sign(typeId);
				}
				H5.H5Tclose(typeId);
				
				if (classId == HDF5Constants.H5T_VLEN) {
					// TODO find correct real dataType
					H5.H5Aclose(attributeId);
					throw new UnsupportedDataTypeException("DataType H5T_VLEN of attribute \"" + name + "\" in treeElement \""
							+ getPathFromFile() + getName() + "\" is not supported");
				}
				
				if (classId == HDF5Constants.H5T_REFERENCE) {
					H5.H5Aclose(attributeId);
					throw new UnsupportedDataTypeException("DataType H5T_REFERENCE of attribute \"" + name + "\" in treeElement \""
							+ getPathFromFile() + getName() + "\" is not supported");
				}
				
				dataType = new Hdf5DataType(attributeId, classId, size, unsigned, vlen, /* TODO stringLength 0L */ 0L, false);
				H5.H5Aclose(attributeId);
				
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				NodeLogger.getLogger("Hdf5 Files").error("DataType for \"" + name + "\" could not be created", hlnpe);	
			}
			
			return dataType;
			
		} else {
			throw new IllegalArgumentException("There isn't an attribute \"" + name + "\" in treeElement \""
					+ getPathFromFile() + getName() + "\"");
		}
	}
	
	public abstract void open();
	
	public abstract void close();
}

package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;

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
	protected Hdf5TreeElement(final String name, final String filePath)
			throws NullPointerException, IllegalArgumentException {
		if (name == null) {
			NodeLogger.getLogger("HDF5 Files").error("name is null",
					new NullPointerException());
			throw new NullPointerException();
		} else if (name.equals("")) {
			NodeLogger.getLogger("HDF5 Files").error("name may not be the empty String!",
					new IllegalArgumentException());
			throw new IllegalArgumentException();
		} else if (name.contains("/")) {
			NodeLogger.getLogger("HDF5 Files").error("name " + name + " contains '/'",
					new IllegalArgumentException());
			throw new IllegalArgumentException();
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
	
	public Hdf5Attribute<?> getAttribute(final String name) {
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
		
		if (!found && existsAttribute(name)) {
			attribute = Hdf5Attribute.getInstance(this, name);
		}
		
		return attribute;
	}
	
	// TODO use library function instead of this
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
	
	public List<String> loadAttributeNames() {
		List<String> attrNames = new LinkedList<>();
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
				NodeLogger.getLogger("HDF5 Files").error("The parent "
						+ treeElement.getName() + " of " + path + " is not open!", new IllegalStateException());
			}
		} catch (HDF5LibraryException | NullPointerException lnpe) {
			lnpe.printStackTrace();
		}
		
		return attrNames;
	}
	
	Map<String, Hdf5DataType> getDirectAttributesInfo() {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		String path = getPathFromFileWithName(false);
		
		Iterator<String> iterAttr = loadAttributeNames().iterator();
		while (iterAttr.hasNext()) {
			String name = iterAttr.next();
			paths.put(path + name, findAttributeType(name));
		}
		
		return paths;
	}
	
	/**
	 * 
	 * @param name name of the attribute in this treeElement
	 * @return dataType of the attribute (TODO doesn't work for {@code H5T_VLEN} and {@code H5T_REFERENCE} at the moment)
	 */
	Hdf5DataType findAttributeType(String name) {
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
		                    	try {
		            				H5.H5Aclose(attributeId);
		    			            H5.H5Sclose(dataspaceId);
		    						NodeLogger.getLogger("HDF5 Files").warn("Array of Attribute \"" 
		    								+ getPathFromFileWithName(true) + name + "\" has length 0");
		    						return null;
		    					} catch (HDF5LibraryException hle) {
		    						hle.printStackTrace();
		    					}
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
					NodeLogger.getLogger("HDF5 Files").warn("DataType H5T_VLEN of attribute \"" + name + "\" in treeElement \""
							+ getPathFromFile() + getName() + "\" is not supported");
					H5.H5Aclose(attributeId);
					return null;
				}
				
				if (classId == HDF5Constants.H5T_REFERENCE) {
					NodeLogger.getLogger("HDF5 Files").warn("DataType H5T_REFERENCE of attribute \"" + name + "\" in treeElement \""
							+ getPathFromFile() + getName() + "\" is not supported");
					H5.H5Aclose(attributeId);
					return null;
				}
				
				dataType = new Hdf5DataType(classId, size, unsigned, vlen, false);
				if (classId == HDF5Constants.H5T_STRING) {
					dataType.getHdfType().initInstanceString(attributeId);
				}
				H5.H5Aclose(attributeId);
				
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
			
			return dataType;
		} else {
			NodeLogger.getLogger("HDF5 Files").error("There isn't an attribute \"" + name + "\" in treeElement \""
					+ getPathFromFile() + getName() + '"', new IllegalArgumentException());
			return null;
		}
	}
	
	public void addAttribute(Hdf5Attribute<?> attribute) {
		try {
            if (getElementId() >= 0) {
            	attribute.setAttributeId(H5.H5Aopen(getElementId(), attribute.getName(), HDF5Constants.H5P_DEFAULT));
        		getAttributes().add(attribute);
        		attribute.loadDimension();
            	attribute.setOpen(true);
            }
        } catch (HDF5LibraryException le) {
        	// TODO makes no sense at the moment, but will be changed when the HDF5WriterDialog has been implemented
        	if (attribute.getType().isHdfType(Hdf5HdfDataType.STRING)) {
        		attribute.getType().getHdfType().createInstanceString(attribute.getAttributeId(),
        				attribute.getType().getHdfType().getStringLength());
        	}
        	
        	// Create the data space for the attribute.
        	long[] dims = { attribute.getDimension() };
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

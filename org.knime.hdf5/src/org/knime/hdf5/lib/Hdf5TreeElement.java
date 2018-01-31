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
	
	Hdf5DataType findAttributeType(String name) {
		if (loadAttributeNames().contains(name)) {
			long attributeId = -1;
			long dataspaceId = -1;
			String dataType = "";
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
				dataType = H5.H5Tget_class_name(H5.H5Tget_class(typeId));
				size = (int) H5.H5Tget_size(typeId);
				vlen = dataType.equals("H5T_VLEN") || H5.H5Tis_variable_str(typeId);
				if (dataType.equals("H5T_INTEGER") || dataType.equals("H5T_CHAR")) {
					unsigned = HDF5Constants.H5T_SGN_NONE == H5.H5Tget_sign(typeId);
				} else if (vlen) {
					// TODO determine correct dataType
					
					/*
					long t = H5.H5Tget_native_type(typeId);
					System.out.println(H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_CHAR)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_SHORT)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_INT)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_LONG)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_LLONG)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_UCHAR)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_USHORT)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_UINT)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_ULONG)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_ULLONG)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_FLOAT)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_DOUBLE)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_LDOUBLE)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_B8)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_B16)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_B32)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_B64)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_HERR)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_OPAQUE)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_HSIZE)
							+ "\t" + H5.H5Tequal(t, HDF5Constants.H5T_NATIVE_HADDR) + "\t" + name);
					*/
				}
				
				H5.H5Tclose(typeId);
				H5.H5Aclose(attributeId);
			} catch (HDF5LibraryException | NullPointerException lnpe) {
				lnpe.printStackTrace();
			}
			
			return new Hdf5DataType(dataType, size, unsigned, vlen, false);
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
        		attribute.updateDimension();
            	attribute.setOpen(true);
            }
        } catch (HDF5LibraryException le) {
        	if (attribute.getType().isHdfType(Hdf5HdfDataType.STRING)) {
        		attribute.getType().createStringTypes(attribute.getStringLength());
        	}
        	
        	// Create the data space for the attribute.
        	// the array length can only be 1 for Strings
        	long[] dims = { attribute.getType().isHdfType(Hdf5HdfDataType.STRING) ? 1 : attribute.getDimension() };
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

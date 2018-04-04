package org.knime.hdf5.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5DataTypeTemplate;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataTypeTemplate;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
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
	
	public synchronized Hdf5Attribute<?> createAttribute(final String name, final long dimension, final Hdf5DataType type) throws IOException {
		Hdf5Attribute<?> attribute = null;
		
		try {
			if (!existsAttribute(name)) {
				attribute = Hdf5Attribute.createAttribute(this, name, dimension, type);
				
			} else {
				throw new IOException("There is already an attribute with the name \""
						+ name + "\" in this treeElement");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of attribute could not be checked", hlnpe);
			/* attribute stays null */
		}
		
		return attribute;
	}
	
	public synchronized Hdf5Attribute<?> getAttribute(final String name) throws IOException {
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
		
		if (!found) {
			try {
				if (existsAttribute(name)) {
					attribute = Hdf5Attribute.openAttribute(this, name);
					
				} else {
					throw new IOException("Attribute \"" + getPathFromFileWithName() + name + "\" does not exist");
				}
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				NodeLogger.getLogger("HDF5 Files").error("Existence of attribute could not be checked", hlnpe);
				/* attribute stays null */
			}
		}
		
		return attribute;
	}
	
	public boolean existsAttribute(final String name) throws HDF5LibraryException, NullPointerException {
		return H5.H5Aexists(getElementId(), name);
	}
	
	public Hdf5Attribute<?> getAttributeByPath(final String path) throws IOException {
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
		String name = this instanceof Hdf5File ? "/" : getName();
		
		try {
			if (treeElement.isOpen()) {
				numAttrs = H5.H5Oget_info(getElementId()).num_attrs;
				for (int i = 0; i < numAttrs; i++) {
					attrNames.add(H5.H5Aget_name_by_idx(treeElement.getElementId(), name,
							HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC, i,
							HDF5Constants.H5P_DEFAULT));
				}
			} else {
				NodeLogger.getLogger("HDF5 Files").error("The parent \"" + treeElement.getName()
						+ "\" of \"" + name + "\" is not open!", new IllegalStateException());
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
	
	public Map<String, Hdf5DataType> getAllAttributesInfo() {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		
		if (isOpen()) {
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
			
			
			
			if (this instanceof Hdf5Group) {
				Iterator<String> iterDS = ((Hdf5Group) this).loadDataSetNames().iterator();
				while (iterDS.hasNext()) {
					try {
						Hdf5DataSet<?> dataSet = ((Hdf5Group) this).getDataSet(iterDS.next());
						paths.putAll(dataSet.getAllAttributesInfo());
						
					} catch (IOException | NullPointerException ionpe) {
						NodeLogger.getLogger("HDF5 Files").error(ionpe.getMessage(), ionpe);
					}
				}
			
				Iterator<String> iterG = ((Hdf5Group) this).loadGroupNames().iterator();
				while (iterG.hasNext()) {
					try {
						Hdf5Group group = ((Hdf5Group) this).getGroup(iterG.next());
						paths.putAll(group.getAllAttributesInfo());
						
					} catch (IOException | NullPointerException ionpe) {
						NodeLogger.getLogger("HDF5 Files").error(ionpe.getMessage(), ionpe);
					}
				}
			}
		} else {
			throw new IllegalStateException("\"" + getPathFromFileWithName() + "\" is not open");
		}
		
		return paths;
	}
	
	@SuppressWarnings("unchecked")
	public Hdf5Attribute<?> createAndWriteAttributeFromFlowVariable(FlowVariable flowVariable) throws IOException {
		Hdf5DataTypeTemplate dataTypeTempl = null;
		
		switch (flowVariable.getType()) {
		case INTEGER:
			dataTypeTempl = new Hdf5DataTypeTemplate(
					new Hdf5HdfDataTypeTemplate(HdfDataType.INTEGER, Hdf5HdfDataType.DEFAULT_STRING_LENGTH), 
					Hdf5KnimeDataType.INTEGER, false, true);
			Hdf5Attribute<Integer> attributeInteger = (Hdf5Attribute<Integer>) createAttribute(flowVariable.getName().substring(flowVariable.getName().lastIndexOf("/") + 1), 1L, dataTypeTempl);
			attributeInteger.write(new Integer[] { flowVariable.getIntValue() });
			return attributeInteger;
			
		case DOUBLE:
			dataTypeTempl = new Hdf5DataTypeTemplate(
					new Hdf5HdfDataTypeTemplate(HdfDataType.DOUBLE, Hdf5HdfDataType.DEFAULT_STRING_LENGTH), 
					Hdf5KnimeDataType.DOUBLE, false, true);
			Hdf5Attribute<Double> attributeDouble = (Hdf5Attribute<Double>) createAttribute(flowVariable.getName().substring(flowVariable.getName().lastIndexOf("/") + 1), 1L, dataTypeTempl);
			attributeDouble.write(new Double[] { flowVariable.getDoubleValue() });
			return attributeDouble;
			
		case STRING:
			dataTypeTempl = new Hdf5DataTypeTemplate(
					new Hdf5HdfDataTypeTemplate(HdfDataType.STRING, Hdf5HdfDataType.DEFAULT_STRING_LENGTH), 
					Hdf5KnimeDataType.STRING, false, true);
			Hdf5Attribute<String> attributeString = (Hdf5Attribute<String>) createAttribute(flowVariable.getName().substring(flowVariable.getName().lastIndexOf("/") + 1), 1L, dataTypeTempl);
			attributeString.write(new String[] { flowVariable.getStringValue() });
			return attributeString;
			
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	/**
	 *
	 * @param object only ATTRIBUTE and DATASET allowed, DATASET only for instances of Hdf5Group
	 * @return
	 */
	protected DataTableSpec createSpecOfObjects(int objectId) {
		List<DataColumnSpec> colSpecList = new ArrayList<>();
		
		Map<String, Hdf5DataType> objInfo = objectId == HDF5Constants.H5I_ATTR ? getAllAttributesInfo() : ((Hdf5Group) this).getAllDataSetsInfo();
		Iterator<String> iter = objInfo.keySet().iterator();
		while (iter.hasNext()) {
			String objPath = iter.next();
			Hdf5DataType dataType = objInfo.get(objPath);
			if (dataType != null) {
				try {
					DataType objType = dataType.getKnimeType().getColumnDataType();
					colSpecList.add(new DataColumnSpecCreator(objPath, objType).createSpec());
					
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
				}
			}	
		}
		
		return new DataTableSpec(colSpecList.toArray(new DataColumnSpec[] {}));
	}
	
	public DataTableSpec createSpecOfAttributes() {
		return createSpecOfObjects(HDF5Constants.H5I_ATTR);
	}
	
	/**
	 * 
	 * @param name name of the attribute in this treeElement
	 * @return dataType of the attribute (TODO doesn't work for {@code H5T_VLEN} and {@code H5T_REFERENCE} at the moment)
	 * @throws UnsupportedDataTypeException 
	 */
	Hdf5DataType findAttributeType(String name) throws UnsupportedDataTypeException, IllegalArgumentException {
		Hdf5DataType dataType = null;
		
		boolean attrExists = false;
		try {
			attrExists = existsAttribute(name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of attribute could not be checked", hlnpe);
		}
		
		if (attrExists) {
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
				
				dataType = Hdf5DataType.openDataType(attributeId, classId, size, unsigned, vlen);
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

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
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.AttributeNodeEdit;

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
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		return m_open;
	}

	protected void setOpen(boolean open) {
		if (isFile()) {
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
	 * Returns {@code true} if this is an instance of Hdf5Group. It is either an Hdf5Group or Hdf5File.
	 * 
	 * @return {@code true} if this is an instance of Hdf5Group
	 */
	public boolean isGroup() {
		return this instanceof Hdf5Group;
	}
	
	public boolean isFile() {
		return this instanceof Hdf5File;
	}
	
	public boolean isDataSet() {
		return this instanceof Hdf5DataSet;
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
		return isFile() ? (endSlash ? "/" : "") : getPathFromFile() + getName() + "/";
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
						+ name + "\" in the treeElement \"" + getPathFromFileWithName() + "\"");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("Existence of attribute could not be checked", hlnpe);
			/* attribute stays null */
		}
		
		return attribute;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <Type> boolean createAndWriteAttribute(final String name, Type[] values) throws IOException {
		Hdf5Attribute<Type> attribute = null;
		Hdf5DataType dataType = null;
		
		Class valuesType = values.getClass().getComponentType();
		if (valuesType == Integer.class) {
			dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(HdfDataType.INTEGER, Endian.BIG_ENDIAN),
					Hdf5KnimeDataType.INTEGER, false, false, 0L);
			
		} else if (valuesType == Double.class) {
			dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(HdfDataType.DOUBLE, Endian.BIG_ENDIAN),
					Hdf5KnimeDataType.DOUBLE, false, false, 0L);
			
		} else if (valuesType == String.class) {
			long stringLength = 0;
			for (Type value : values) {
				long length = ((String) value).length();
				stringLength = length > stringLength ? length : stringLength;
			}
			dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(HdfDataType.STRING, Endian.BIG_ENDIAN),
					Hdf5KnimeDataType.STRING, false, false, stringLength);
		}
		
		if (dataType != null) {
			attribute = (Hdf5Attribute<Type>) createAttribute(name, values.length, dataType);
			return attribute.write(values);
		}
		
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public boolean createAndWriteAttribute(AttributeNodeEdit edit, FlowVariable flowVariable) throws IOException, HDF5LibraryException, NullPointerException {
		if (existsAttribute(edit.getName())) {
			if (edit.isOverwrite()) {
				deleteAttribute(edit.getName());
				
			} else {
				IOException ioe = new IOException("Abort: attribute \"" + getPathFromFileWithName() + edit.getName() + "\" already exists");
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
				return false;
			}
		}
		
		long stringLength = edit.isFixed() ? edit.getStringLength() : flowVariable.getValueAsString().length();
		Hdf5DataType dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(edit.getHdfType(), edit.getEndian()), edit.getKnimeType(), false, false, stringLength);
		
		switch (flowVariable.getType()) {
		case INTEGER:
			Hdf5Attribute<Integer> attributeInteger = (Hdf5Attribute<Integer>) createAttribute(edit.getName(), 1L, dataType);
			return attributeInteger.write(new Integer[] { flowVariable.getIntValue() });
			
		case DOUBLE:
			Hdf5Attribute<Double> attributeDouble = (Hdf5Attribute<Double>) createAttribute(edit.getName(), 1L, dataType);
			return attributeDouble.write(new Double[] { flowVariable.getDoubleValue() });
			
		case STRING:
			Hdf5Attribute<String> attributeString = (Hdf5Attribute<String>) createAttribute(edit.getName(), 1L, dataType);
			return attributeString.write(new String[] { flowVariable.getStringValue() });
			
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	public synchronized Hdf5Attribute<?> getAttribute(final String name) throws IOException {
		Hdf5Attribute<?> attribute = null;
		
		Iterator<Hdf5Attribute<?>> iter = getAttributes().iterator();
		boolean found = false;
		while (!found && iter.hasNext()) {
			Hdf5Attribute<?> attr = iter.next();
			if (attr.getName().equals(name)) {
				attribute = attr;
				attr.open();
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
			if (isGroup()) {
				String pathWithoutName = path.substring(0, path.length() - name.length() - 1);
				try {
					Hdf5DataSet<?> dataSet = ((Hdf5Group) this).getDataSetByPath(pathWithoutName);
					attribute = dataSet.getAttribute(name);
					
				} catch (IOException ioe) {
					Hdf5Group group = ((Hdf5Group) this).getGroupByPath(pathWithoutName);
					attribute = group.getAttribute(name);
				}
			}
		} else {
			attribute = getAttribute(path);
		}
		
		return attribute;
	}
	
	/**
	 * 
	 * @param 	name the name of the attribute to delete
	 * @return 	-1 if deletion from disk was unsuccessful <br>
	 * 			0 if deletion from disk was successful, but unsuccessful deletion from runtime data <br>
	 * 			1 if fully successful
	 * @throws HDF5LibraryException
	 * @throws NullPointerException
	 */
	public synchronized int deleteAttribute(final String name) throws HDF5LibraryException, NullPointerException {
		int success = H5.H5Adelete(getElementId(), name);
		
		if (success >= 0) {
			success = 0;
			for (Hdf5Attribute<?> attr : m_attributes) {
				if (name.equals(attr.getName())) {
					removeAttribute(attr);
					success++;
					break;
				}
			}
		}
		
		return success;
	}
	
	void addAttribute(Hdf5Attribute<?> attribute) {
		m_attributes.add(attribute);
		attribute.setPathFromFile(getPathFromFileWithName(false));
		attribute.setParent(this);
	}
	
	private void removeAttribute(Hdf5Attribute<?> attribute) {
		m_attributes.remove(attribute);
		attribute.setPathFromFile("");
		attribute.setParent(null);
	}
	
	public List<String> loadAttributeNames() throws IllegalStateException {
		List<String> attrNames = new ArrayList<>();
		long numAttrs = -1;
		Hdf5TreeElement treeElement = isFile() ? this : getParent();
		String name = isFile() ? "/" : getName();
		
		try {
			if (treeElement.isOpen()) {
				numAttrs = H5.H5Oget_info(getElementId()).num_attrs;
				for (int i = 0; i < numAttrs; i++) {
					attrNames.add(H5.H5Aget_name_by_idx(treeElement.getElementId(), name,
							HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC, i,
							HDF5Constants.H5P_DEFAULT));
				}
			} else {
				throw new IllegalStateException("The parent \"" + treeElement.getName()
						+ "\" of \"" + name + "\" is not open!");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			NodeLogger.getLogger("HDF5 Files").error("List of attributes could not be loaded", hlnpe);
		}
		
		return attrNames;
	}
	
	public Map<String, Hdf5DataType> getAllAttributesInfo() throws IllegalStateException {
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
			
			if (isGroup()) {
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
	
	/**
	 *
	 * @param object only ATTRIBUTE and DATASET allowed, DATASET only for instances of Hdf5Group
	 * @return
	 */
	protected DataTableSpec createSpecOfObjects(int objectId) throws IllegalStateException {
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
	
	public DataTableSpec createSpecOfAttributes() throws IllegalStateException {
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
			Endian endian = null;
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
				endian = H5.H5Tget_order(typeId) == HDF5Constants.H5T_ORDER_LE ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN;
				vlen = classId == HDF5Constants.H5T_VLEN || H5.H5Tis_variable_str(typeId);
				if (classId == HDF5Constants.H5T_INTEGER) {
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
				
				dataType = Hdf5DataType.openDataType(attributeId, classId, size, endian, unsigned, vlen);
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

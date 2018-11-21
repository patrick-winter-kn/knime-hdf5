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
import org.knime.hdf5.nodes.writer.edit.EditDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

abstract public class Hdf5TreeElement {

	private final String m_name;
	
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
	protected Hdf5TreeElement(final String name)
			throws NullPointerException, IllegalArgumentException {
		if (name == null) {
			throw new NullPointerException("name cannot be null");
			
		} else if (name.equals("")) {
			throw new IllegalArgumentException("name cannot be the Empty String");
			
		} else if (name.contains("/")) {
			throw new IllegalArgumentException("name \"" + name + "\" cannot contain '/'");
		}

		m_name = name;
	}
	
	public static String[] getPathAndName(String pathWithName) {
		if (pathWithName != null) {
			int firstSlashInName = pathWithName.indexOf("\\/");
			String pathWithFirstPartOfName = firstSlashInName == -1 ? pathWithName : pathWithName.substring(0, firstSlashInName);
			int pathStringLength = pathWithFirstPartOfName.lastIndexOf("/");
			
			String name = pathWithName.substring(pathStringLength + 1);
			name = name.replaceAll("\\\\/", "/");
			String pathWithoutName = pathStringLength == -1 ? "" : pathWithName.substring(0, pathStringLength);
			
			return new String[] { pathWithoutName, name };
		}
		
		return new String[2];
	}
	
	public static String getUniqueName(List<String> usedNames, String name) {
		String newName = name;
		
		if (usedNames.contains(newName)) {
			String oldName = name;
			int i = 1;
			
			if (oldName.matches(".*\\([1-9][0-9]*\\)")) {
				i = Integer.parseInt(oldName.substring(oldName.lastIndexOf("(") + 1, oldName.lastIndexOf(")")));
				oldName = oldName.substring(0, oldName.lastIndexOf("("));
			}
			
			while (usedNames.contains(newName)) {
				newName = oldName + "(" + i + ")";
				i++;
			}
		}
		
		return newName;
	}
	
	public String getName() {
		return m_name;
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
	
	public boolean isValid() {
		return m_elementId != -1;
	}
	
	/**
	 * 
	 * @param endSlash {@code true} if an {@code '/'} should be added after the name (only possible if it is no {@code Hdf5File})
	 * @return
	 */
	public String getPathFromFileWithName(boolean endSlash) {
		return (isFile() ? "" : getPathFromFile() + getName() + (endSlash ? "/" : ""));
	}
	
	public String getPathFromFileWithName() {
		return getPathFromFileWithName(false);
	}
	
	public Hdf5TreeElement createBackup(String prefix) throws IOException, IllegalStateException, IllegalArgumentException {
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		if (prefix.contains("/")) {
			throw new IllegalArgumentException("Prefix for backup file cannot contain '/'");
		}
		
		return m_parent.copyObject(m_name, m_parent, getUniqueName(m_parent.loadObjectNames(), prefix + m_name));
	}
	
	public synchronized Hdf5Attribute<?> createAttribute(final String name, final long dimension, final Hdf5DataType type) throws IOException {
		if (!existsAttribute(name)) {
			return Hdf5Attribute.createAttribute(this, name, dimension, type);
			
		} else {
			throw new IOException("There is already an attribute with the name \""
					+ name + "\" in treeElement \"" + getPathFromFileWithName() + "\"");
		}
	}
	
	public Hdf5Attribute<?> createAttributeFromEdit(AttributeNodeEdit edit, long dimension) throws IOException {
		EditDataType editDataType = edit.getEditDataType();
		Hdf5DataType dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(editDataType.getOutputType(), editDataType.getEndian()),
				Hdf5KnimeDataType.getKnimeDataType(edit.getInputType(), false), false, false, editDataType.getStringLength());
		
		if (!existsAttribute(edit.getName())) {
			return createAttribute(edit.getName(), dimension, dataType);
		
		} else {
			throw new IOException("Attribute with the same name \""
					+ edit.getName() + "\" already exists in treeElement \"" + getPathFromFileWithName() + "\"");
		}
	}
	
	@SuppressWarnings("unchecked")
	public Hdf5Attribute<?> createAndWriteAttribute(final String name, Object[] values, boolean unsigned) throws IOException {
		Hdf5Attribute<Object> attribute = null;
		
		HdfDataType hdfType = HdfDataType.getHdfDataType(values, unsigned);
		Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(hdfType, false);
		long stringLength = 0L;
		if (hdfType == HdfDataType.STRING) {
			for (Object value : values) {
				long length = ((String) value).length();
				stringLength = length > stringLength ? length : stringLength;
			}
		}
		Hdf5DataType dataType = Hdf5DataType.createDataType(Hdf5HdfDataType.getInstance(hdfType, Endian.BIG_ENDIAN),
				knimeType, false, false, stringLength);
		
		if (dataType != null) {
			attribute = (Hdf5Attribute<Object>) createAttribute(name, values.length, dataType);
			
			boolean success = false;
			try {
				success = attribute.write(values, Rounding.DOWN);
				
			} finally {
				if (!success && existsAttribute(name)) {
					deleteAttribute(name);
				}
			}
		}
		
		return existsAttribute(name) ? getAttribute(name) : null;
	}
	
	@SuppressWarnings("unchecked")
	public Hdf5Attribute<?> createAndWriteAttribute(AttributeNodeEdit edit, FlowVariable flowVariable) throws IOException {
		Object[] values = Hdf5Attribute.getFlowVariableValues(flowVariable);
		Hdf5Attribute<Object> attribute = (Hdf5Attribute<Object>) createAttributeFromEdit(edit, values.length);
		if (attribute != null) {
			Rounding rounding = edit.getEditDataType().getRounding();
			
			boolean success = false;
			try {
				if (!edit.isFlowVariableArrayUsed() && flowVariable.getType() == FlowVariable.Type.STRING) {
					success = attribute.write(new String[] { flowVariable.getStringValue() }, rounding);
				} else {
					success = attribute.write(values, rounding);
				}
			} finally {
				if (!success && existsAttribute(edit.getName())) {
					deleteAttribute(edit.getName());
				}
			}
		}
	
		return existsAttribute(edit.getName()) ? getAttribute(edit.getName()) : null;
	}
	
	public Hdf5Attribute<?> createAndWriteAttribute(AttributeNodeEdit edit, Hdf5Attribute<?> copyAttribute) throws IOException {
		try {
			Hdf5Attribute<?> newAttribute = createAttributeFromEdit(edit, copyAttribute.getDimension());
			if (newAttribute != null) {
				boolean success = false;
				try {
					copyAttribute.open();
					success = newAttribute.copyValuesFrom(copyAttribute, edit.getEditDataType().getRounding());
					
				} finally {
					if (!success && existsAttribute(edit.getName())) {
						deleteAttribute(edit.getName());
					}
				}
			}
		} catch (HDF5LibraryException | IOException hlioe) {
			throw new IOException("Attribute \"" + copyAttribute.getPathFromFileWithName()
					+ "\" could not be copied to \"" + getPathFromFileWithName() + "\": " + hlioe.getMessage(), hlioe);
		}
		
		return existsAttribute(edit.getName()) ? getAttribute(edit.getName()) : null;
	}
	
	public Hdf5Attribute<?> copyAttribute(Hdf5Attribute<?> copyAttribute, Hdf5TreeElement newParent, String newName) throws IOException {
		Hdf5Attribute<?> newAttribute = null;
		
		try {
			newAttribute = newParent.createAttribute(newName, copyAttribute.getDimension(), Hdf5DataType.createCopyFrom(copyAttribute.getType()));
			
			if (newAttribute != null) {
				boolean success = false;
				try {
					copyAttribute.open();
					success = newAttribute.copyValuesFrom(copyAttribute, Rounding.DOWN);
					
				} finally {
					if (!success && existsAttribute(newName)) {
						deleteAttribute(newName);
					}
				}
			}
		} catch (HDF5LibraryException | IOException hlioe) {
			throw new IOException("Attribute \"" + copyAttribute.getPathFromFileWithName()
					+ "\" could not be copied with name \"" + newName + "\": " + hlioe.getMessage(), hlioe);
		}

		return existsAttribute(newName) ? getAttribute(newName) : null;
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
			if (existsAttribute(name)) {
				attribute = Hdf5Attribute.openAttribute(this, name);
				
			} else {
				throw new IOException("Attribute \"" + name + "\" in \"" + getPathFromFileWithName() + "\" does not exist");
			}
		}
		
		return attribute;
	}
	
	public Hdf5Attribute<?> getAttributeByPath(final String path) throws IOException {
		Hdf5Attribute<?> attribute = null;
		
		if (path.matches(".*(?<!\\\\)/.*")) {
			String[] pathAndName = getPathAndName(path);
			attribute = getAttributeByPath(pathAndName[0], pathAndName[1]);
			
		} else {
			attribute = getAttribute(path.replaceAll("\\\\/", "/"));
		}
		
		return attribute;
	}
	
	public Hdf5Attribute<?> getAttributeByPath(final String path, final String name) throws IOException {
		Hdf5TreeElement treeElement = this;
		
		if (!path.isEmpty()) {
			if (!isGroup()) {
				throw new IOException("Only groups can contain paths for child treeElements");
			}
			
			try {
				treeElement = ((Hdf5Group) this).getDataSetByPath(path);
			} catch (IOException ioe) {
				treeElement = ((Hdf5Group) this).getGroupByPath(path);
			}
		}
		
		return treeElement.getAttribute(name);
	}
	
	public Hdf5Attribute<?> updateAttribute(final String name) throws IOException {
		for (Hdf5Attribute<?> attr : getAttributes()) {
			if (attr.getName().equals(name)) {
				removeAttribute(attr);
				break;
			}
		}
		
		return getAttribute(name);
	}
	
	public boolean existsAttribute(final String name) throws IOException {
		try {
			return H5.H5Aexists(getElementId(), name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			throw new IOException("Existence of attribute \"" + name + "\" in treeElement \""
					+ getPathFromFileWithName() + "\" could not be checked: " + hlnpe.getMessage(), hlnpe);
		}
	}
	
	public synchronized Hdf5Attribute<?> renameAttribute(String oldName, String newName) throws IOException {
		boolean success = false;
		
		try {
			Hdf5Attribute<?> attribute = getAttribute(oldName);
			attribute.close();
			success = H5.H5Arename(getElementId(), oldName, newName) >= 0;
			if (success) {
				removeAttribute(attribute);
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Attribute in \"" + getPathFromFileWithName(true)
					+ "\" could not be renamed from \"" + oldName + "\" to \"" + newName + "\": " + hlionpe.getMessage(), hlionpe);
		}
		
		return success ? getAttribute(newName) : null;
	}
	
	/**
	 * 
	 * @param 	name the name of the attribute to delete
	 * @return 	-1 if deletion from disk was unsuccessful <br>
	 * 			0 if deletion from disk was successful, but unsuccessful deletion from runtime data <br>
	 * 			1 if fully successful
	 * @throws IOException 
	 * @throws HDF5LibraryException
	 * @throws NullPointerException
	 */
	public synchronized int deleteAttribute(final String name) throws IOException {
		int success = -1;
		
		try {
			Hdf5Attribute<?> attribute = getAttribute(name);
			attribute.close();
			success = !attribute.isOpen() ? H5.H5Adelete(getElementId(), name) : -1;
			success = success >= 0 ? (removeAttribute(attribute) ? 1 : 0) : success;
			
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Attribute \"" + getPathFromFileWithName(true) + name
					+ "\" could not be deleted from disk: " + hlionpe.getMessage(), hlionpe);
		}
		
		return success;
	}
	
	void addAttribute(Hdf5Attribute<?> attribute) {
		m_attributes.add(attribute);
		attribute.setPathFromFile(getPathFromFileWithName(true));
		attribute.setParent(this);
	}
	
	private boolean removeAttribute(Hdf5Attribute<?> attribute) {
		attribute.setParent(null);
		attribute.setPathFromFile("");
		attribute.setAttributeId(-1);
		return m_attributes.remove(attribute);
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
			String path = getPathFromFileWithName(true);
			
			Iterator<String> iterAttr = loadAttributeNames().iterator();
			while (iterAttr.hasNext()) {
				String name = iterAttr.next();
				
				try {
					Hdf5DataType dataType = findAttributeType(name);
					name = name.replaceAll("/"/*"(?<!\\\\)/"*/, "\\\\/");
					paths.put(path + name, dataType);
					
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").warn("Attribute \"" + path + name
							+ "\" could not be loaded: " + udte.getMessage());
					
				} catch (IOException | NullPointerException ionpe) {
					NodeLogger.getLogger("HDF5 Files").error("Attribute \"" + path + name
							+ "\" could not be loaded: " + ionpe.getMessage(), ionpe);
				}
			}
			
			if (isGroup()) {
				Iterator<String> iterDS = ((Hdf5Group) this).loadDataSetNames().iterator();
				while (iterDS.hasNext()) {
					String name = iterDS.next();
					
					try {
						Hdf5DataSet<?> dataSet = ((Hdf5Group) this).getDataSet(name);
						paths.putAll(dataSet.getAllAttributesInfo());
						
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger("HDF5 Files").warn("DataSet \"" + path + name
								+ "\" could not be loaded: " + udte.getMessage(), udte);
						
					} catch (IOException | NullPointerException ionpe) {
						NodeLogger.getLogger("HDF5 Files").error("DataSet \"" + path + name
								+ "\" could not be loaded: " + ionpe.getMessage(), ionpe);
					}
				}
			
				Iterator<String> iterG = ((Hdf5Group) this).loadGroupNames().iterator();
				while (iterG.hasNext()) {
					String name = iterG.next();
					
					try {
						Hdf5Group group = ((Hdf5Group) this).getGroup(name);
						paths.putAll(group.getAllAttributesInfo());
						
					} catch (IOException | NullPointerException ionpe) {
						NodeLogger.getLogger("HDF5 Files").error("Group \"" + path + name
								+ "\" could not be loaded: " + ionpe.getMessage(), ionpe);
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
	 * @throws IOException 
	 */
	Hdf5DataType findAttributeType(String name) throws IOException {
		Hdf5DataType dataType = null;
		
		if (existsAttribute(name)) {
			long attributeId = -1;

			try {
				attributeId = H5.H5Aopen(getElementId(), name, HDF5Constants.H5P_DEFAULT);
				dataType = Hdf5DataType.openDataType(attributeId);
				
			} catch (HDF5LibraryException | NullPointerException hlnpe) {
				throw new IOException("DataType for attribute \"" + name + "\" in treeElement \""
						+ getPathFromFileWithName() + "\" could not be created: " + hlnpe.getMessage(), hlnpe);
				
			} finally {
				try {
					if (attributeId >= 0) {
						H5.H5Aclose(attributeId);
					}
				} catch (HDF5LibraryException hle) {
					NodeLogger.getLogger(getClass()).error("Attribute \"" + name + "\" in treeElement \""
							+ getPathFromFileWithName() + "\" could not be closed");
				}
			}
			
			return dataType;
			
		} else {
			throw new IllegalArgumentException("Attribute \"" + name + "\" in treeElement \""
					+ getPathFromFileWithName() + "\" does not exist");
		}
	}
	
	public abstract boolean open() throws IOException;
	
	public abstract boolean close();
}

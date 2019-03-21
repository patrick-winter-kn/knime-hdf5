package org.knime.hdf5.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

/**
 * Abstract class for hdf objects that appear as an tree element in the
 * hdf file, i.e. accessed through H5O in the hdf api. <br>
 * <br>
 * <b>Note:</b> This library only supports groups and dataSets as hdf objects.
 * It does not support named data types.
 */
abstract public class Hdf5TreeElement {

	private final String m_name;
	
	private final List<Hdf5Attribute<?>> m_attributes = new ArrayList<>();
	
	private long m_elementId = -1;
	
	private String m_pathFromFile = "";
	
	private Hdf5Group m_parent;

	/**
	 * Manages the access to the opening and closing process of this treeElement
	 * to guarantee that no thread closes an open treeElement while another
	 * thread is reading it.
	 */
	private final ReentrantReadWriteLock m_openLock = new ReentrantReadWriteLock(true);
	
	/**
	 * @param name the name of the treeElement
	 * @throws NullPointerException if the name is {@code null}
	 * @throws IllegalArgumentException if the name is empty or contains a '/'
	 */
	protected Hdf5TreeElement(final String name)
			throws NullPointerException, IllegalArgumentException {
		if (name == null) {
			throw new NullPointerException("name cannot be null");
			
		} else if (name.isEmpty()) {
			throw new IllegalArgumentException("name cannot be empty");
			
		} else if (name.contains("/")) {
			throw new IllegalArgumentException("name \"" + name + "\" cannot contain '/'");
		}

		m_name = name;
	}
	
	/**
	 * Separates the path from the name. It also includes the case for
	 * {@linkplain Hdf5Attribute} where '/' is allowed in the name.
	 * 
	 * @param pathWithName the path containing the name
	 * @return the separated pair of path and name
	 * @see Hdf5TreeElement#getAttributeByPath(String)
	 */
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
	
	/**
	 * Constructs a new name out of the input name such that is not contained in
	 * the input list.
	 * <br>
	 * For the input name {@code x}, it returns the first name that fulfills this
	 * property out of the list {@code [x, x(1), x(2), x(3), ...]}.
	 * <br>
	 * For the input name {@code x(i)}, it returns the first name that fulfills this
	 * property out of the list {@code [x(i), x(i+1), x(i+2), x(i+3), ...]}.
	 * <br>
	 * <br>
	 * <b>Note:</b> The constructed name will not be added to the input list.
	 * 
	 * @param usedNames list of names that are already used
	 * @param name the new name for the list {@code usedNames}
	 * @return a name that is constructed out of {@code name} that is not
	 * 	contained in {@code usedNames}
	 */
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

	/**
	 * @return the child attributes of this treeElement
	 */
	protected Hdf5Attribute<?>[] getAttributes() {
		synchronized (m_attributes) {
			return m_attributes.toArray(new Hdf5Attribute<?>[m_attributes.size()]);
		}
	}

	/**
	 * @return the id of this treeElement which is open in an hdf file (or -1 if closed)
	 */
	protected long getElementId() {
		return m_elementId;
	}

	protected void setElementId(long elementId) {
		m_elementId = elementId;
	}

	/**
	 * @return {@code true} if the treeElement is open
	 * @throws IllegalStateException if the method is used for a {@linkplain Hdf5File}
	 * 	(use {@linkplain Hdf5File#isOpen()} instead)
	 */
	protected boolean isOpen() throws IllegalStateException {
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		return m_elementId >= 0;
	}

	/**
	 * @return the path within the file
	 */
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
	 * @return {@code true} if this is an instance of {@linkplain Hdf5Group}
	 * 	(which can be a Hdf5Group or {@linkplain Hdf5File})
	 */
	public boolean isGroup() {
		return this instanceof Hdf5Group;
	}
	
	/**
	 * @return {@code true} if this is an instance of {@linkplain Hdf5File}
	 */
	public boolean isFile() {
		return this instanceof Hdf5File;
	}

	/**
	 * @return {@code true} if this is an instance of {@linkplain Hdf5DataSet}
	 */
	public boolean isDataSet() {
		return this instanceof Hdf5DataSet;
	}
	
	/**
	 * @return {@code true} if this treeElement exists in the hdf file
	 * @throws IOException if an internal error occurred while checking the existence
	 * @throws IllegalStateException if the method is used for a {@linkplain Hdf5File}
	 * 	(use {@linkplain Hdf5File#exists()} instead)
	 */
	public boolean exists() throws IOException, IllegalStateException {
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		
		if (m_parent != null) {
			int objectType = m_parent.getObjectTypeByName(m_name);
			if (isGroup()) {
				return objectType == HDF5Constants.H5I_GROUP;
				
			} else if (isDataSet()) {
				return objectType == HDF5Constants.H5I_DATASET;
				
			} else {
				throw new IllegalStateException("Wrong method used for " + getClass().getSimpleName());
			}
		}
		
		return false;
	}
	
	/**
	 * @throws IOException if this treeElement does not exist
	 */
	protected void checkExists() throws IOException {
    	if (!exists()) {
			throw new IOException("TreeElement does not exist");
		}
	}

	/**
	 * @throws IOException if this treeElement is not open
	 */
	protected void checkOpen() throws IOException {
		checkExists();
    	if (!isOpen()) {
			throw new IOException("TreeElement is not open");
		}
	}
	
	/**
	 * Acquires the lock such that no other thread can see/edit if this treeElement
	 * open.
	 */
	protected void lockWriteOpen() {
		m_openLock.writeLock().lock();
	}

	/**
	 * @see #lockWriteOpen()
	 */
	protected void unlockWriteOpen() {
		m_openLock.writeLock().unlock();
	}

	/**
	 * Acquires the lock such that no other thread can edit if this treeElement
	 * open.
	 */
	protected void lockReadOpen() {
		m_openLock.readLock().lock();
	}

	/**
	 * @see #lockReadOpen()
	 */
	protected void unlockReadOpen() {
		m_openLock.readLock().unlock();
	}
	
	/**
	 * @param endSlash {@code true} if a '/' should be added after the
	 * 	name (only possible if it is no {@linkplain Hdf5File})
	 * @return the concatenation of {@linkplain #getPathFromFile()},
	 * 	{@linkplain #getName()} and an optional '/' (or empty String if it is a
	 * 	{@linkplain Hdf5File})
	 */
	public String getPathFromFileWithName(boolean endSlash) {
		return (isFile() ? "" : getPathFromFile() + getName() + (endSlash ? "/" : ""));
	}
	
	/**
	 * @return the concatenation of {@linkplain #getPathFromFile()},
	 * 	{@linkplain #getName()} (or empty String if it is a {@linkplain Hdf5File})
	 */
	public String getPathFromFileWithName() {
		return getPathFromFileWithName(false);
	}
	
	/**
	 * @param prefix the prefix for the name like "temp_"
	 * @return the instance for the newly created copy of this treeElement
	 * 	in the hdf file
	 * @throws IOException if an internal error occurred while creating
	 * @throws IllegalStateException if the method is used for a {@linkplain Hdf5File}
	 * 	(use {@linkplain Hdf5File#createBackup(String)} instead)
	 * @throws IllegalArgumentException if the prefix contains '/'
	 */
	public Hdf5TreeElement createBackup(String prefix) throws IOException, IllegalStateException, IllegalArgumentException {
		if (isFile()) {
			throw new IllegalStateException("Wrong method used for Hdf5File");
		}
		if (prefix.contains("/")) {
			throw new IllegalArgumentException("Prefix for backup cannot contain '/'");
		}
		
		return m_parent.copyObject(m_name, m_parent, getUniqueName(Arrays.asList(m_parent.loadObjectNames()), prefix + m_name));
	}
	
	/**
	 * Creates a new attribute as a child of this treeElement.
	 * 
	 * @param name name of the attribute
	 * @param dimension number of values that fit in the (one-dimensional)
	 * 	attribute
	 * @param type the data type for the attribute
	 * @return the newly created attribute
	 * @throws IOException if an internal error occurred while creating
	 */
	public Hdf5Attribute<?> createAttribute(final String name, final long dimension, final Hdf5DataType type) throws IOException {
		if (!existsAttribute(name)) {
			return Hdf5Attribute.createAttribute(this, name, dimension, type);
			
		} else {
			throw new IOException("There is already an attribute with the name \""
					+ name + "\" in treeElement \"" + getPathFromFileWithName() + "\"");
		}
	}
	
	/**
	 * Creates a new attribute as a child of this treeElement using an edit.
	 * 
	 * @param edit information about name and data type for the attribute creation
	 * @param dimension number of values that fit in the (one-dimensional)
	 * 	attribute
	 * @return the newly created attribute
	 * @throws IOException if an internal error occurred while creating
	 */
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
	
	/**
	 * Creates a new attribute as a child of this treeElement and writes
	 * the input values in it.
	 * 
	 * @param name name of the attribute
	 * @param values values for the attribute (see
	 * 	{@linkplain HdfDataType#getHdfDataType(Object[], boolean)}
	 * 	for more information on the data type)
	 * @param unsigned if the data type is unsigned
	 * @return the newly created attribute
	 * @throws IOException if an internal error occurred while creating
	 */
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
	
	/**
	 * Creates a new attribute as a child of this treeElement using an edit
	 * and writes values of the flow variable in it.
	 * 
	 * @param edit information about name and data type for the attribute creation
	 * @param flowVariable the flow variable that should be used for the values
	 * @return the newly created attribute
	 * @throws IOException if an internal error occurred while creating
	 */
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
	
	/**
	 * Copies the copyAttribute as a child of this treeElement.
	 * 
	 * @param copyAttribute the attribute that should be copied
	 * @param newName the name of the new attribute
	 * @return the newly created attribute
	 * @throws IOException if an internal error occurred while copying
	 */
	public Hdf5Attribute<?> copyAttribute(Hdf5Attribute<?> copyAttribute, String newName) throws IOException {
		Hdf5Attribute<?> newAttribute = null;
		
		try {
			newAttribute = createAttribute(newName, copyAttribute.getDimension(), Hdf5DataType.createCopyFrom(copyAttribute.getType()));
			
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

	/**
	 * Copies the copyAttribute as a child of this treeElement using an edit.
	 * 
	 * @param edit information about the name and data type for the attribute creation
	 * @param copyAttribute the flow variable that should be used for the values
	 * @return the newly created attribute
	 * @throws IOException if an internal error occurred while creating
	 */
	public Hdf5Attribute<?> copyAttribute(AttributeNodeEdit edit, Hdf5Attribute<?> copyAttribute) throws IOException {
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
	
	/**
	 * @param name the name of the attribute
	 * @return the open attribute with the input name if it exists
	 * @throws IOException if an internal error occurred
	 */
	public Hdf5Attribute<?> getAttribute(final String name) throws IOException {
		Hdf5Attribute<?> attribute = null;
		
		synchronized (m_attributes) {
			// check if the attribute has already been loaded and update it if necessary
			for (Hdf5Attribute<?> attr : getAttributes()) {
				if (attr.getName().equals(name)) {
					attribute = attr;
					attribute.updateAttribute();
					attribute.open();
					break;
				}
			}
			
			if (attribute == null) {
				// load the attribute in the list of child attributes
				if (existsAttribute(name)) {
					attribute = Hdf5Attribute.openAttribute(this, name);
					
				} else {
					throw new IOException("Attribute \"" + name + "\" in \"" + getPathFromFileWithName() + "\" does not exist");
				}
			}
		}
		
		return attribute;
	}
	
	/**
	 * The path from file with name for attributes is different from those for
	 * treeElements because '/' is allowed in the name of attributes:
	 * <br>
	 * The separator in the path is still '/'.
	 * <br>
	 * The '/' in the name are changed to '\/' for the concatenation of path and name.
	 * 
	 * @param path the path from file to the attribute
	 * @return the open attribute if it exists
	 * @throws IOException if an internal error occurred
	 */
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
	
	public boolean existsAttribute(final String name) throws IOException {
		try {
			lockReadOpen();
			checkOpen();
			
			return H5.H5Aexists(m_elementId, name);
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
			throw new IOException("Existence of attribute \"" + name + "\" in treeElement \""
					+ getPathFromFileWithName() + "\" could not be checked: " + hlnpe.getMessage(), hlnpe);
			
		} finally {
        	unlockReadOpen();
        }
	}
	
	/**
	 * <b>Note:</b> Be careful that the instance of the old name is not usable anymore since
	 * the attribute in the hdf file does not exist anymore.
	 * 
	 * @param oldName the old name of the child attribute
	 * @param newName the new name of the child attribute
	 * @return the new attribute
	 * @throws IOException if an internal error occurred
	 */
	public Hdf5Attribute<?> renameAttribute(String oldName, String newName) throws IOException {
		boolean success = false;
		
		try {
			if (existsAttribute(newName)) {
				throw new IOException("Attribute in destination already exists");
			}
			
			Hdf5Attribute<?> attribute = getAttribute(oldName);
			attribute.close();
			success = H5.H5Arename(m_elementId, oldName, newName) >= 0;
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
	 * <b>Note:</b> Be careful that the instance of the name is not usable anymore since
	 * the attribute in the hdf file does not exist anymore.
	 * 
	 * @param name the name of the attribute to delete
	 * @return {@code true} if the deletion form the hdf file was successful
	 * @throws IOException if an internal error occurred
	 */
	public boolean deleteAttribute(final String name) throws IOException {
		boolean success = false;
		
		try {
			Hdf5Attribute<?> attribute = getAttribute(name);
			attribute.close();
			success = !attribute.isOpen() ? H5.H5Adelete(m_elementId, name) >= 0 : false;
			if (success) {
				removeAttribute(attribute);
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("Attribute \"" + getPathFromFileWithName(true) + name
					+ "\" could not be deleted from disk: " + hlionpe.getMessage(), hlionpe);
		}
		
		return success;
	}
	
	void addAttribute(Hdf5Attribute<?> attribute) {
		synchronized (m_attributes) {
			m_attributes.add(attribute);
			attribute.setPathFromFile(getPathFromFileWithName(true));
			attribute.setParent(this);
		}
	}
	
	private boolean removeAttribute(Hdf5Attribute<?> attribute) {
		synchronized (m_attributes) {
			attribute.setParent(null);
			attribute.setPathFromFile("");
			attribute.setAttributeId(-1);
			return m_attributes.remove(attribute);
		}
	}
	
	/**
	 * @return an array of all names of child attributes
	 * @throws IOException if an internal error occurred
	 */
	public String[] loadAttributeNames() throws IOException {
		String[] attrNames = null;
		Hdf5TreeElement treeElement = isFile() ? this : getParent();
		String name = isFile() ? "/" : getName();
		
		try {
			treeElement.lockReadOpen();
			treeElement.checkOpen();
			
			int numAttrs = (int) H5.H5Oget_info(m_elementId).num_attrs;
			attrNames = new String[numAttrs];
			for (int i = 0; i < numAttrs; i++) {
				attrNames[i] = H5.H5Aget_name_by_idx(treeElement.getElementId(), name,
						HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC, i,
						HDF5Constants.H5P_DEFAULT);
			}
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("List of attributes could not be loaded: " + hlionpe.getMessage(), hlionpe);
			
		} finally {
			treeElement.unlockReadOpen();
		}
		
		return attrNames;
	}
	
	/**
	 * @return all descendant attributes mapped to their data type
	 * @throws IOException if an internal error occurred
	 * @throws IllegalStateException if this treeElement is not open
	 */
	public Map<String, Hdf5DataType> getAllAttributesInfo() throws IOException, IllegalStateException {
		Map<String, Hdf5DataType> paths = new LinkedHashMap<>();
		
		/*
		 * TODO cannot use checkOpen() and m_openLock here since there might be deadlocks then
		 * (tested on 4 readers and 4 writers executed in parallel) 
		 */
		if (isOpen()) {
			String path = getPathFromFileWithName(true);
			
			for (String name : loadAttributeNames()) {
				// there might be some attributes with unsupported dataTypes which should be ignored
				try {
					Hdf5DataType dataType = findAttributeType(name);
					name = name.replaceAll("/", "\\\\/");
					paths.put(path + name, dataType);
					
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger(getClass()).warn("Attribute \"" + path + name
							+ "\" could not be loaded: " + udte.getMessage());
					
				} catch (IOException | NullPointerException ionpe) {
					NodeLogger.getLogger(getClass()).error("Attribute \"" + path + name
							+ "\" could not be loaded: " + ionpe.getMessage(), ionpe);
				}
			}
			
			if (isGroup()) {
				for (String name : ((Hdf5Group) this).loadDataSetNames()) {
					try {
						Hdf5DataSet<?> dataSet = ((Hdf5Group) this).getDataSet(name);
						paths.putAll(dataSet.getAllAttributesInfo());
						
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger(getClass()).warn("DataSet \"" + path + name
								+ "\" could not be loaded: " + udte.getMessage(), udte);
					}
				}
			
				for (String name : ((Hdf5Group) this).loadGroupNames()) {
					Hdf5Group group = ((Hdf5Group) this).getGroup(name);
					paths.putAll(group.getAllAttributesInfo());
				}
			}
		} else {
			throw new IllegalStateException("\"" + getPathFromFileWithName() + "\" is not open");
		}
		
		return paths;
	}
	
	/**
	 * @param objectId only ATTRIBUTE and DATASET allowed, DATASET only for
	 * 	instances of {@linkplain Hdf5Group}
	 * @return the specification of all descendant attributes or dataSets
	 * @throws IllegalStateException if a descendant is not open
	 */
	protected DataTableSpec createSpecOfObjects(int objectId) throws IllegalStateException {
		List<DataColumnSpec> colSpecList = new ArrayList<>();
		
		try {
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
						NodeLogger.getLogger(getClass()).error(udte.getMessage(), udte);
					}
				}	
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error("Specs could not be loaded: " + ioe.getMessage(), ioe);
		}
		
		return new DataTableSpec(colSpecList.toArray(new DataColumnSpec[] {}));
	}
	
	/**
	 * @return the specification of all descendant attributes
	 * @throws IllegalStateException if a descendant is not open
	 */
	public DataTableSpec createSpecOfAttributes() throws IllegalStateException {
		return createSpecOfObjects(HDF5Constants.H5I_ATTR);
	}
	
	/**
	 * @param name name of the child attribute of this treeElement
	 * @throws UnsupportedDataTypeException if the data type is not supported
	 * 	(for {@code HDF5Constants.H5T_VLEN, HDF5Constants.H5T_REFERENCE})
	 * @throws IOException if an internal error occurred
	 */
	Hdf5DataType findAttributeType(String name) throws UnsupportedDataTypeException, IOException {
		Hdf5DataType dataType = null;
		long attributeId = -1;
		
		try {
			lockReadOpen();
			checkOpen();
			
			if (existsAttribute(name)) {
				attributeId = H5.H5Aopen(m_elementId, name, HDF5Constants.H5P_DEFAULT);
				dataType = Hdf5DataType.openDataType(attributeId);
			} else {
				throw new IllegalArgumentException("Attribute \"" + name + "\" in treeElement \""
						+ getPathFromFileWithName() + "\" does not exist");
			}
		} catch (UnsupportedDataTypeException udte) {
			throw udte;
			
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("DataType for attribute \"" + name + "\" in treeElement \""
					+ getPathFromFileWithName() + "\" could not be created: " + hlionpe.getMessage(), hlionpe);
			
		} finally {
			unlockReadOpen();
			
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
			
	}
	
	/**
	 * Opens this treeElement and all of its ancestors (if necessary).
	 * 
	 * @return {@code true} if the treeElement is opened
	 * @throws IOException if an internal error occurred
	 */
	public abstract boolean open() throws IOException;

	/**
	 * Closes this treeElement and all of its descendants (if necessary).
	 * 
	 * @return {@code true} if the treeElement is closed
	 * @throws IOException if an internal error occurred
	 */
	public abstract boolean close() throws IOException;
}

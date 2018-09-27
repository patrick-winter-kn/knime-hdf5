package org.knime.hdf5.nodes.writer.edit;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.TransferHandler;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.DataTypeChooser;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;

public class DataSetNodeEdit extends TreeNodeEdit {

	private static final String COLUMN_PROPERTY_NAMES = "knime.columnnames";
	
	private static final String COLUMN_PROPERTY_TYPES = "knime.columntypes";

	private HdfDataType m_inputType;
	
	private EditDataType m_editDataType = new EditDataType();
	
	private int m_numberOfDimensions;
	
	private long m_inputRowCount = ColumnNodeEdit.UNKNOWN_ROW_COUNT;
	
	private int m_compressionLevel;
	
	private long m_chunkRowSize = 1L;
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();

	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	public DataSetNodeEdit(GroupNodeEdit parent, String name) {
		this(parent, null, name, EditOverwritePolicy.NONE, EditAction.CREATE);
	}
	
	private DataSetNodeEdit(GroupNodeEdit parent, DataSetNodeEdit copyDataSet, boolean noAction) {
		this(parent, copyDataSet.getInputPathFromFileWithName(), copyDataSet.getName(), copyDataSet.getEditOverwritePolicy(),
				noAction ? copyDataSet.getEditAction() : (copyDataSet.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY));
		copyAdditionalPropertiesFrom(copyDataSet);
		if (getEditAction() == EditAction.COPY) {
			copyDataSet.addIncompleteCopy(this);
		}
	}

	public DataSetNodeEdit(GroupNodeEdit parent, Hdf5DataSet<?> dataSet) {
		this(parent, dataSet.getPathFromFileWithName(), dataSet.getName(), EditOverwritePolicy.NONE, EditAction.NO_ACTION);
		Hdf5HdfDataType hdfType = dataSet.getType().getHdfType();
		m_inputType = hdfType.getType();
		m_editDataType.setValues(m_inputType, hdfType.getEndian(), Rounding.DOWN, false, (int) hdfType.getStringLength());
		m_numberOfDimensions = dataSet.getDimensions().length;
		m_numberOfDimensions = m_numberOfDimensions > 2 ? 2 : (m_numberOfDimensions < 1 ? 1 : m_numberOfDimensions);
		m_compressionLevel = dataSet.getCompressionLevel();
		m_chunkRowSize = (int) dataSet.getChunkRowSize();
		setHdfObject(dataSet);
	}
	
	DataSetNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name, EditOverwritePolicy editOverwritePolicy, EditAction editAction) {
		super(inputPathFromFileWithName, !(parent instanceof FileNodeEdit) ? parent.getOutputPathFromFileWithName() : "", name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new DataSetNodeMenu());
		m_editDataType.setEndian(Endian.LITTLE_ENDIAN);
		parent.addDataSetNodeEdit(this);
	}
	
	public DataSetNodeEdit copyDataSetEditTo(GroupNodeEdit parent, boolean copyWithoutChildren) {
		DataSetNodeEdit newDataSetEdit = new DataSetNodeEdit(parent, this, copyWithoutChildren);
		newDataSetEdit.addEditToParentNode();

		for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
			if (!copyWithoutChildren || columnEdit.getEditAction().isCreateOrCopyAction()) {
				columnEdit.copyColumnEditTo(newDataSetEdit, false);
			}
		}
		
		for (AttributeNodeEdit attributeEdit : getAttributeNodeEdits()) {
			if (!copyWithoutChildren || attributeEdit.getEditAction().isCreateOrCopyAction()) {
				attributeEdit.copyAttributeEditTo(newDataSetEdit, false);
			}
		}
		
		return newDataSetEdit;
	}
	
	public HdfDataType getInputType() {
		return m_inputType;
	}
	
	public EditDataType getEditDataType() {
		return m_editDataType;
	}

	public int getNumberOfDimensions() {
		return m_numberOfDimensions;
	}

	private void setNumberOfDimensions(int numberOfDimensions) {
		m_numberOfDimensions = numberOfDimensions;
	}
	
	public long getInputRowCount() {
		return m_inputRowCount;
	}
	
	void setInputRowCount(long inputRowCount) {
		m_inputRowCount = inputRowCount;
	}

	public int getCompressionLevel() {
		return m_compressionLevel;
	}

	private void setCompressionLevel(int compressionLevel) {
		m_compressionLevel = compressionLevel;
	}

	public long getChunkRowSize() {
		return m_chunkRowSize;
	}

	private void setChunkRowSize(long chunkRowSize) {
		m_chunkRowSize = chunkRowSize;
	}

	private boolean havePropertiesChanged() {
		boolean propertiesChanged = true;
		
		if (getInputPathFromFileWithName() != null) {
			try {
				Hdf5DataSet<?> copyDataSet = ((Hdf5File) getRoot().getHdfObject()).getDataSetByPath(getInputPathFromFileWithName());
				
				propertiesChanged = m_inputType != m_editDataType.getOutputType()
						|| copyDataSet.getType().getHdfType().getEndian() != m_editDataType.getEndian()
						|| copyDataSet.getType().getHdfType().getStringLength() != m_editDataType.getStringLength()
						|| copyDataSet.getDimensions().length != m_numberOfDimensions
						|| copyDataSet.getCompressionLevel() != m_compressionLevel
						|| copyDataSet.getChunkRowSize() != m_chunkRowSize
						|| copyDataSet.numberOfColumns() != getColumnInputTypes().length;
				
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
		}
		
		return propertiesChanged;
	}
	
	public HdfDataType[] getColumnInputTypes() {
		List<HdfDataType> hdfTypes = new ArrayList<>();
		for (ColumnNodeEdit edit : m_columnEdits) {
			if (edit.getEditAction() != EditAction.DELETE) {
				hdfTypes.add(edit.getInputType());
			}
		}
		return hdfTypes.toArray(new HdfDataType[] {});
	}
	
	public ColumnNodeEdit[] getColumnNodeEdits() {
		return m_columnEdits.toArray(new ColumnNodeEdit[] {});
	}	

	public AttributeNodeEdit[] getAttributeNodeEdits() {
		return m_attributeEdits.toArray(new AttributeNodeEdit[] {});
	}

	public void addColumnNodeEdit(ColumnNodeEdit edit) {
		m_columnEdits.add(edit);
		edit.setParent(this);
		considerColumnNodeEdit(edit);
	}
	
	void considerColumnNodeEdit(ColumnNodeEdit edit) {
		if (edit.getEditAction() != EditAction.DELETE) {
			m_inputType = m_inputType != null && edit.getInputType().getPossiblyConvertibleHdfTypes().contains(m_inputType) ? m_inputType : edit.getInputType();
			if (!m_inputType.getPossiblyConvertibleHdfTypes().contains(m_editDataType.getOutputType())) {
				m_editDataType.setOutputType(m_inputType);
				setEditAction(EditAction.MODIFY);
			}
			m_inputRowCount = m_inputRowCount == ColumnNodeEdit.UNKNOWN_ROW_COUNT ? edit.getInputRowCount() : m_inputRowCount;
		}
	}

	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
		edit.setParent(this);
	}
	
	ColumnNodeEdit getColumnNodeEdit(String inputPathFromFileWithName, int inputColumnIndex) {
		if (inputPathFromFileWithName != null) {
			for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
				if (inputPathFromFileWithName.equals(columnEdit.getInputPathFromFileWithName())
						&& inputColumnIndex == columnEdit.getInputColumnIndex() && !columnEdit.getEditAction().isCreateOrCopyAction()) {
					return columnEdit;
				}
			}
		}
		return null;
	}
	
	AttributeNodeEdit getAttributeNodeEdit(String inputPathFromFileWithName, EditAction editAction) {
		if (inputPathFromFileWithName != null) {
			for (AttributeNodeEdit attributeEdit : m_attributeEdits) {
				if (inputPathFromFileWithName.equals(attributeEdit.getInputPathFromFileWithName())
						&& (editAction == EditAction.CREATE) == (attributeEdit.getEditAction() == EditAction.CREATE)) {
					return attributeEdit;
				}
			}
		}
		return null;
	}

	public void removeColumnNodeEdit(ColumnNodeEdit edit) {
		m_columnEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		disconsiderColumnNodeEdit(edit);
	}
	
	void disconsiderColumnNodeEdit(ColumnNodeEdit edit) {
		if (edit.getEditAction() != EditAction.DELETE) {
			// TODO find better algorithm here
			for (HdfDataType possibleInputType : HdfDataType.values()) {
				boolean convertible = true;
				for (HdfDataType inputType : getColumnInputTypes()) {
					if (!inputType.getPossiblyConvertibleHdfTypes().contains(possibleInputType)) {
						convertible = false;
						break;
					}
				}
				if (convertible) {
					m_inputType = possibleInputType;
					break;
				}
			}
			
			long inputRowCount = ColumnNodeEdit.UNKNOWN_ROW_COUNT;
			for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
				if (columnEdit.getEditAction() != EditAction.DELETE && columnEdit != edit
						&& columnEdit.getInputRowCount() != ColumnNodeEdit.UNKNOWN_ROW_COUNT) {
					inputRowCount = columnEdit.getInputRowCount();
					break;
				}
			}
			m_inputRowCount = inputRowCount;
		}
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
	}
	
	private void reorderColumnEdits(List<ColumnNodeEdit> edits) {
		List<ColumnNodeEdit> newEdits = new ArrayList<>();
		for (ColumnNodeEdit columnEdit : edits) {
			newEdits.add(columnEdit);
			if (columnEdit != m_columnEdits.get(newEdits.size()-1)) {
				columnEdit.setEditAction(EditAction.MODIFY);
			}
		}
		for (ColumnNodeEdit columnEdit : m_columnEdits) {
			if (columnEdit.getEditAction() == EditAction.DELETE) {
				newEdits.add(columnEdit);
			}
		}
		
		m_columnEdits.clear();
		m_columnEdits.addAll(newEdits);
	}
	
	private boolean isOneDimensionPossible() {
		int editCount = 0;
		for (ColumnNodeEdit edit : m_columnEdits) {
			if (edit.getEditAction() != EditAction.DELETE) {
				editCount++;
			}
		}
		return editCount <= 1;
	}
	
	/**
	 * so far with overwrite of properties
	 */
	void integrate(DataSetNodeEdit copyEdit, long inputRowCount, boolean lastValidationBeforeExecution) {
		// TODO changing the order of the columns is not supported yet
		for (ColumnNodeEdit copyColumnEdit : copyEdit.getColumnNodeEdits()) {
			if (copyColumnEdit.getEditAction().isCreateOrCopyAction()) {
				copyColumnEdit.copyColumnEditTo(this, false);
				if (copyColumnEdit.getEditAction() == EditAction.CREATE) {
					copyColumnEdit.setInputRowCount(inputRowCount);
				}
			} else {
				ColumnNodeEdit columnEdit = getColumnNodeEdit(copyColumnEdit.getInputPathFromFileWithName(), copyColumnEdit.getInputColumnIndex());
				if (columnEdit != null && copyColumnEdit.getEditAction() != EditAction.NO_ACTION) {
					removeColumnNodeEdit(columnEdit);
					copyColumnEdit.copyColumnEditTo(this, !columnEdit.getEditAction().isCreateOrCopyAction());
				}
			}
		}
		
		for (AttributeNodeEdit copyAttributeEdit : copyEdit.getAttributeNodeEdits()) {
			if (copyAttributeEdit.getEditAction() != EditAction.NO_ACTION) {
				if (lastValidationBeforeExecution && useOverwritePolicy(copyAttributeEdit, inputRowCount)) {
					continue;
				}
				AttributeNodeEdit attributeEdit = getAttributeNodeEdit(copyAttributeEdit.getInputPathFromFileWithName(), copyAttributeEdit.getEditAction());
				boolean isCreateOfCopyAction = copyAttributeEdit.getEditAction().isCreateOrCopyAction();
				if (attributeEdit != null && !isCreateOfCopyAction) {
					attributeEdit.copyPropertiesFrom(copyAttributeEdit);
				} else {
					copyAttributeEdit.copyAttributeEditTo(this, !isCreateOfCopyAction);
				}
			}
		}
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		if (copyEdit instanceof DataSetNodeEdit) {
			DataSetNodeEdit copyDataSetEdit = (DataSetNodeEdit) copyEdit;
			m_inputType = copyDataSetEdit.getInputType();
			m_editDataType.setValues(copyDataSetEdit.getEditDataType());
			m_numberOfDimensions = copyDataSetEdit.getNumberOfDimensions();
			m_compressionLevel = copyDataSetEdit.getCompressionLevel();
			m_chunkRowSize = copyDataSetEdit.getChunkRowSize();
		}
	}
	
	@Override
	public String getToolTipText() {
		return "(" + m_editDataType.getOutputType().toString() + ") " + super.getToolTipText();
	}

	@Override
	protected TreeNodeEdit[] getAllChildren() {
		List<TreeNodeEdit> children = new ArrayList<>();
		
		children.addAll(m_columnEdits);
		children.addAll(m_attributeEdits);
		
		return children.toArray(new TreeNodeEdit[0]);
	}

	@Override
	protected void removeFromParent() {
		((GroupNodeEdit) getParent()).removeDataSetNodeEdit(this);
		setParent(null);
	}
	
	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);
		
		m_editDataType.saveSettingsTo(settings);
		m_numberOfDimensions = isOneDimensionPossible() ? m_numberOfDimensions : 2;
		settings.addInt(SettingsKey.NUMBER_OF_DIMENSIONS.getKey(), m_numberOfDimensions);
		settings.addInt(SettingsKey.COMPRESSION.getKey(), m_compressionLevel);
		settings.addLong(SettingsKey.CHUNK_ROW_SIZE.getKey(), m_chunkRowSize);
		
	    NodeSettingsWO columnSettings = settings.addNodeSettings(SettingsKey.COLUMNS.getKey());
	    NodeSettingsWO attributeSettings = settings.addNodeSettings(SettingsKey.ATTRIBUTES.getKey());
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        NodeSettingsWO editSettings = columnSettings.addNodeSettings("" + edit.hashCode());
			edit.saveSettingsTo(editSettings);
		}
	    
		for (AttributeNodeEdit edit : m_attributeEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
		        NodeSettingsWO editSettings = attributeSettings.addNodeSettings("" + edit.hashCode());
				edit.saveSettingsTo(editSettings);
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
	        if (!getEditAction().isCreateOrCopyAction()) {
				Hdf5Group parent = (Hdf5Group) getParent().getHdfObject();
				if (parent != null) {
		        	setHdfObject(parent.getDataSet(getName()));
				}
	        }
		} catch (IOException ioe) {
			// nothing to do here: edit will be invalid anyway
		}

		m_editDataType.loadSettingsFrom(settings);
		setNumberOfDimensions(settings.getInt(SettingsKey.NUMBER_OF_DIMENSIONS.getKey()));
		setCompressionLevel(settings.getInt(SettingsKey.COMPRESSION.getKey()));
		setChunkRowSize(settings.getLong(SettingsKey.CHUNK_ROW_SIZE.getKey()));
		
		NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
		Enumeration<NodeSettingsRO> columnEnum = columnSettings.children();
		while (columnEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = columnEnum.nextElement();
			ColumnNodeEdit edit = new ColumnNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
					editSettings.getInt(SettingsKey.INPUT_COLUMN_INDEX.getKey()), editSettings.getString(SettingsKey.NAME.getKey()),
					HdfDataType.get(editSettings.getInt(SettingsKey.INPUT_TYPE.getKey())), editSettings.getLong(SettingsKey.INPUT_ROW_COUNT.getKey()),
					EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
			edit.loadSettingsFrom(editSettings);
		}

		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			AttributeNodeEdit edit = new AttributeNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
					editSettings.getString(SettingsKey.NAME.getKey()), EditOverwritePolicy.values()[editSettings.getInt(SettingsKey.EDIT_OVERWRITE_POLICY.getKey())],
					HdfDataType.get(editSettings.getInt(SettingsKey.INPUT_TYPE.getKey())), EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
			edit.loadSettingsFrom(editSettings);
        }
	}
	
	void loadChildrenOfHdfObject() throws IOException {
		Hdf5DataSet<?> dataSet = (Hdf5DataSet<?>) getHdfObject();
		HdfDataType dataType = dataSet.getType().getHdfType().getType();

		long rowCount = dataSet.getDimensions().length >= 1 ? dataSet.getDimensions()[0] : 1;
		if (dataSet.getDimensions().length == 2) {
			if (dataSet.getDimensions()[1] > Integer.MAX_VALUE) {
				NodeLogger.getLogger(getClass()).warn("Number of columns of dataSet \"" + dataSet.getPathFromFileWithName()
						+ "\" has overflown the Integer values.");
			}
			int colCount = (int) dataSet.getDimensions()[1];
			for (int i = 0; i < colCount; i++) {
				ColumnNodeEdit columnEdit = new ColumnNodeEdit(this, i, "col" + (i+1), dataType, rowCount);
				columnEdit.addEditToParentNode();
			}
		} else if (dataSet.getDimensions().length < 2) {
			ColumnNodeEdit columnEdit = new ColumnNodeEdit(this, 0, "col", dataType, rowCount);
			columnEdit.addEditToParentNode();
			
		} else {
			NodeLogger.getLogger(getClass()).warn("DataSet \"" + dataSet.getPathFromFileWithName()
					+ "\" could not be loaded  (has more than 2 dimensions (" + dataSet.getDimensions().length + "))");
		}
		
    	try {
    		for (String attributeName : dataSet.loadAttributeNames()) {
    			Hdf5Attribute<?> child = dataSet.updateAttribute(attributeName);
    			AttributeNodeEdit childEdit = new AttributeNodeEdit(this, child);
    			childEdit.addEditToParentNode();
    		}
    	} catch (NullPointerException npe) {
    		throw new IOException(npe.getMessage());
    	}
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		return getName().contains("/") ? InvalidCause.NAME_CHARS : null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return (edit instanceof DataSetNodeEdit || edit instanceof GroupNodeEdit) && this != edit && getName().equals(edit.getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE
				&& (!(edit instanceof DataSetNodeEdit) || getEditOverwritePolicy() == EditOverwritePolicy.NONE && edit.getEditOverwritePolicy() == EditOverwritePolicy.NONE
						|| getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE && edit.getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		DataTableSpec tableSpec = inputTable != null ? inputTable.getDataTableSpec() : null;
		List<ColumnNodeEdit> copyColumnEditList = new ArrayList<>();
		Hdf5File file = (Hdf5File) getRoot().getHdfObject();
		
		int[] specIndices = new int[getColumnInputTypes().length];
		for (int i = 0; i < specIndices.length; i++) {
			ColumnNodeEdit edit = getColumnNodeEdits()[i];
			if (edit.getEditAction() == EditAction.CREATE) {
				specIndices[i] = tableSpec.findColumnIndex(edit.getName());
			} else {
				specIndices[i] = -1;
				copyColumnEditList.add(edit);
			}
		}
		
		ColumnNodeEdit[] copyColumnEdits = copyColumnEditList.toArray(new ColumnNodeEdit[0]);
		Hdf5DataSet<?>[] copyDataSets = new Hdf5DataSet<?>[copyColumnEdits.length];
		for (int i = 0; i < copyDataSets.length; i++) {
			try {
				copyDataSets[i] = file.getDataSetByPath(copyColumnEdits[i].getInputPathFromFileWithName());
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
		}

		Hdf5Group parent = (Hdf5Group) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		if (inputTable != null) {
			m_inputRowCount = inputTable.size();
			for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
				if (columnEdit.getEditAction() == EditAction.CREATE) {
					columnEdit.setInputRowCount(m_inputRowCount);
				}
			}
		}
		
		Hdf5DataSet<Object> dataSet = null;
		try{
			dataSet = (Hdf5DataSet<Object>) parent.createDataSetFromEdit(this);
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		setHdfObject(dataSet);
		boolean success = dataSet != null;
		if (success) {
			CloseableRowIterator iter = inputTable != null ? inputTable.iterator() : null;
			try {
				for (long rowIndex = 0; rowIndex < m_inputRowCount; rowIndex++) {
					DataRow row = iter != null ? iter.next() : null;
					
					Object[] copyValues = new Object[copyDataSets.length];
					for (int i = 0; i < copyValues.length; i++) {
						copyValues[i] = copyDataSets[i].readRow(rowIndex)[copyColumnEdits[i].getInputColumnIndex()];
					}
					
					success &= dataSet.writeRowToDataSet(row, specIndices, rowIndex, copyValues, m_editDataType.getRounding(), m_editDataType.getStandardValue());
				}
			} catch (HDF5DataspaceInterfaceException | UnsupportedDataTypeException hdiudte) {
				NodeLogger.getLogger(getClass()).error(hdiudte.getMessage(), hdiudte);
			}
			
			if (iter != null) {
				iter.close();
			}
			
			if (saveColumnProperties) {
				String[] columnNames = new String[specIndices.length];
				String[] columnTypes = new String[specIndices.length];
				for (int i = 0; i < columnNames.length; i++) {
					DataColumnSpec spec = tableSpec.getColumnSpec(specIndices[i]);
					columnNames[i] = spec.getName();
					columnTypes[i] = spec.getType().getName();
				}

				try {
					success &= dataSet.createAndWriteAttribute(COLUMN_PROPERTY_NAMES, columnNames);
					success &= dataSet.createAndWriteAttribute(COLUMN_PROPERTY_TYPES, columnTypes);
				} catch (IOException ioe) {
					NodeLogger.getLogger("HDF5 Files").warn("Property attributes of dataSet \"" + dataSet.getPathFromFileWithName() + "\" could not be written completely");
				}
			}
		}
		
		return success;
	}

	@Override
	protected boolean copyAction() {
		if (!havePropertiesChanged()) {
			try {
				Hdf5DataSet<?> oldDataSet = ((Hdf5File) getRoot().getHdfObject()).getDataSetByPath(getInputPathFromFileWithName());
				
				Hdf5Group newParent = (Hdf5Group) getParent().getHdfObject();
				if (!newParent.isFile()) {
					newParent.open();
				}
				
				Hdf5DataSet<?> newDataSet = (Hdf5DataSet<?>) oldDataSet.getParent().copyObject(oldDataSet.getName(), newParent, getName());
				setHdfObject(newDataSet);
				return newDataSet != null;
				
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
			
			return false;
			
		} else {
			return createAction(null, null, false);
		}
	}

	@Override
	protected boolean deleteAction() {
		boolean success = false;
		
		Hdf5Group parent = (Hdf5Group) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		
		String name = getInputPathFromFileWithName().substring(getInputPathFromFileWithName().lastIndexOf("/") + 1);
		try {
			success = parent.deleteObject(name) >= 0;
			if (success) {
				setHdfObject((Hdf5DataSet<?>) null);
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		return success;
	}

	@Override
	protected boolean modifyAction() {
		if (!havePropertiesChanged()) {
			try {
				Hdf5DataSet<?> oldDataSet = ((Hdf5File) getRoot().getHdfObject()).getDataSetByPath(getInputPathFromFileWithName());
				
				Hdf5Group newParent = (Hdf5Group) getParent().getHdfObject();
				if (!newParent.isFile()) {
					newParent.open();
				}
				
				Hdf5DataSet<?> newDataSet = (Hdf5DataSet<?>) oldDataSet.getParent().moveObject(oldDataSet.getName(), newParent, getName());
				setHdfObject(newDataSet);
				return newDataSet != null;
				
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
			
			return false;
			
		} else {
			// TODO do it like in AttributeNodeEdit when it really works there in all cases
			return true;
		}
	}
	
	public class DataSetNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = -6418394582185524L;
    	
		private DataSetNodeMenu() {
			super(true, false, true);
    	}
		
		@Override
		protected PropertiesDialog getPropertiesDialog() {
			return new DataSetPropertiesDialog();
		}

		@Override
		protected void onDelete() {
			DataSetNodeEdit edit = DataSetNodeEdit.this;
			TreeNodeEdit parentOfVisible = edit.getParent();
        	edit.setDeletion(edit.getEditAction() != EditAction.DELETE);
            parentOfVisible.reloadTreeWithEditVisible(true);
		}
		
		private class DataSetPropertiesDialog extends PropertiesDialog {
	    	
			private static final long serialVersionUID = -9060929832634560737L;

			private JTextField m_nameField = new JTextField(15);
			private JComboBox<EditOverwritePolicy> m_overwriteField = new JComboBox<>(EditOverwritePolicy.values());
			private DataTypeChooser m_dataTypeChooser = m_editDataType.new DataTypeChooser(true);
			private JCheckBox m_useOneDimensionField = new JCheckBox();
			private JCheckBox m_compressionCheckBox;
			private JSpinner m_compressionField = new JSpinner(new SpinnerNumberModel(0, 0, 9, 1));
			private JSpinner m_chunkField = new JSpinner(new SpinnerNumberModel((Long) 1L, (Long) 1L, (Long) Long.MAX_VALUE, (Long) 1L));
			private JList<ColumnNodeEdit> m_columnList = new JList<>(new DefaultListModel<>());
	    	
			private DataSetPropertiesDialog() {
				super(DataSetNodeMenu.this, "DataSet properties");

				addProperty("Name: ", m_nameField);
				addProperty("Overwrite: ", m_overwriteField);
				m_dataTypeChooser.addToPropertiesDialog(this);
				addProperty("Create dataSet with one dimension: ", m_useOneDimensionField);
				
				m_compressionField.setEnabled(false);
				m_compressionCheckBox = addProperty("Compression: ", m_compressionField, new ChangeListener() {

					@Override
					public void stateChanged(ChangeEvent e) {
						boolean selected = m_compressionCheckBox.isSelected();
						m_compressionField.setEnabled(selected);
						m_chunkField.setEnabled(selected);
					}
				});
				m_chunkField.setEnabled(false);
				addProperty("Chunk row size: ", m_chunkField);
				
				DefaultListModel<ColumnNodeEdit> editModel = (DefaultListModel<ColumnNodeEdit>) m_columnList.getModel();
				editModel.clear();
				for (ColumnNodeEdit edit : getColumnNodeEdits()) {
					editModel.addElement(edit);
				}
				
				m_columnList.setVisibleRowCount(-1);
				m_columnList.setCellRenderer(new DefaultListCellRenderer() {

					private static final long serialVersionUID = -3499393207598804129L;

					@Override
					public Component getListCellRendererComponent(JList<?> list,
							Object value, int index, boolean isSelected,
							boolean cellHasFocus) {
						super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);

						if (value instanceof ColumnNodeEdit) {
							ColumnNodeEdit edit = (ColumnNodeEdit) value;
							
							setText(edit.getName());
							list.setToolTipText(edit.getToolTipText());
							Icon icon = null;
							try {
								icon = Hdf5KnimeDataType.getKnimeDataType(edit.getInputType(), true).getColumnDataType().getIcon();
							} catch (UnsupportedDataTypeException udte) {
								NodeLogger.getLogger(DataSetNodeEdit.class).warn(udte.getMessage(), udte);
							}
							setIcon(icon);
						}
						
						return this;
					}
				});
				
				m_columnList.setDragEnabled(true);
				m_columnList.setTransferHandler(new TransferHandler() {

					private static final long serialVersionUID = 1192996062714565516L;
					
					private int[] transferableIndices;

		            public int getSourceActions(JComponent comp) {
		                return MOVE;
		            }
		            
		            protected Transferable createTransferable(JComponent comp) {
		                if (comp instanceof JList) {
		                	JList<?> list = (JList<?>) comp;
		                	transferableIndices = list.getSelectedIndices();
		                }
		                return new StringSelection("");
		            }
		            
		            public boolean canImport(TransferHandler.TransferSupport info) {
		            	JList.DropLocation dl = (JList.DropLocation) info.getDropLocation();
		                return dl.getIndex() != -1;
		            }

		            // TODO also use exportDone()
					public boolean importData(TransferHandler.TransferSupport info) {
		                if (!info.isDrop()) {
		                    return false;
		                }
		                
		                JList.DropLocation dl = (JList.DropLocation) info.getDropLocation();
		                int startIndex = dl.getIndex();
		                int transferableCount = transferableIndices.length;
		                List<ColumnNodeEdit> edits = Collections.list(editModel.elements());
		                for (int i = 0; i < transferableCount; i++) {
			                editModel.add(startIndex + i, edits.get(transferableIndices[i]));
		                }
		                for (int i = transferableCount - 1; i >= 0; i--) {
		                	int index = transferableIndices[i];
		                	index += startIndex > index ? 0 : transferableCount;
		                	editModel.remove(index);
		                }
		                m_columnList.setModel(editModel);
		                
		                return true;
		            }
				});
				m_columnList.setDropMode(DropMode.INSERT);
				
				m_columnList.addMouseListener(new MouseAdapter() {
					
		    		@Override
		    		public void mousePressed(MouseEvent e) {
		    			if (e.isPopupTrigger()) {
		    				createMenu(e);
		    			}
		    		}
		    		
		    		@Override
		    		public void mouseReleased(MouseEvent e) {
		    			if (e.isPopupTrigger()) {
		    				createMenu(e);
		    			}
		    		}
		    		
		    		private void createMenu(MouseEvent e) {
		    			if (m_columnList.getModel().getSize() > 1) {
		    				int index = m_columnList.locationToIndex(e.getPoint());
			    			ColumnNodeEdit edit = m_columnList.getModel().getElementAt(index);
							if (edit != null) {
								m_columnList.setSelectedIndex(index);
								JPopupMenu menu = new JPopupMenu();
								JMenuItem itemDelete = new JMenuItem("Delete");
				    			itemDelete.addActionListener(new ActionListener() {
									
									@Override
									public void actionPerformed(ActionEvent e) {
										int dialogResult = JOptionPane.showConfirmDialog(menu,
												"Are you sure to delete this column", "Warning", JOptionPane.YES_NO_OPTION);
										if (dialogResult == JOptionPane.YES_OPTION){
											edit.setDeletion(true);
											((DefaultListModel<ColumnNodeEdit>) m_columnList.getModel()).remove(index);
											if (m_columnList.getModel().getSize() == 1) {
												m_useOneDimensionField.setEnabled(true);
											}
										}
									}
								});
				    			menu.add(itemDelete);
				    			menu.show(e.getComponent(), e.getX(), e.getY());
							}
		    			}
		    		}
				});
				
				final JScrollPane jsp = new JScrollPane(m_columnList);
				jsp.setPreferredSize(new Dimension(0, 100));
				addProperty("Columns: ", jsp, 1.0);
				
				pack();
			}
			
			@Override
			protected void loadFromEdit() {
				DataSetNodeEdit edit = DataSetNodeEdit.this;
				m_nameField.setText(edit.getName());
				m_overwriteField.setSelectedItem(edit.getEditOverwritePolicy());
				m_dataTypeChooser.loadFromDataType(edit.getInputType().getPossiblyConvertibleHdfTypes(), m_inputType == HdfDataType.FLOAT32);
				boolean oneDimensionPossible = isOneDimensionPossible();
				m_useOneDimensionField.setEnabled(oneDimensionPossible);
				m_useOneDimensionField.setSelected(oneDimensionPossible && m_numberOfDimensions == 1);
				m_compressionCheckBox.setSelected(edit.getCompressionLevel() > 0);
				m_compressionField.setValue(edit.getCompressionLevel());
				m_chunkField.setValue(edit.getChunkRowSize());
				
				DefaultListModel<ColumnNodeEdit> columnModel = (DefaultListModel<ColumnNodeEdit>) m_columnList.getModel();
				columnModel.clear();
				for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
					if (columnEdit.getEditAction() != EditAction.DELETE) {
						columnModel.addElement(columnEdit);
					}
				}
			}

			@Override
			protected void saveToEdit() {
				DataSetNodeEdit edit = DataSetNodeEdit.this;
				edit.setName(m_nameField.getText());
				edit.setEditOverwritePolicy((EditOverwritePolicy) m_overwriteField.getSelectedItem());
				m_dataTypeChooser.saveToDataType();
				edit.setNumberOfDimensions(m_useOneDimensionField.isSelected() ? 1 : 2);
				edit.setCompressionLevel(m_compressionField.isEnabled() ? (Integer) m_compressionField.getValue() : 0);
				edit.setChunkRowSize(m_compressionField.isEnabled() ? (Long) m_chunkField.getValue() : 1);
				
				reorderColumnEdits(Collections.list(((DefaultListModel<ColumnNodeEdit>) m_columnList.getModel()).elements()));
				
				for (int i = 0; i < edit.getColumnNodeEdits().length; i++) {
					DefaultMutableTreeNode treeNode = edit.getColumnNodeEdits()[i].getTreeNode();
					edit.getTreeNode().remove(treeNode);
					edit.getTreeNode().insert(treeNode, i);
				}
				
				edit.setEditAction(EditAction.MODIFY);
				
				edit.reloadTreeWithEditVisible();
			}
		}
    }
}

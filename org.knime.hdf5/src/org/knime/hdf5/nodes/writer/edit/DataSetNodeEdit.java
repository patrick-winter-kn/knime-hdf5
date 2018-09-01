package org.knime.hdf5.nodes.writer.edit;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
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
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
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
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.TransferHandler;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.data.DataCell;
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
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class DataSetNodeEdit extends TreeNodeEdit {

	private static final String COLUMN_PROPERTY_NAMES = "knime.columnnames";
	
	private static final String COLUMN_PROPERTY_TYPES = "knime.columntypes";
	
	private HdfDataType m_inputType;
	
	private HdfDataType m_outputType;

	private Endian m_endian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private int m_numberOfDimensions;
	
	private long m_inputRowCount = ColumnNodeEdit.UNKNOWN_ROW_COUNT;
	
	private int m_compressionLevel;
	
	private int m_chunkRowSize = 1;

	private OverwritePolicy m_overwritePolicy = OverwritePolicy.ABORT;
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();

	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	public DataSetNodeEdit(GroupNodeEdit parent, String name) {
		this(parent, null, name, EditAction.CREATE);
	}
	
	private DataSetNodeEdit( GroupNodeEdit parent, DataSetNodeEdit copyDataSet) {
		this(parent, copyDataSet.getInputPathFromFileWithName(), copyDataSet.getName(),
				copyDataSet.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
		copyAdditionalPropertiesFrom(copyDataSet);
		if (getEditAction() == EditAction.COPY) {
			copyDataSet.addIncompleteCopy(this);
		}
	}

	public DataSetNodeEdit(GroupNodeEdit parent, Hdf5DataSet<?> dataSet) {
		this(parent, dataSet.getPathFromFileWithName(), dataSet.getName(), EditAction.NO_ACTION);
		m_inputType = dataSet.getType().getHdfType().getType();
		m_outputType = m_inputType;
		m_endian = dataSet.getType().getHdfType().getEndian();
		m_stringLength = (int) dataSet.getType().getHdfType().getStringLength();
		m_numberOfDimensions = dataSet.getDimensions().length;
		m_numberOfDimensions = m_numberOfDimensions > 2 ? 2 : (m_numberOfDimensions < 1 ? 1 : m_numberOfDimensions);
		m_compressionLevel = dataSet.getCompressionLevel();
		m_chunkRowSize = (int) dataSet.getChunkRowSize();
		setHdfObject(dataSet);
	}
	
	DataSetNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editAction);
		setTreeNodeMenu(new DataSetNodeMenu());
		m_endian = Endian.LITTLE_ENDIAN;
		parent.addDataSetNodeEdit(this);
	}
	
	public DataSetNodeEdit copyDataSetEditTo(GroupNodeEdit parent) {
		DataSetNodeEdit newDataSetEdit = new DataSetNodeEdit(parent, this);

		for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
			columnEdit.copyColumnEditTo(newDataSetEdit);
		}
		
		for (AttributeNodeEdit attributeEdit : getAttributeNodeEdits()) {
			attributeEdit.copyAttributeEditTo(newDataSetEdit);
		}
		
		return newDataSetEdit;
	}

	public HdfDataType getInputType() {
		return m_inputType;
	}

	public HdfDataType getOutputType() {
		return m_outputType;
	}

	private void setOutputType(HdfDataType outputType) {
		m_outputType = outputType;
	}

	public Endian getEndian() {
		return m_endian;
	}

	private void setEndian(Endian endian) {
		m_endian = endian;
	}

	public boolean isFixed() {
		return m_fixed;
	}

	private void setFixed(boolean fixed) {
		m_fixed = fixed;
	}

	public int getStringLength() {
		return m_stringLength;
	}

	private void setStringLength(int stringLength) {
		m_stringLength = stringLength;
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

	public int getChunkRowSize() {
		return m_chunkRowSize;
	}

	private void setChunkRowSize(int chunkRowSize) {
		m_chunkRowSize = chunkRowSize;
	}

	public OverwritePolicy getOverwritePolicy() {
		return m_overwritePolicy;
	}

	private void setOverwritePolicy(OverwritePolicy overwritePolicy) {
		m_overwritePolicy = overwritePolicy;
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
		m_inputType = m_inputType != null && edit.getInputType().getConvertibleHdfTypes().contains(m_inputType) ? m_inputType : edit.getInputType();
		m_outputType = m_inputType.getConvertibleHdfTypes().contains(m_outputType) ? m_outputType : m_inputType;
		m_columnEdits.add(edit);
		edit.setParent(this);
		m_inputRowCount = m_inputRowCount == ColumnNodeEdit.UNKNOWN_ROW_COUNT ? edit.getInputRowCount() : m_inputRowCount;
	}

	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
		edit.setParent(this);
	}

	public boolean existsColumnNodeEdit(ColumnNodeEdit edit) {
		for (ColumnNodeEdit columnEdit : m_columnEdits) {
			if (edit.getName().equals(columnEdit.getName())) {
				return true;
			}
		}
		return false;
	}
	
	public boolean existsAttributeNodeEdit(AttributeNodeEdit edit) {
		for (AttributeNodeEdit attributeEdit : m_attributeEdits) {
			if (edit.getName().equals(attributeEdit.getName())) {
				return true;
			}
		}
		return false;
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

		// TODO find better algorithm here
		for (HdfDataType possibleInputType : HdfDataType.values()) {
			boolean convertible = true;
			for (HdfDataType inputType : getColumnInputTypes()) {
				if (!inputType.getConvertibleHdfTypes().contains(possibleInputType)) {
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
			if (columnEdit.getInputRowCount() != ColumnNodeEdit.UNKNOWN_ROW_COUNT) {
				inputRowCount = columnEdit.getInputRowCount();
				break;
			}
		}
		m_inputRowCount = inputRowCount;
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
	}
	
	private void setColumnEdits(List<ColumnNodeEdit> edits) {
		m_columnEdits.clear();
		m_columnEdits.addAll(edits);
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
	void integrate(DataSetNodeEdit copyEdit, long inputRowCount) {
		m_columnEdits.clear();
		if (getTreeNode() != null) {
			getTreeNode().removeAllChildren();
		}
		
		for (ColumnNodeEdit copyColumnEdit : copyEdit.getColumnNodeEdits()) {
			addColumnNodeEdit(copyColumnEdit);
			copyColumnEdit.addEditToNode(getTreeNode());
			if (copyColumnEdit.getEditAction() == EditAction.CREATE) {
				copyColumnEdit.setInputRowCount(inputRowCount);
			}
		}
		
		for (AttributeNodeEdit copyAttributeEdit : copyEdit.getAttributeNodeEdits()) {
			AttributeNodeEdit attributeEdit = getAttributeNodeEdit(copyAttributeEdit.getInputPathFromFileWithName(), copyAttributeEdit.getEditAction());
			if (attributeEdit != null && !attributeEdit.getEditAction().isCreateOrCopyAction() && !copyAttributeEdit.getEditAction().isCreateOrCopyAction()) {
				removeAttributeNodeEdit(attributeEdit);
			}
			addAttributeNodeEdit(copyAttributeEdit);
			copyAttributeEdit.addEditToNode(getTreeNode());
		}
		
		copyPropertiesFrom(copyEdit);
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		if (copyEdit instanceof DataSetNodeEdit) {
			DataSetNodeEdit copyDataSetEdit = (DataSetNodeEdit) copyEdit;
			m_inputType = copyDataSetEdit.getInputType();
			m_outputType = copyDataSetEdit.getOutputType();
			m_endian = copyDataSetEdit.getEndian();
			m_fixed = copyDataSetEdit.isFixed();
			m_stringLength = copyDataSetEdit.getStringLength();
			m_numberOfDimensions = copyDataSetEdit.getNumberOfDimensions();
			m_compressionLevel = copyDataSetEdit.getCompressionLevel();
			m_chunkRowSize = copyDataSetEdit.getChunkRowSize();
			m_overwritePolicy = copyDataSetEdit.getOverwritePolicy();
		}
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
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);

		settings.addInt(SettingsKey.OUTPUT_TYPE.getKey(), m_outputType.getTypeId());
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addBoolean(SettingsKey.FIXED.getKey(), m_fixed);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
		m_numberOfDimensions = isOneDimensionPossible() ? m_numberOfDimensions : 2;
		settings.addInt(SettingsKey.NUMBER_OF_DIMENSIONS.getKey(), m_numberOfDimensions);
		settings.addInt(SettingsKey.COMPRESSION.getKey(), m_compressionLevel);
		settings.addInt(SettingsKey.CHUNK_ROW_SIZE.getKey(), m_chunkRowSize);
		settings.addString(SettingsKey.OVERWRITE_POLICY.getKey(), m_overwritePolicy.getName());
		
	    NodeSettingsWO columnSettings = settings.addNodeSettings(SettingsKey.COLUMNS.getKey());
	    NodeSettingsWO attributeSettings = settings.addNodeSettings(SettingsKey.ATTRIBUTES.getKey());
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        NodeSettingsWO editSettings = columnSettings.addNodeSettings("" + edit.hashCode());
			edit.saveSettings(editSettings);
		}
	    
		for (AttributeNodeEdit edit : m_attributeEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
		        NodeSettingsWO editSettings = attributeSettings.addNodeSettings("" + edit.hashCode());
				edit.saveSettings(editSettings);
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
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
		
		setOutputType(HdfDataType.get(settings.getInt(SettingsKey.OUTPUT_TYPE.getKey())));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		setNumberOfDimensions(settings.getInt(SettingsKey.NUMBER_OF_DIMENSIONS.getKey()));
		setCompressionLevel(settings.getInt(SettingsKey.COMPRESSION.getKey()));
		setChunkRowSize(settings.getInt(SettingsKey.CHUNK_ROW_SIZE.getKey()));
		setOverwritePolicy(OverwritePolicy.get(settings.getString(SettingsKey.OVERWRITE_POLICY.getKey())));
		
		NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
		Enumeration<NodeSettingsRO> columnEnum = columnSettings.children();
		while (columnEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = columnEnum.nextElement();
			ColumnNodeEdit edit = new ColumnNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
					editSettings.getString(SettingsKey.NAME.getKey()), HdfDataType.get(editSettings.getInt(SettingsKey.INPUT_TYPE.getKey())),
					editSettings.getLong(SettingsKey.INPUT_ROW_COUNT.getKey()), editSettings.getInt(SettingsKey.INPUT_COLUMN_INDEX.getKey()),
					EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
			edit.loadSettings(editSettings);
		}

		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			AttributeNodeEdit edit = new AttributeNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
					editSettings.getString(SettingsKey.NAME.getKey()), HdfDataType.get(editSettings.getInt(SettingsKey.INPUT_TYPE.getKey())),
					EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
			edit.loadSettings(editSettings);
        }
	}
	
	void loadChildrenOfHdfObject() throws IOException {
		Hdf5DataSet<?> dataSet = (Hdf5DataSet<?>) getHdfObject();
		HdfDataType dataType = dataSet.getType().getHdfType().getType();

		long rowCount = dataSet.getDimensions().length >= 1 ? dataSet.getDimensions()[0] : 1;
		if (dataSet.getDimensions().length == 2) {
			if (dataSet.getDimensions()[1] > Integer.MAX_VALUE) {
				NodeLogger.getLogger(getClass()).warn("Number of columns of dataSet " + dataSet.getPathFromFileWithName()
						+ " has overflown the Integer values.");
			}
			int colCount = (int) dataSet.getDimensions()[1];
			for (int i = 0; i < colCount; i++) {
				ColumnNodeEdit columnEdit = new ColumnNodeEdit(this, "col" + (i+1), dataType, rowCount, i);
				columnEdit.addEditToNode(m_treeNode);
			}
		} else if (dataSet.getDimensions().length < 2) {
			ColumnNodeEdit columnEdit = new ColumnNodeEdit(this, "col", dataType, rowCount, 0);
			columnEdit.addEditToNode(m_treeNode);
		}
		
    	try {
    		for (String attributeName : dataSet.loadAttributeNames()) {
    			Hdf5Attribute<?> child = dataSet.updateAttribute(attributeName);
    			AttributeNodeEdit childEdit = new AttributeNodeEdit(this, child);
    			childEdit.addEditToNode(m_treeNode);
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
		return (edit instanceof DataSetNodeEdit || edit instanceof GroupNodeEdit) && !edit.equals(this)
				&& edit.getName().equals(getName()) && getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE;
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
			} else if (edit.getEditAction() == EditAction.COPY) {
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
		
		if (getOutputType() == HdfDataType.STRING) {
			int stringLength = 0;
			
			if (inputTable != null) {
				CloseableRowIterator iter = inputTable.iterator();
				while (iter.hasNext()) {
					DataRow row = iter.next();
					for (int i = 0; i < specIndices.length; i++) {
						if (getColumnNodeEdits()[i].getEditAction() == EditAction.CREATE) {
							DataCell cell = row.getCell(specIndices[i]);
							int newStringLength = cell.toString().length();
							stringLength = newStringLength > stringLength ? newStringLength : stringLength;
						}
					}
				}
			}
			for (int i = 0; i < copyColumnEdits.length; i++) {
				int inputColumnIndex = copyColumnEdits[i].getInputColumnIndex();
				for (int j = 0; j < copyDataSets[i].numberOfRows(); j++) {
					int newStringLength = copyDataSets[i].readRow(j)[inputColumnIndex].toString().length();
					stringLength = newStringLength > stringLength ? newStringLength : stringLength;
				}
			}

			if (!isFixed()) {
				setStringLength(stringLength);
				
			} else if (stringLength > getStringLength()) {
				NodeLogger.getLogger(getClass()).warn("Fixed string length has been changed from " + getStringLength() + " to " + stringLength);
				setStringLength(stringLength);
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
		
		Hdf5DataSet<Object> dataSet = (Hdf5DataSet<Object>) parent.createDataSetFromEdit(this);
		setHdfObject(dataSet);
		boolean success = dataSet != null;
		if (success) {
			CloseableRowIterator iter = inputTable != null ? inputTable.iterator() : null;
			try {
				for (long rowId = 0; rowId < m_inputRowCount; rowId++) {
					DataRow row = iter != null ? iter.next() : null;
					
					Object[] copyValues = new Object[copyDataSets.length];
					for (int i = 0; i < copyValues.length; i++) {
						copyValues[i] = copyDataSets[i].readRow(rowId)[copyColumnEdits[i].getInputColumnIndex()];
					}
					
					success &= dataSet.writeRowToDataSet(row, specIndices, rowId, copyValues);
				}
			} catch (UnsupportedDataTypeException udte) {
				NodeLogger.getLogger(getClass()).error(udte.getMessage(), udte);
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
		return createAction(null, null, false);
	}

	@Override
	protected boolean deleteAction() {
		Hdf5Group parent = (Hdf5Group) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		
		String name = getInputPathFromFileWithName().substring(getInputPathFromFileWithName().lastIndexOf("/") + 1);
		boolean success = parent.deleteDataSet(name) >= 0;
		if (success) {
			setHdfObject((Hdf5DataSet<?>) null);
		}
		
		return success;
	}

	@Override
	protected boolean modifyAction() {
		return true;
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
			private JComboBox<HdfDataType> m_typeField = new JComboBox<>();
			private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
			private JCheckBox m_useOneDimensionField = new JCheckBox();
			private JCheckBox m_compressionCheckBox;
			private JSpinner m_compressionField = new JSpinner(new SpinnerNumberModel(0, 0, 9, 1));
			private JSpinner m_chunkField = new JSpinner(new SpinnerNumberModel(1, 1, Integer.MAX_VALUE, 1));
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			private JRadioButton m_overwriteInsert = new JRadioButton("insert");
			private JList<ColumnNodeEdit> m_editList = new JList<>(new DefaultListModel<>());
	    	
			private DataSetPropertiesDialog() {
				super(DataSetNodeMenu.this, "DataSet properties");
				setMinimumSize(new Dimension(450, 500));

				addProperty("Name: ", m_nameField);
				
				m_typeField.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						boolean isString = (HdfDataType) m_typeField.getSelectedItem() == HdfDataType.STRING;
						m_endianField.setEnabled(!isString);
						m_stringLengthAuto.setEnabled(isString);
						m_stringLengthFixed.setEnabled(isString);
						m_stringLengthSpinner.setEnabled(isString && m_stringLengthFixed.isSelected());
					}
				});
				
				addProperty("Type: ", m_typeField);
				addProperty("Endian: ", m_endianField);
				
				JPanel stringLengthField = new JPanel();
				ButtonGroup stringLengthGroup = new ButtonGroup();
				stringLengthField.setLayout(new GridBagLayout());
				GridBagConstraints constraints = new GridBagConstraints();
				constraints.fill = GridBagConstraints.HORIZONTAL;
				constraints.gridx = 0;
				constraints.gridy = 0;
				constraints.weightx = 0.0;
				stringLengthField.add(m_stringLengthAuto, constraints);
				stringLengthGroup.add(m_stringLengthAuto);
				m_stringLengthAuto.setSelected(true);
				constraints.gridx++;
				stringLengthField.add(m_stringLengthFixed, constraints);
				stringLengthGroup.add(m_stringLengthFixed);
				constraints.gridx++;
				constraints.weightx = 1.0;
				stringLengthField.add(m_stringLengthSpinner, constraints);
				m_stringLengthSpinner.setEnabled(false);
				m_stringLengthFixed.addChangeListener(new ChangeListener() {
					
					@Override
					public void stateChanged(ChangeEvent e) {
						m_stringLengthSpinner.setEnabled(m_stringLengthFixed.isSelected());
					}
				});
				addProperty("String length: ", stringLengthField);

				addProperty("Create dataSet with one dimension", m_useOneDimensionField);
				
				m_compressionField.setEnabled(false);
				m_compressionCheckBox = addProperty("Compression: ", m_compressionField, new ChangeListener() {

					@Override
					public void stateChanged(ChangeEvent e) {
						boolean selected = ((JCheckBox) e.getSource()).isSelected();
						m_compressionField.setEnabled(selected);
						m_chunkField.setEnabled(selected);
					}
				});
				m_chunkField.setEnabled(false);
				addProperty("Chunk row size: ", m_chunkField);
				
				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				overwriteField.add(m_overwriteNo);
				overwriteGroup.add(m_overwriteNo);
				m_overwriteNo.setSelected(true);
				overwriteField.add(m_overwriteYes);
				overwriteGroup.add(m_overwriteYes);
				overwriteField.add(m_overwriteInsert);
				overwriteGroup.add(m_overwriteInsert);
				addProperty("Overwrite: ", overwriteField);
				
				DefaultListModel<ColumnNodeEdit> editModel = (DefaultListModel<ColumnNodeEdit>) m_editList.getModel();
				editModel.clear();
				for (ColumnNodeEdit edit : getColumnNodeEdits()) {
					editModel.addElement(edit);
				}
				
				m_editList.setVisibleRowCount(-1);
				m_editList.setCellRenderer(new DefaultListCellRenderer() {

					private static final long serialVersionUID = -3499393207598804129L;

					@Override
					public Component getListCellRendererComponent(JList<?> list,
							Object value, int index, boolean isSelected,
							boolean cellHasFocus) {
						super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);

						if (value instanceof ColumnNodeEdit) {
							ColumnNodeEdit edit = (ColumnNodeEdit) value;
							
							setText(edit.getName() + " (" + edit.getInputType().toString() + ")");
							Icon icon = null;
							try {
								icon = Hdf5KnimeDataType.getKnimeDataType(edit.getInputType(), true).getColumnDataType().getIcon();
							} catch (UnsupportedDataTypeException udte) {
								
							}
							setIcon(icon);
						}
						
						return this;
					}
				});
				
				m_editList.setDragEnabled(true);
				m_editList.setTransferHandler(new TransferHandler() {

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
		                m_editList.setModel(editModel);
		                
		                return true;
		            }
				});
				m_editList.setDropMode(DropMode.INSERT);
				
				m_editList.addMouseListener(new MouseAdapter() {
					
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
		    			if (m_editList.getModel().getSize() > 1) {
		    				int index = m_editList.locationToIndex(e.getPoint());
			    			ColumnNodeEdit edit = m_editList.getModel().getElementAt(index);
							if (edit != null) {
								m_editList.setSelectedIndex(index);
								JPopupMenu menu = new JPopupMenu();
								JMenuItem itemDelete = new JMenuItem("Delete");
				    			itemDelete.addActionListener(new ActionListener() {
									
									@Override
									public void actionPerformed(ActionEvent e) {
										int dialogResult = JOptionPane.showConfirmDialog(menu,
												"Are you sure to delete this column", "Warning", JOptionPane.YES_NO_OPTION);
										if (dialogResult == JOptionPane.YES_OPTION){
											edit.setDeletion(true);
											((DefaultListModel<ColumnNodeEdit>) m_editList.getModel()).remove(index);
											if (m_editList.getModel().getSize() == 1) {
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
				
				final JScrollPane jsp = new JScrollPane(m_editList);
				addProperty("Columns: ", jsp, 1.0);
			}
			
			@Override
			protected void initPropertyItems() {
				DataSetNodeEdit edit = DataSetNodeEdit.this;
				m_nameField.setText(edit.getName());
				m_typeField.setModel(new DefaultComboBoxModel<HdfDataType>(edit.getInputType().getConvertibleHdfTypes().toArray(new HdfDataType[] {})));
				m_typeField.setSelectedItem(edit.getOutputType());
				m_endianField.setSelectedItem(edit.getEndian());
				m_stringLengthAuto.setSelected(!edit.isFixed());
				m_stringLengthFixed.setSelected(edit.isFixed());
				m_stringLengthSpinner.setValue(edit.getStringLength());
				boolean oneDimensionPossible = isOneDimensionPossible();
				m_useOneDimensionField.setEnabled(oneDimensionPossible);
				m_useOneDimensionField.setSelected(oneDimensionPossible && m_numberOfDimensions == 1);
				m_compressionCheckBox.setSelected(edit.getCompressionLevel() > 0);
				m_compressionField.setValue(edit.getCompressionLevel());
				m_chunkField.setValue(edit.getChunkRowSize());
				m_overwriteNo.setSelected(edit.getOverwritePolicy() == OverwritePolicy.ABORT);
				m_overwriteYes.setSelected(edit.getOverwritePolicy() == OverwritePolicy.OVERWRITE);
				m_overwriteInsert.setSelected(edit.getOverwritePolicy() == OverwritePolicy.INSERT);
				
				DefaultListModel<ColumnNodeEdit> columnModel = (DefaultListModel<ColumnNodeEdit>) m_editList.getModel();
				columnModel.clear();
				for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
					if (columnEdit.getEditAction() != EditAction.DELETE) {
						columnModel.addElement(columnEdit);
					}
				}
			}

			@Override
			protected void editPropertyItems() {
				DataSetNodeEdit edit = DataSetNodeEdit.this;
				edit.setName(m_nameField.getText());
				edit.setOutputType((HdfDataType) m_typeField.getSelectedItem());
				edit.setEndian((Endian) m_endianField.getSelectedItem());
				edit.setFixed(m_stringLengthFixed.isSelected());
				edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				edit.setNumberOfDimensions(m_useOneDimensionField.isSelected() ? 1 : 2);
				edit.setCompressionLevel(m_compressionField.isEnabled() ? (Integer) m_compressionField.getValue() : 0);
				edit.setChunkRowSize(m_compressionField.isEnabled() ? (Integer) m_chunkField.getValue() : 1);
				edit.setOverwritePolicy(m_overwriteYes.isSelected() ? OverwritePolicy.OVERWRITE
						: (m_overwriteNo.isSelected() ? OverwritePolicy.ABORT : OverwritePolicy.INSERT));
				
				List<ColumnNodeEdit> newEdits = new ArrayList<>();
				newEdits.addAll(Collections.list(((DefaultListModel<ColumnNodeEdit>) m_editList.getModel()).elements()));
				for (ColumnNodeEdit columnEdit : edit.getColumnNodeEdits()) {
					if (columnEdit.getEditAction() == EditAction.DELETE) {
						newEdits.add(columnEdit);
					}
				}
				edit.setColumnEdits(newEdits);
				
				edit.getTreeNode().removeAllChildren();
				for (ColumnNodeEdit columnEdit : edit.getColumnNodeEdits()) {
					edit.getTreeNode().add(new DefaultMutableTreeNode(columnEdit));
				}
				
				edit.setEditAction(EditAction.MODIFY);
				
				edit.reloadTreeWithEditVisible();
			}
		}
    }
}

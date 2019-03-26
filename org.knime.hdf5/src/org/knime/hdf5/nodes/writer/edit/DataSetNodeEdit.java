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
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
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

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.RadionButtonPanel;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.DataTypeChooser;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;

/**
 * Class for edits on dataSets in an hdf file. The respective hdf
 * source is an {@linkplain Hdf5DataSet}.
 */
public class DataSetNodeEdit extends TreeNodeEdit {
	
	private boolean m_overwriteWithNewColumns;

	private HdfDataType m_inputType;
	
	private EditDataType m_editDataType = new EditDataType();
	
	private int m_inputNumberOfDimensions;
	
	private boolean m_useOneDimension;
	
	private long m_inputRowSize = ColumnNodeEdit.UNKNOWN_ROW_SIZE;
	
	private int m_compressionLevel;
	
	private long m_chunkRowSize = 1L;
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();

	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();

	/**
	 * Creates a new dataSet edit to CREATE a new dataSet with the {@code name}
	 * in the hdf group of {@code parent}.
	 * 
	 * @param parent the parent of this edit
	 * @param name the output name of this edit
	 */
	public DataSetNodeEdit(GroupNodeEdit parent, String name) {
		this(parent, name, null);
	}

	/**
	 * Copies the dataSet edit {@code copyDataSet} to {@code parent} with all
	 * properties, but without its child edits.
	 * <br>
	 * <br>
	 * If {@code needsCopySource} is {@code true}, the action of this edit
	 * will be set to COPY, except if {@code copyDataSet}'s edit action is CREATE.
	 * In all other cases, the action of this edit is the same as of {@code copyDataSet}.
	 * 
	 * @param parent the parent of this edit
	 * @param copyDataSet the dataSet edit to copy from
	 * @param needsCopySource if the {@code copyDataSet} is needed to execute a COPY action
	 * 	with this edit later
	 */
	private DataSetNodeEdit(GroupNodeEdit parent, DataSetNodeEdit copyDataSet, boolean needsCopySource) {
		this(parent, copyDataSet.getInputPathFromFileWithName(), copyDataSet.getName(), copyDataSet.getEditOverwritePolicy(),
				needsCopySource ? (copyDataSet.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY) : copyDataSet.getEditAction());
		copyAdditionalPropertiesFrom(copyDataSet);
		if (needsCopySource && getEditAction() == EditAction.COPY) {
			setCopyEdit(copyDataSet);
		}
	}
	
	/**
	 * Does the same as {@linkplain #DataSetNodeEdit(GroupNodeEdit, String)}
	 * if {@code unsupportedCause} is {@code null}.
	 * Otherwise, it creates a new placeholder for an invalid dataSet with
	 * the cause {@code unsupportedCause}.
	 * 
	 * @param parent the parent of this edit
	 * @param name the name of this edit
	 * @see #DataSetNodeEdit(GroupNodeEdit, String)
	 */
	DataSetNodeEdit(GroupNodeEdit parent, String name, String unsupportedCause) {
		this(parent, null, name, EditOverwritePolicy.NONE, unsupportedCause == null ? EditAction.CREATE : EditAction.NO_ACTION);
		setUnsupportedCause(unsupportedCause);
		// remove menu to consider the edit as unsupported
		if (unsupportedCause != null) {
			setTreeNodeMenu(null);
		}
	}

	/**
	 * Initializes a new dataSet edit for the input hdf {@code dataSet}.
	 * The edit action is set to NO_ACTION.
	 * 
	 * @param parent the parent of this edit
	 * @param dataSet the hdf dataSet for this edit
	 * @throws IllegalArgumentException if the parent of {@code dataSet} and the
	 * 	hdf object of {@code parent} are not the same hdf group
	 */
	public DataSetNodeEdit(GroupNodeEdit parent, Hdf5DataSet<?> dataSet) throws IllegalArgumentException {
		this(parent, dataSet.getPathFromFileWithName(), dataSet.getName(), EditOverwritePolicy.NONE, EditAction.NO_ACTION);
		
		if (dataSet.getParent() != parent.getHdfObject()) {
			throw new IllegalArgumentException("Hdf dataSet cannot be added to this edit.");
		}
		
		m_inputNumberOfDimensions = dataSet.getDimensions().length;
		if (m_inputNumberOfDimensions == 0) {
			throw new IllegalArgumentException("Hdf dataSet is scalar.");
			
		} else if (m_inputNumberOfDimensions > 2) {
			throw new IllegalArgumentException("Hdf dataSet has more than 2 dimensions.");
		}
		
		Hdf5HdfDataType hdfType = dataSet.getType().getHdfType();
		m_inputType = hdfType.getType();
		m_editDataType.setValues(m_inputType, m_inputType.getPossiblyConvertibleHdfTypes(), hdfType.getEndian(),
				Rounding.DOWN, m_inputType.isFloat(), false, (int) hdfType.getStringLength());
		m_useOneDimension = m_inputNumberOfDimensions == 1;
		m_compressionLevel = dataSet.getCompressionLevel();
		m_chunkRowSize = (int) dataSet.getChunkRowSize();
		setHdfObject(dataSet);
	}
	
	/**
	 * Initializes a new dataSet edit with all core settings.
	 * 
	 * @param parent the parent of this edit
	 * @param inputPathFromFileWithName the path of this edit's hdf dataSet
	 * @param name the output name of this edit
	 * @param editOverwritePolicy the overwrite policy for this edit
	 * @param editAction the action of this edit
	 */
	DataSetNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name, EditOverwritePolicy editOverwritePolicy, EditAction editAction) {
		super(inputPathFromFileWithName, !(parent instanceof FileNodeEdit) ? parent.getOutputPathFromFileWithName() : "", name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new DataSetNodeMenu());
		m_editDataType.setEndian(Endian.LITTLE_ENDIAN);
		parent.addDataSetNodeEdit(this);
	}

	/**
	 * Copies this edit to {@code parent} with all children.
	 * 
	 * @param parent the destination of the new copy
	 * @return the new copy
	 */
	public DataSetNodeEdit copyDataSetEditTo(GroupNodeEdit parent) {
		return copyDataSetEditTo(parent, true);
	}

	/**
	 * Copies this edit to {@code parent} with all children.
	 * 
	 * @param parent the destination of the new copy
	 * @param needsCopySource if the information about this edit is needed for
	 * 	the new edit
	 * @return the new copy
	 */
	DataSetNodeEdit copyDataSetEditTo(GroupNodeEdit parent, boolean needsCopySource) {
		DataSetNodeEdit newDataSetEdit = new DataSetNodeEdit(parent, this, needsCopySource);
		newDataSetEdit.addEditToParentNodeIfPossible();

		for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
			columnEdit.copyColumnEditTo(newDataSetEdit, needsCopySource);
		}
		
		for (AttributeNodeEdit attributeEdit : getAttributeNodeEdits()) {
			attributeEdit.copyAttributeEditTo(newDataSetEdit, needsCopySource);
		}
		
		return newDataSetEdit;
	}
	
	/**
	 * Returns if new columns, that should be added to this dataSet,
	 * should overwrite the existing column on the respective column index.
	 * Otherwise, new columns will just be inserted.
	 * 
	 * @return if new columns should overwrite existing columns
	 */
	boolean isOverwriteWithNewColumns() {
		return m_overwriteWithNewColumns;
	}
	
	private void setOverwriteWithNewColumns(boolean overwriteWithNewColumns) {
		m_overwriteWithNewColumns = overwriteWithNewColumns;
	}
	
	/**
	 * @return the input type of the hdf dataSet or (if the dataSet should be
	 * created) the smallest input type that fits all input columns
	 */
	public HdfDataType getInputType() {
		return m_inputType;
	}
	
	/**
	 * Sets the new input type and updates the list of possible output types
	 * in the {@linkplain EditDataType}.
	 * 
	 * @param inputType the new input type
	 * @see DataSetNodeEdit#getInputType()
	 */
	private void setInputType(HdfDataType inputType) {
		m_inputType = inputType;
		m_editDataType.setPossibleOutputTypes(m_inputType != null ?
				m_inputType.getPossiblyConvertibleHdfTypes() : new ArrayList<>());
	}
	
	/**
	 * @return information on the output data type of this dataSet edit
	 */
	public EditDataType getEditDataType() {
		return m_editDataType;
	}
	
	/**
	 * Returns if the number of dimensions for the edit of the hdf dataSet is 1.
	 * Otherwise, it is 2. Other numbers of dimensions are not supported so far.
	 * 
	 * @return the number of dimensions
	 */
	public boolean usesOneDimension() {
		return m_useOneDimension;
	}
	
	private void setUseOneDimension(boolean useOneDimension) {
		m_useOneDimension = useOneDimension;
	}
	
	/**
	 * @return the input row size of the children column edits
	 * 	(if unequal row sizes, the edit will be invalid anyway)
	 */
	public long getInputRowSize() {
		return m_inputRowSize;
	}
	
	void setInputRowSize(long inputRowSize) {
		m_inputRowSize = inputRowSize;
	}

	/**
	 * @return the compression level
	 * @see Hdf5DataSet#getCompressionLevel()
	 */
	public int getCompressionLevel() {
		return m_compressionLevel;
	}

	private void setCompressionLevel(int compressionLevel) {
		m_compressionLevel = compressionLevel;
	}

	/**
	 * @return the size of the row chunks
	 * @see Hdf5DataSet#getChunkRowSize()
	 */
	public long getChunkRowSize() {
		return m_chunkRowSize;
	}

	private void setChunkRowSize(long chunkRowSize) {
		m_chunkRowSize = chunkRowSize;
	}

	/**
	 * @return the children of this edit which are column edits
	 */
	public ColumnNodeEdit[] getColumnNodeEdits() {
		return m_columnEdits.toArray(new ColumnNodeEdit[m_columnEdits.size()]);
	}
	
	/**
	 * @return the input types of the columns that will not be deleted
	 */
	public HdfDataType[] getColumnInputTypes() {
		ColumnNodeEdit[] columnEdits = getNotDeletedColumnNodeEdits();
		HdfDataType[] hdfTypes = new HdfDataType[columnEdits.length];
		for (int i = 0; i < hdfTypes.length; i++) {
			hdfTypes[i] = columnEdits[i].getInputType();
		}
		return hdfTypes;
	}

	/**
	 * @return the children of this edit which are column edits and will not
	 * 	be deleted when executing this edit
	 */
	ColumnNodeEdit[] getNotDeletedColumnNodeEdits() {
		List<ColumnNodeEdit> columnEdits = new ArrayList<>();
		for (ColumnNodeEdit edit : m_columnEdits) {
			if (edit.getEditAction() != EditAction.DELETE) {
				columnEdits.add(edit);
			}
		}
		return columnEdits.toArray(new ColumnNodeEdit[] {});
	}
	
	/**
	 * Checks if at most this edit contains at most 1 column edit that does
	 * not get deleted.
	 * 
	 * @return if the new dataSet can also be created with 1 dimension
	 */
	private boolean isOneDimensionPossible() {
		return getNotDeletedColumnNodeEdits().length <= 1;
	}
	
	/**
	 * @return the number of columns that already exist in the hdf dataSet
	 * 	of this edit
	 */
	int getRequiredColumnCountForExecution() {
		int colCount = 0;
		
		for (ColumnNodeEdit columnEdit : m_columnEdits) {
			if (!columnEdit.getEditAction().isCreateOrCopyAction()) {
				colCount++;
			}
		}
		
		return colCount;
	}

	/**
	 * Get the child column edit with the specified input path and the column index
	 * in its origin hdf dataSet within the hdf file of this edit's root or
	 * {@code null} if none exists.
	 * 
	 * @param inputPathFromFileWithName the input path from file
	 * @param inputColumnIndex the input path from file
	 * @return the searched child group edit
	 */
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
	
	/**
	 * Get the index of {@code columnEdit} in this edit defined by its position
	 * in the list of this edit's child column edits which will not be deleted.
	 * 
	 * @param columnEdit the child column edit
	 * @return the index of the column edit to be created while execution or -1
	 * 	if it will not be created here
	 */
	int getIndexOfColumnEdit(ColumnNodeEdit columnEdit) {
		int index = -1;
		
		ColumnNodeEdit[] columnEdits = getColumnNodeEdits();
		if (columnEdit.getEditAction() != EditAction.DELETE) {
			for (int i = 0; i < columnEdits.length; i++) {
				if (!(columnEdits[i].getEditAction() == EditAction.DELETE
						|| m_overwriteWithNewColumns && columnEdits[i].getEditAction().isCreateOrCopyAction())) {
					index++;
				}
				if (columnEdits[i] == columnEdit) {
					break;
				}
			}
		}
			
		return index;
	}

	/**
	 * @return the children of this edit which are attribute edits
	 */
	public AttributeNodeEdit[] getAttributeNodeEdits() {
		return m_attributeEdits.toArray(new AttributeNodeEdit[m_attributeEdits.size()]);
	}

	/**
	 * For {@code editAction == EditAction.CREATE}, get the child attribute edit
	 * (with CREATE action) for the flowVariable with the name
	 * {@code inputPathFromFileWithName} or {@code null} if none exists.
	 * <br>
	 * <br>
	 * For {@code editAction == EditAction.COPY}, get the child attribute edit
	 * with COPY action with the specified input path within the hdf file of
	 * this edit's root or {@code null} if none exists.
	 * <br>
	 * <br>
	 * For other {@code editAction}s, get the child attribute edit
	 * without COPY action with the specified input path within the hdf file of
	 * this edit's root or {@code null} if none exists.
	 * 
	 * @param inputPathFromFileWithName the input path from file
	 * @param editAction the input path from file
	 * @return the searched child attribute edit
	 */
	AttributeNodeEdit getAttributeNodeEdit(String inputPathFromFileWithName, EditAction editAction) {
		if (inputPathFromFileWithName != null) {
			for (AttributeNodeEdit attributeEdit : m_attributeEdits) {
				if (inputPathFromFileWithName.equals(attributeEdit.getInputPathFromFileWithName())
						&& (editAction == EditAction.CREATE) == (attributeEdit.getEditAction() == EditAction.CREATE)
						&& (editAction == EditAction.COPY) == (attributeEdit.getEditAction() == EditAction.COPY)) {
					return attributeEdit;
				}
			}
		}
		return null;
	}

	/**
	 * Adds a new column edit to this dataSet edit. Also updates the input type,
	 * number of dimensions and input row size.
	 * 
	 * @param edit the new column edit
	 */
	void addColumnNodeEdit(ColumnNodeEdit edit) {
		m_columnEdits.add(edit);
		edit.setParent(this);
		considerColumnNodeEdit(edit);
	}
	
	/**
	 * Updates the input type, number of dimensions and input row size on
	 * {@code newColumnEdit} is newly existing in this dataSet edit.
	 * 
	 * @param newColumnEdit the new column edit existing in this edit
	 */
	void considerColumnNodeEdit(ColumnNodeEdit newColumnEdit) {
		if (newColumnEdit.getEditAction() != EditAction.DELETE) {
			// update the input data type if the new column does not fit in the current one
			if (m_inputType == null || !newColumnEdit.getInputType().getPossiblyConvertibleHdfTypes().contains(m_inputType)) {
				setInputType(newColumnEdit.getInputType());
			}
			if (!m_inputType.getPossiblyConvertibleHdfTypes().contains(m_editDataType.getOutputType())) {
				m_editDataType.setOutputType(m_inputType);
				setEditAction(EditAction.MODIFY);
			}

			// use one dimension if the hdf dataSet also has one dimension or does not exist yet
			m_useOneDimension = m_useOneDimension && isOneDimensionPossible()
					&& (getEditAction().isCreateOrCopyAction() || m_inputNumberOfDimensions == 1);
			
			// update the row size if none is known so far
			m_inputRowSize = m_inputRowSize == ColumnNodeEdit.UNKNOWN_ROW_SIZE ? newColumnEdit.getInputRowSize() : m_inputRowSize;
		}
	}

	void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
		edit.setParent(this);
	}

	/**
	 * Removes the column edit from this dataSet edit. Also updates the input type,
	 * number of dimensions and input row size.
	 * 
	 * @param edit the new column edit
	 */
	void removeColumnNodeEdit(ColumnNodeEdit edit) {
		m_columnEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		disconsiderColumnNodeEdit(edit);
	}
	
	/**
	 * Updates the input type, number of dimensions and input row size on
	 * {@code oldColumnEdit} which has already been removed from this dataSet
	 * edit.
	 * 
	 * @param oldColumnEdit the old column edit removed from this edit
	 */
	void disconsiderColumnNodeEdit(ColumnNodeEdit oldColumnEdit) {
		if (oldColumnEdit.getEditAction() != EditAction.DELETE) {
			HdfDataType previousInputType = m_inputType;
			setInputType(null);

			// update the input data type if there are more possible data types after removing the old column edit
			Set<HdfDataType> uniqueColumnInputTypes = new HashSet<>(Arrays.asList(getColumnInputTypes()));
			if (!uniqueColumnInputTypes.contains(previousInputType)) {
				for (HdfDataType possibleInputType : uniqueColumnInputTypes) {
					boolean convertible = false;
					for (HdfDataType inputType : uniqueColumnInputTypes) {
						convertible = true;
						if (!inputType.getAlwaysConvertibleHdfTypes().contains(possibleInputType)) {
							convertible = false;
							break;
						}
					}
					if (convertible) {
						setInputType(possibleInputType);
						break;
					}
				}
			} else {
				setInputType(previousInputType);
			}
			
			// use one dimension if the hdf dataSet also has one dimension or does not exist yet
			m_useOneDimension = isOneDimensionPossible()
					&& (getEditAction().isCreateOrCopyAction() || m_inputNumberOfDimensions == 1);

			// update the row size if there still exists a known one
			long inputRowSize = ColumnNodeEdit.UNKNOWN_ROW_SIZE;
			for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
				if (columnEdit.getEditAction() != EditAction.DELETE && columnEdit != oldColumnEdit
						&& columnEdit.getInputRowSize() != ColumnNodeEdit.UNKNOWN_ROW_SIZE) {
					inputRowSize = columnEdit.getInputRowSize();
					break;
				}
			}
			m_inputRowSize = inputRowSize;
		}
	}
	
	void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
	}
	
	/**
	 * Reorder the child column edits to the new order of {@code edits}.
	 * 
	 * @param edits the new order of the child column edits
	 */
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

	@Override
	protected boolean havePropertiesChanged(Object hdfSource) {
		boolean propertiesChanged = true;
		
		Hdf5DataSet<?> copyDataSet = (Hdf5DataSet<?>) hdfSource;
		
		if (hdfSource != null) {
			propertiesChanged = copyDataSet.getType().getHdfType().getType() != m_editDataType.getOutputType()
					|| copyDataSet.getType().getHdfType().getEndian() != m_editDataType.getEndian()
					|| copyDataSet.getType().getHdfType().getStringLength() != m_editDataType.getStringLength()
					|| copyDataSet.getDimensions().length != (m_useOneDimension ? 1 : 2)
					|| copyDataSet.getCompressionLevel() != m_compressionLevel
					|| copyDataSet.getChunkRowSize() != m_chunkRowSize
					|| copyDataSet.numberOfColumns() != getColumnInputTypes().length;
			
			if (!propertiesChanged) {
				int columnIndex = 0;
				for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
					if (columnEdit.getEditAction() != EditAction.NO_ACTION || columnEdit.getInputColumnIndex() != columnIndex) {
						propertiesChanged = true;
						break;
					}
					columnIndex++;
				}
			}
		}
		
		return propertiesChanged;
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		if (copyEdit instanceof DataSetNodeEdit) {
			DataSetNodeEdit copyDataSetEdit = (DataSetNodeEdit) copyEdit;
			m_overwriteWithNewColumns = copyDataSetEdit.isOverwriteWithNewColumns();
			m_inputType = copyDataSetEdit.getInputType();
			m_editDataType.setValues(copyDataSetEdit.getEditDataType());
			m_inputNumberOfDimensions = copyDataSetEdit.m_inputNumberOfDimensions;
			m_useOneDimension = copyDataSetEdit.usesOneDimension();
			m_compressionLevel = copyDataSetEdit.getCompressionLevel();
			m_chunkRowSize = copyDataSetEdit.getChunkRowSize();
		}
	}
	
	@Override
	public String getToolTipText() {
		return (isSupported() ? "(" + getDataTypeInfo()
				+ (m_inputRowSize != ColumnNodeEdit.UNKNOWN_ROW_SIZE ? ", rows: " + m_inputRowSize : "")
				+ ") " : "") + super.getToolTipText();
	}
	
	/**
	 * @return the information about the input and output data type
	 */
	private String getDataTypeInfo() {
		String info = "";
		
		if (m_inputType != null && m_inputType != m_editDataType.getOutputType()) {
			info += m_inputType.toString() + " -> ";
		}
		
		return info + m_editDataType.getOutputType().toString();
	}
	
	@Override
	protected long getProgressToDoInEdit() throws IOException {
		long totalToDo = 0L;
		
		if (getEditAction() != EditAction.NO_ACTION && getEditAction() != EditAction.MODIFY_CHILDREN_ONLY && getEditState() != EditState.SUCCESS) {
			totalToDo += 331L + (havePropertiesChanged(getEditAction() != EditAction.CREATE ? findCopySource() : null) ? m_inputRowSize * getProgressToDoPerRow() : 0L);
		}
		
		return totalToDo;
	}
	
	/**
	 * @return the progress which needs to be done per row when writing
	 * 	the dataSet
	 */
	long getProgressToDoPerRow() {
		HdfDataType dataType = m_editDataType.getOutputType();
		long dataTypeToDo = dataType.isNumber() ? dataType.getSize()/8 : m_editDataType.getStringLength();
		return getColumnInputTypes().length * dataTypeToDo;
	}

	@Override
	protected TreeNodeEdit[] getAllChildren() {
		List<TreeNodeEdit> children = new ArrayList<>();
		
		children.addAll(m_columnEdits);
		children.addAll(m_attributeEdits);
		
		return children.toArray(new TreeNodeEdit[0]);
	}
	
	/**
	 * Integrates the children of {@code copyEdit} (not the attribute edits
	 * with {@code editAction == NO_ACTION}) in this dataSet edit. The
	 * properties of this dataSet edit will stay the same. The properties
	 * of this edit's children will be overwritten. {@code removeOldColumns}
	 * specifies if the existing columns should be removed before integrating.
	 * 
	 * @param copyEdit the dataSet edit to be integrated
	 * @param removeOldColumns if the existing columns should be removed
	 * 	before integrating
	 */
	void integrate(DataSetNodeEdit copyEdit, boolean removeOldColumns) {
		// TODO only supported for dataSets if the dataSet has the correct amount of columns (the same amount as the config was created)
		if (removeOldColumns) {
			for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
				columnEdit.removeFromParent();
			}
		}
		
		/* 
		 * copy the column edits into here and set the hdf objects to the hdf
		 * dataSet of this edit for the copy column edits
		 */
		for (ColumnNodeEdit copyColumnEdit : copyEdit.getColumnNodeEdits()) {
			copyColumnEdit.copyColumnEditTo(this, false).setHdfObject((Hdf5DataSet<?>) getHdfObject());
		}
		
		integrateAttributeEdits(copyEdit);
	}

	@Override
	protected void removeFromParent() {
		((GroupNodeEdit) getParent()).removeDataSetNodeEdit(this);
	}
	
	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);
		
		settings.addBoolean(SettingsKey.OVERWRITE_WITH_NEW_COLUMNS.getKey(), m_overwriteWithNewColumns);
		m_editDataType.saveSettingsTo(settings);
		settings.addInt(SettingsKey.INPUT_NUMBER_OF_DIMENSIONS.getKey(), m_inputNumberOfDimensions);
		settings.addInt(SettingsKey.OUTPUT_NUMBER_OF_DIMENSIONS.getKey(), m_useOneDimension ? 1 : 2);
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
		setOverwriteWithNewColumns(settings.getBoolean(SettingsKey.OVERWRITE_WITH_NEW_COLUMNS.getKey()));
		m_editDataType.loadSettingsFrom(settings);
		m_inputNumberOfDimensions = settings.getInt(SettingsKey.INPUT_NUMBER_OF_DIMENSIONS.getKey());
		
		int numberOfDimensions = settings.getInt(SettingsKey.OUTPUT_NUMBER_OF_DIMENSIONS.getKey());
		if (numberOfDimensions < 1 || numberOfDimensions > 2) {
			throw new InvalidSettingsException("Number of dimensions has to be 1 or 2");
		}
		m_useOneDimension = numberOfDimensions == 1;
		
		setCompressionLevel(settings.getInt(SettingsKey.COMPRESSION.getKey()));
		setChunkRowSize(settings.getLong(SettingsKey.CHUNK_ROW_SIZE.getKey()));
		
		NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
		Enumeration<NodeSettingsRO> columnEnum = columnSettings.children();
		while (columnEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = columnEnum.nextElement();
			ColumnNodeEdit edit = new ColumnNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
					editSettings.getInt(SettingsKey.INPUT_COLUMN_INDEX.getKey()), editSettings.getString(SettingsKey.NAME.getKey()),
					HdfDataType.get(editSettings.getInt(SettingsKey.INPUT_TYPE.getKey())), editSettings.getLong(SettingsKey.INPUT_ROW_SIZE.getKey()),
					EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
			edit.loadSettingsFrom(editSettings);
		}

		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			AttributeNodeEdit edit = new AttributeNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
					editSettings.getString(SettingsKey.NAME.getKey()), EditOverwritePolicy.get(editSettings.getString(SettingsKey.EDIT_OVERWRITE_POLICY.getKey())),
					HdfDataType.get(editSettings.getInt(SettingsKey.INPUT_TYPE.getKey())), EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
			edit.loadSettingsFrom(editSettings);
        }
	}

	/**
	 * Loads the children of the hdf dataSet for which this edit is
	 * for and adds those as newly initialized child edits to this edit.
	 * 
	 * @throws IOException if the hdf dataSet is not loaded or not open or
	 * 	an error in the hdf library occurred
	 */
	void loadChildrenOfHdfObject() throws IOException {
		Hdf5DataSet<?> dataSet = (Hdf5DataSet<?>) getHdfObject();
		
		if (isSupported()) {
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
					columnEdit.addEditToParentNodeIfPossible();
				}
			} else if (dataSet.getDimensions().length < 2) {
				ColumnNodeEdit columnEdit = new ColumnNodeEdit(this, 0, "col", dataType, rowCount);
				columnEdit.addEditToParentNodeIfPossible();	
			}
		}
		
    	try {
    		for (String attributeName : dataSet.loadAttributeNames()) {
    			AttributeNodeEdit childEdit = null;
    			try {
        			Hdf5Attribute<?> child = dataSet.getAttribute(attributeName);
        			childEdit = new AttributeNodeEdit(this, child);
        			childEdit.addEditToParentNodeIfPossible();
        			
    			} catch (UnsupportedDataTypeException udte) {
    				// for unsupported attributes
        			childEdit = new AttributeNodeEdit(this, attributeName, "Unsupported data type");
        			childEdit.addEditToParentNodeIfPossible();
    			}
    		}
    	} catch (NullPointerException npe) {
    		throw new IOException(npe.getMessage());
    	}
	}
	
	@Override
	protected InvalidCause validateEditInternal() {
		return getName().contains("/") || getName().isEmpty() ? InvalidCause.NAME_CHARS :
			getName().startsWith(BACKUP_PREFIX) && !getOutputPathFromFileWithName(true).equals(getInputPathFromFileWithName())
					? InvalidCause.NAME_BACKUP_PREFIX : null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		boolean conflictPossible = !(edit instanceof ColumnNodeEdit) && !(edit instanceof AttributeNodeEdit) && this != edit;
		boolean inConflict = conflictPossible && getName().equals(edit.getName()) && getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE;
		
		return inConflict ? !avoidsOverwritePolicyNameConflict(edit) : conflictPossible && willBeNameConflictWithIgnoredEdit(edit);
	}
	
	/**
	 * If the overwrite policy is 'overwrite', set a DELETE action to those
	 * column edits that should be overwritten by its successor.
	 */
	void useOverwritePolicyOfColumns() {
		if (m_overwriteWithNewColumns) {
			// TODO assumes that there are no two columnEdits with CREATE or COPY next to each other
			ColumnNodeEdit[] columnEdits = getColumnNodeEdits();
			for (int i = 0; i < columnEdits.length; i++) {
				if (columnEdits[i].getEditAction().isCreateOrCopyAction()) {
					columnEdits[i-1].setEditAction(EditAction.DELETE);
				}
			}
		}
	}

	@Override
	protected void createAction(Map<String, FlowVariable> flowVariables, ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			setHdfObject(((Hdf5Group) getOpenedHdfObjectOfParent()).createDataSetFromEdit(this));
			addProgress(331, exec, totalProgressToDo, true);
			
		} finally {
			if (getHdfObject() != null) {
				setEditState(EditState.CREATE_SUCCESS_WRITE_POSTPONED);
			} else {
				setEditSuccess(false);
			}
		}
	}

	@Override
	protected void copyAction(ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			Hdf5DataSet<?> copyDataSet = (Hdf5DataSet<?>) findCopySource();
			
			if (havePropertiesChanged(copyDataSet)) {
				copyColumnsAction(exec, totalProgressToDo);
			} else {
				setHdfObject(((Hdf5Group) getOpenedHdfObjectOfParent()).copyObject(copyDataSet, getName()));
			}
		} finally {
			setEditSuccess(getHdfObject() != null);
		}
	}

	@Override
	protected void deleteAction() throws IOException {
		try {
			Hdf5DataSet<?> dataSet = (Hdf5DataSet<?>) getHdfObject();
			if (((Hdf5Group) getOpenedHdfObjectOfParent()).deleteObject(dataSet.getName())) {
				setHdfObject((Hdf5DataSet<?>) null);
			}
		} finally {
			setEditSuccess(getHdfObject() == null);
		}
	}

	@Override
	protected void modifyAction(ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			Hdf5DataSet<?> oldDataSet = (Hdf5DataSet<?>) getHdfSource();
			if (havePropertiesChanged(oldDataSet)) {
				copyColumnsAction(exec, totalProgressToDo);
			} else {
				if (!oldDataSet.getName().equals(getName())) {
					if (oldDataSet == getHdfBackup()) {
						setHdfObject(((Hdf5Group) getOpenedHdfObjectOfParent()).copyObject(oldDataSet, getName()));
					} else {
						setHdfObject(((Hdf5Group) getOpenedHdfObjectOfParent()).moveObject(oldDataSet, getName()));
					}
				}
			}
		} finally {
			setEditSuccess(getHdfObject() != null);
		}
	}
	
	/**
	 * Creates a new dataSet and copies all values from existing dataSets
	 * specified by the child column edits.
	 * 
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @throws IOException if an error occurred
	 */
	private void copyColumnsAction(ExecutionContext exec, long totalProgressToDo) throws IOException {
		boolean success = false;
		
		Hdf5DataSet<?> dataSet = null;
		try {
			boolean withoutFail = true;
			
			dataSet = ((Hdf5Group) getOpenedHdfObjectOfParent()).createDataSetFromEdit(this);
			addProgress(331, exec, totalProgressToDo, true);
			
			ColumnNodeEdit[] columnEdits = getNotDeletedColumnNodeEdits();
			Hdf5DataSet<?>[] dataSets = new Hdf5DataSet[columnEdits.length];
			long[] columnIndices = new long[columnEdits.length];
			for (int i = 0; i < columnEdits.length; i++) {
				ColumnNodeEdit columnEdit = columnEdits[i];
				dataSets[i] = (Hdf5DataSet<?>) columnEdit.findCopySource();
				columnIndices[i] = columnEdit.getInputColumnIndex();
			}
			
			for (long i = 0; i < m_inputRowSize; i++) {
				withoutFail &= dataSet.copyValuesToRow(i, dataSets, columnIndices, m_editDataType.getRounding());
				addProgress(getProgressToDoPerRow(), exec, totalProgressToDo, false);
			}

			if (getEditAction() == EditAction.MODIFY) {
				Hdf5DataSet<?> oldDataSet = (Hdf5DataSet<?>) getHdfSource();
				for (String attrName : oldDataSet.loadAttributeNames()) {
					withoutFail &= dataSet.copyAttribute(oldDataSet.getAttribute(attrName), attrName) != null;
				}
			}
			
			success = withoutFail;
			
		} catch (HDF5DataspaceInterfaceException hdie) {
			throw new IOException(hdie);
			
		} finally {
			if (success) {
				setEditState(EditState.SUCCESS);
				setHdfObject(dataSet);
				
			} else {
				setEditState(EditState.FAIL);
				if (dataSet != null) {
					((Hdf5Group) getOpenedHdfObjectOfParent()).deleteObject(getName());
				}
			}
		}
	}

	/**
	 * The class for the {@linkplain JPopupMenu} which can be accessed through
	 * a right mouse click on this dataSet edit.
	 */
	public class DataSetNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = -6418394582185524L;
    	
		private DataSetNodeMenu() {
			super(DataSetNodeEdit.this.isSupported(), false, true);
    	}

		/**
		 * Returns the dialog to modify the properties of this dataSet edit.
		 * 
		 * @return the properties dialog
		 */
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

		/**
		 * A {@linkplain JDialog} to set all properties of this dataSet edit.
		 */
		private class DataSetPropertiesDialog extends PropertiesDialog {
	    	
			private static final long serialVersionUID = -9060929832634560737L;
			private static final String INSERT_NEW_COLUMNS = "insert";
			private static final String OVERWRITE_WITH_NEW_COLUMNS = "overwrite";

			private final JTextField m_nameField = new JTextField(15);
			private final JComboBox<EditOverwritePolicy> m_overwriteField = new JComboBox<>(EditOverwritePolicy.getAvailablePoliciesForEdit(DataSetNodeEdit.this));
			private final RadionButtonPanel<String> m_overwriteWithNewColumnsField = new RadionButtonPanel<>(null, INSERT_NEW_COLUMNS, OVERWRITE_WITH_NEW_COLUMNS);
			private final DataTypeChooser m_dataTypeChooser = m_editDataType.new DataTypeChooser(true);
			private final JCheckBox m_useOneDimensionField = new JCheckBox();
			private final JCheckBox m_compressionCheckBox;
			private final JSpinner m_compressionField = new JSpinner(new SpinnerNumberModel(9, 0, 9, 1));
			private final JSpinner m_chunkField = new JSpinner(new SpinnerNumberModel((Long) 1L, (Long) 1L, (Long) Long.MAX_VALUE, (Long) 1L));
			private final JList<ColumnNodeEdit> m_columnList = new JList<>(new DefaultListModel<>());
	    	
			private DataSetPropertiesDialog() {
				super(DataSetNodeMenu.this, "DataSet properties");

				addProperty("Name: ", m_nameField);
				addProperty("Overwrite: ", m_overwriteField);
				
				addProperty("Overwrite for new columns: ", m_overwriteWithNewColumnsField);
				m_overwriteWithNewColumnsField.setEnabled(!getEditAction().isCreateOrCopyAction());
				
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
		                
		                // add the transfered column edits to the model at the new indices
		                for (int i = 0; i < transferableCount; i++) {
			                editModel.add(startIndex + i, edits.get(transferableIndices[i]));
		                }

		                // remove the transfered column edits from the model at the old indices
		                for (int i = transferableCount - 1; i >= 0; i--) {
		                	int index = transferableIndices[i];
		                	index += startIndex > index ? 0 : transferableCount;
		                	editModel.remove(index);
		                }
		                
		                // update the model
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
				m_overwriteWithNewColumnsField.setSelectedValue(edit.isOverwriteWithNewColumns() ? OVERWRITE_WITH_NEW_COLUMNS : INSERT_NEW_COLUMNS);
				m_dataTypeChooser.loadFromDataType();
				boolean oneDimensionPossible = isOneDimensionPossible();
				m_useOneDimensionField.setEnabled(oneDimensionPossible);
				m_useOneDimensionField.setSelected(edit.usesOneDimension());
				
				boolean useCompression = edit.getCompressionLevel() > 0;
				m_compressionCheckBox.setSelected(useCompression);
				if (useCompression) {
					m_compressionField.setValue(edit.getCompressionLevel());
					m_chunkField.setValue(edit.getChunkRowSize());
				}
				
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
				edit.setOverwriteWithNewColumns(m_overwriteWithNewColumnsField.getSelectedValue().equals(OVERWRITE_WITH_NEW_COLUMNS));
				m_dataTypeChooser.saveToDataType();
				edit.setUseOneDimension(m_useOneDimensionField.isSelected());
				
				boolean useCompression = m_compressionField.isEnabled();
				edit.setCompressionLevel(useCompression ? (Integer) m_compressionField.getValue() : 0);
				edit.setChunkRowSize(useCompression ? (Long) m_chunkField.getValue() : 1);
				
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

	@Override
	public String toString() {
		return "{ input=" + getInputPathFromFileWithName() + ",output=" + getOutputPathFromFileWithName()
				+ ",action=" + getEditAction() + ",state=" + getEditState() 
				+ ",overwrite=" + getEditOverwritePolicy() + ",overwriteWithNewColumns=" + m_overwriteWithNewColumns + ",valid=" + isValid()
				+ ",dimension" + (usesOneDimension() ? "=" + m_inputRowSize : 
					"s=[" + m_inputRowSize + ", " + getNotDeletedColumnNodeEdits().length + "]")
				+ ",inputType=" + m_inputType + ",editDataType=" + m_editDataType
				+ ",compressionLevel=" + m_compressionLevel + ",chunkRowSize=" + m_chunkRowSize 
				+ ",dataSet=" + getHdfObject() + ",backup=" + getHdfBackup() + " }";
	}
}

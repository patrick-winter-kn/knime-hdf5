package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.Map;

import javax.swing.JPopupMenu;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;

/**
 * Class for edits on columns of dataSets in an hdf file. The respective hdf
 * source is specified by the {@linkplain Hdf5DataSet} and
 * {@linkplain ColumnNodeEdit#getInputColumnIndex()}.
 */
public class ColumnNodeEdit extends TreeNodeEdit {

	public static final long UNKNOWN_ROW_SIZE = -1;
	
	private static final int NO_COLUMN_INDEX = -1;
	
	private InvalidCause m_inputInvalidCause;

	private final int m_inputColumnIndex;
	
	private int m_outputColumnIndex;
	
	private final HdfDataType m_inputType;
	
	private long m_inputRowSize;

	/**
	 * Copies the column edit {@code copyColumn} to {@code parent} with all
	 * properties.
	 * <br>
	 * <br>
	 * If {@code needsCopySource} is {@code true}, the action of this edit
	 * will be set to COPY, except if {@code copyColumn}'s edit action is CREATE.
	 * In all other cases, the action of this edit is the same as of {@code copyColumn}.
	 * 
	 * @param parent the parent of this edit
	 * @param copyColumn the column edit to copy from
	 * @param needsCopySource if the {@code copyColumn} is needed to execute a COPY action
	 * 	with this edit later
	 */
	private ColumnNodeEdit(DataSetNodeEdit parent, ColumnNodeEdit copyColumn, boolean needsCopySource) {
		this(parent, copyColumn.getInputPathFromFileWithName(), copyColumn.getInputColumnIndex(),
				copyColumn.getName(), copyColumn.getInputType(), copyColumn.getInputRowSize(),
				needsCopySource ? (copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY) : copyColumn.getEditAction());
		copyAdditionalPropertiesFrom(copyColumn);
		if (needsCopySource && getEditAction() == EditAction.COPY) {
			setCopyEdit(copyColumn.getParent());
		}
		
		/* 
		 * is needed to set the edit action of the parent dataSet edit to MODIFY
		 * instead of MODIFY_CHILDREN_ONLY
		 */
		updateDataSetEditAction(parent);
	}

	/**
	 * Initializes a new column edit with the input knime {@code columnSpec}.
	 * The edit action is set to CREATE.
	 * 
	 * @param parent the parent of this edit
	 * @param columnSpec the knime column spec for this edit
	 */
	public ColumnNodeEdit(DataSetNodeEdit parent, DataColumnSpec columnSpec) {
		this(parent, columnSpec.getName(), NO_COLUMN_INDEX, columnSpec.getName(), HdfDataType.getHdfDataType(columnSpec.getType()),
				UNKNOWN_ROW_SIZE, EditAction.CREATE);
		updateDataSetEditAction(parent);
	}
	
	/**
	 * Does updates on the edit action of the {@code parent} dataSet edit
	 * which are not included in the update of a {@linkplain TreeNodeEdit}.
	 * 
	 * @param parent the parent dataSet edit for this edit
	 */
	private void updateDataSetEditAction(DataSetNodeEdit parent) {
		/*if (parent.getEditAction().isCreateOrCopyAction()) {
			parent.setEditAction(EditAction.CREATE);
		} else */if (parent.getEditAction() != EditAction.DELETE) {
			parent.setEditAction(EditAction.MODIFY);
		}
	}

	/**
	 * Initializes a new column edit for the input hdf dataSet of the
	 * {@code parent}. The edit action is set to NO_ACTION.
	 * 
	 * @param parent the parent of this edit
	 * @param inputColumnIndex the input column of this edit in the hdf dataSet
	 * 	of the parent (set to
	 * 	{@linkplain ColumnNodeEdit#NO_COLUMN_INDEX} if this edit is initialized
	 * 	from a knime column spec)
	 * @param name the name of this edit
	 * @param inputType the input data type of this edit
	 * @param inputRowSize the input row size of this edit (set to
	 * 	{@linkplain ColumnNodeEdit#UNKNOWN_ROW_SIZE} if this edit is initialized
	 * 	from a knime column spec)
	 */
	ColumnNodeEdit(DataSetNodeEdit parent, int inputColumnIndex, String name, HdfDataType inputType, long inputRowSize) {
		this(parent, parent.getInputPathFromFileWithName(), inputColumnIndex, name, inputType, inputRowSize, EditAction.NO_ACTION);
		setHdfObject((Hdf5TreeElement) parent.getHdfObject());
	}

	/**
	 * Initializes a new column edit with all core settings.
	 * 
	 * @param parent the parent of this edit
	 * @param inputPathFromFileWithName the path of the parent's edit's hdf dataSet
	 * @param inputColumnIndex the input column of this edit in the hdf dataSet
	 * 	of the parent (set to
	 * 	{@linkplain ColumnNodeEdit#NO_COLUMN_INDEX} if this edit is initialized
	 * 	from a knime column spec)
	 * @param name the output name of this edit
	 * @param inputType the input data type of this edit
	 * @param inputRowSize the input row size of this edit (set to
	 * 	{@linkplain ColumnNodeEdit#UNKNOWN_ROW_SIZE} if this edit is initialized
	 * 	from a knime column spec)
	 * @param editAction the action of this edit
	 */
	ColumnNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, int inputColumnIndex, String name, HdfDataType inputType,
			long inputRowSize, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, EditOverwritePolicy.NONE, editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_inputColumnIndex = inputColumnIndex;
		m_inputType = inputType;
		m_inputRowSize = inputRowSize;
		parent.addColumnNodeEdit(this);
	}

	/**
	 * Copies this edit to {@code parent}.
	 * 
	 * @param parent the destination of the new copy
	 * @return the new copy
	 */
	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent) {
		return copyColumnEditTo(parent, true);
	}

	/**
	 * Copies this edit to {@code parent}.
	 * 
	 * @param parent the destination of the new copy
	 * @param needsCopySource if the information about this edit is needed for
	 * 	the new edit
	 * @return the new copy
	 */
	ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent, boolean needsCopySource) {
		ColumnNodeEdit newColumnEdit = new ColumnNodeEdit(parent, this, needsCopySource);
		newColumnEdit.addEditToParentNodeIfPossible();
		
		return newColumnEdit;
	}
	
	/**
	 * @return if the input column specs contradict with this edit
	 */
	InvalidCause getInputInvalidCause() {
		return m_inputInvalidCause;
	}

	/**
	 * @return the column index of the column in the parent's hdf dataSet
	 * 	(or {@linkplain ColumnNodeEdit#NO_COLUMN_INDEX} if this edit was
	 * 	initialized from a knime column spec)
	 */
	int getInputColumnIndex() {
		return m_inputColumnIndex;
	}

	/**
	 * @return the column index for the parent's hdf dataSet
	 */
	int getOutputColumnIndex() {
		return m_outputColumnIndex;
	}

	/**
	 * @return the input type of the source (either the knime column spec or
	 * 	the hdf dataSet)
	 */
	public HdfDataType getInputType() {
		return m_inputType;
	}

	/**
	 * @return the input row size of this column edit (or
	 * {@linkplain ColumnNodeEdit#UNKNOWN_ROW_SIZE} if this edit was initialized
	 * from a knime column spec)
	 */
	long getInputRowSize() {
		return m_inputRowSize;
	}
	
	/**
	 * Only changes the input row size if it is still unknown.
	 * 
	 * @param inputRowSize the new input row size
	 */
	void setInputRowSize(long inputRowSize) {
		if (m_inputRowSize == UNKNOWN_ROW_SIZE) {
			m_inputRowSize = inputRowSize;
			if (((DataSetNodeEdit) getParent()).getInputRowSize() == UNKNOWN_ROW_SIZE) {
				((DataSetNodeEdit) getParent()).setInputRowSize(inputRowSize);
			}
		}
	}
	
	@Override
	protected void setDeletion(boolean isDelete) {
		if (isDelete && getParent() != null) {
			((DataSetNodeEdit) getParent()).disconsiderColumnNodeEdit(this);
		}
		super.setDeletion(isDelete);
		if (!isDelete && getParent() != null) {
			((DataSetNodeEdit) getParent()).considerColumnNodeEdit(this);
		}
	}
	
	/**
	 * The properties of a column edit will never change.
	 * 
	 * @return false
	 */
	@Override
	protected boolean havePropertiesChanged(Object hdfSource) {
		return false;
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		m_inputInvalidCause = ((ColumnNodeEdit) copyEdit).getInputInvalidCause();
	}
	
	@Override
	public String getToolTipText() {
		return "(" + m_inputType.toString()
				+ (m_inputRowSize != ColumnNodeEdit.UNKNOWN_ROW_SIZE ? ", rows: " + m_inputRowSize : "")
				+ ") " + super.getToolTipText();
	}
	
	@Override
	protected long getProgressToDoInEdit() {
		return 0L;
	}

	@Override
	protected TreeNodeEdit[] getAllChildren() {
		return new TreeNodeEdit[0];
	}
	
	@Override
	protected void removeFromParent() {
		((DataSetNodeEdit) getParent()).removeColumnNodeEdit(this);
		setParent(null);
	}
	
	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);

		if (m_inputInvalidCause != null) {
			settings.addInt(SettingsKey.INPUT_INVALID_CAUSE.getKey(), m_inputInvalidCause.ordinal());
		}
		settings.addInt(SettingsKey.INPUT_COLUMN_INDEX.getKey(), m_inputColumnIndex);
		settings.addInt(SettingsKey.OUTPUT_COLUMN_INDEX.getKey(), ((DataSetNodeEdit) getParent()).getIndexOfColumnEdit(this));
		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		settings.addLong(SettingsKey.INPUT_ROW_SIZE.getKey(), m_inputRowSize);
	}
	
	@Override
	protected void loadSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey(SettingsKey.INPUT_INVALID_CAUSE.getKey())) {
			m_inputInvalidCause = InvalidCause.values()[settings.getInt(SettingsKey.INPUT_INVALID_CAUSE.getKey())];
		}
		m_outputColumnIndex = settings.getInt(SettingsKey.OUTPUT_COLUMN_INDEX.getKey());
	}
	
	@Override
	protected InvalidCause validateEditInternal() {
		DataSetNodeEdit parent = (DataSetNodeEdit) getParent();
		
		InvalidCause cause = getEditAction() != EditAction.DELETE && m_inputRowSize != UNKNOWN_ROW_SIZE
				&& m_inputRowSize != parent.getInputRowSize() ? InvalidCause.ROW_COUNT : null;
		
		// for column overwrite poicy 'overwrite': check if all new columns have a column to overwrite
		if (cause == null && parent.isOverwriteWithNewColumns()) {
			ColumnNodeEdit[] columnEdits = parent.getColumnNodeEdits();
			for (int i = 0; i < columnEdits.length; i++) {
				if (columnEdits[i] == this) {
					cause = getEditAction().isCreateOrCopyAction() && (i-1 < 0 || columnEdits[i-1].getEditAction().isCreateOrCopyAction())
							? InvalidCause.COLUMN_OVERWRITE : null;
					break;
				}
			}
		}
		
		// check if the selected output type of the parent dataSet edit fits for the input data of this column
		if (cause == null && getEditAction() != EditAction.CREATE && getEditAction() != EditAction.DELETE) {
			Object[] values = null;
			EditDataType parentDataType = parent.getEditDataType();
			
			try {
				// TODO maybe check this when the columns loaded for the first time
				TreeNodeEdit copyEdit = getEditAction() == EditAction.COPY ? getCopyEdit() : this;
				Hdf5DataSet<?> dataSet = copyEdit != null ? (Hdf5DataSet<?>) copyEdit.getHdfSource() : null;
				if (dataSet != null) {
					dataSet.open();
					
					// only supported for dataSets with max. 2 dimensions
					if (m_inputRowSize == dataSet.numberOfRows()) {
						values = dataSet.readColumn(dataSet.getDimensions().length > 1 ? new long[] { getInputColumnIndex() } : new long[0]);
					} else {
						cause = InvalidCause.INPUT_ROW_SIZE;
					}
				}
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error("Validation of dataType of new column \""
						+ getOutputPathFromFileWithName() +  "\" could not be checked: " + ioe.getMessage(), ioe);
				
			} catch (HDF5DataspaceInterfaceException ioe) {
				cause = cause == null ? InvalidCause.COLUMN_INDEX : cause;
			}
			
			if (values != null && cause == null) {
				cause = !parentDataType.getOutputType().areValuesConvertible(values, m_inputType, parentDataType) ? InvalidCause.OUTPUT_DATA_TYPE : cause;
			}
		}
		
		return cause;
	}
	
	/**
	 * @return if the output data type may be used for this edit, but it
	 * 	depends on the input data
	 */
	public boolean isMaybeInvalid() {
		EditDataType parentDataType = ((DataSetNodeEdit) getParent()).getEditDataType();
		return !m_inputType.getAlwaysConvertibleHdfTypes().contains(parentDataType.getOutputType())
				|| !parentDataType.getOutputType().isNumber() && parentDataType.isFixedStringLength();
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return false;
	}
	
	/**
	 * Validates the CREATE action of this edit based on the knime input
	 * {@code columnSpecs} (not based on the data) such that it will be
	 * noticed if some knime input is different from the one when the
	 * settings were defined.
	 * 
	 * @param colSpecs the knime column specs of the input table
	 */
	void validateCreateAction(DataColumnSpec[] colSpecs) {
		m_inputInvalidCause = null;
		for (DataColumnSpec colSpec : colSpecs) {
			if (colSpec.getName().equals(getInputPathFromFileWithName())) {
				m_inputInvalidCause = m_inputType == HdfDataType.getHdfDataType(colSpec.getType()) ? null : InvalidCause.INPUT_DATA_TYPE;
				return;
			}
		}
		
		m_inputInvalidCause = InvalidCause.NO_COPY_SOURCE;
	}

	/**
	 * Not supported here.
	 */
	@Override
	protected void createAction(Map<String, FlowVariable> flowVariables, ExecutionContext exec, long totalProgressToDo) {
	}

	/**
	 * Not supported here.
	 */
	@Override
	protected void copyAction(ExecutionContext exec, long totalProgressToDo) {
	}

	/**
	 * Not supported here.
	 */
	@Override
	protected void deleteAction() {
	}

	/**
	 * Not supported here.
	 */
	@Override
	protected void modifyAction(ExecutionContext exec, long totalProgressToDo) {
	}
	
	/**
	 * The class for the {@linkplain JPopupMenu} which can be accessed through
	 * a right mouse click on this column edit.
	 */
	public class ColumnNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = 7696321716083384515L;
    	
		private ColumnNodeMenu() {
			super(false, false, true);
    	}

		@Override
		protected void onDelete() {
			ColumnNodeEdit edit = ColumnNodeEdit.this;
			TreeNodeEdit parentOfVisible = edit.getParent();
			edit.setDeletion(edit.getEditAction() != EditAction.DELETE);

        	DataSetNodeEdit parentEdit = (DataSetNodeEdit) parentOfVisible;
        	parentEdit.setEditAction(EditAction.MODIFY);
        	if (parentEdit.getColumnInputTypes().length == 0) {
        		parentOfVisible = parentEdit.getParent();
				parentEdit.setDeletion(true);
        	}

            parentOfVisible.reloadTreeWithEditVisible(true);
		}
	}

	@Override
	public String toString() {
		return "{ input=" + getInputPathFromFileWithName() + ",inputColumnIndex=" + m_inputColumnIndex
				+ ",output=" + getOutputPathFromFileWithName() + ",outputColumnIndex=" + m_outputColumnIndex
				+ ",action=" + getEditAction() + ",state=" + getEditState()
				+ ",overwrite=" + getEditOverwritePolicy() + ",valid=" + isValid()
				+ ",rowSize=" + m_inputRowSize + ",inputType=" + m_inputType 
				+ ",dataSet=" + getHdfObject() + ",backupDataSet=" + getHdfBackup() + " }";
	}
}

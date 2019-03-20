package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.Map;

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

public class ColumnNodeEdit extends TreeNodeEdit {

	public static final long UNKNOWN_ROW_COUNT = -1;
	
	private static final int NO_COLUMN_INDEX = -1;
	
	private InvalidCause m_inputInvalidCause;

	private final int m_inputColumnIndex;
	
	private int m_outputColumnIndex;
	
	private final HdfDataType m_inputType;
	
	private long m_inputRowCount;
	
	private ColumnNodeEdit(DataSetNodeEdit parent, ColumnNodeEdit copyColumn, boolean needsCopySource) {
		this(parent, copyColumn.getInputPathFromFileWithName(), copyColumn.getInputColumnIndex(),
				copyColumn.getName(), copyColumn.getInputType(), copyColumn.getInputRowCount(),
				needsCopySource ? (copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY) : copyColumn.getEditAction());
		copyAdditionalPropertiesFrom(copyColumn);
		if (needsCopySource && getEditAction() == EditAction.COPY) {
			setCopyEdit(copyColumn.getParent());
		}
		updateDataSetEditAction(parent);
	}

	public ColumnNodeEdit(DataSetNodeEdit parent, DataColumnSpec columnSpec) {
		this(parent, columnSpec.getName(), NO_COLUMN_INDEX, columnSpec.getName(), HdfDataType.getHdfDataType(columnSpec.getType()),
				UNKNOWN_ROW_COUNT, EditAction.CREATE);
		updateDataSetEditAction(parent);
	}
	
	private void updateDataSetEditAction(DataSetNodeEdit parent) {
		/*if (parent.getEditAction().isCreateOrCopyAction()) {
			parent.setEditAction(EditAction.CREATE);
		} else */if (parent.getEditAction() != EditAction.DELETE) {
			parent.setEditAction(EditAction.MODIFY);
		}
	}
	
	ColumnNodeEdit(DataSetNodeEdit parent, int inputColumnIndex, String name, HdfDataType inputType, long inputRowCount) {
		this(parent, parent.getInputPathFromFileWithName(), inputColumnIndex, name, inputType, inputRowCount, EditAction.NO_ACTION);
		setHdfObject((Hdf5TreeElement) parent.getHdfObject());
	}
	
	ColumnNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, int inputColumnIndex, String name, HdfDataType inputType,
			long inputRowCount, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, EditOverwritePolicy.NONE, editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_inputColumnIndex = inputColumnIndex;
		m_inputType = inputType;
		m_inputRowCount = inputRowCount;
		parent.addColumnNodeEdit(this);
	}

	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent) {
		return copyColumnEditTo(parent, true);
	}
	
	ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent, boolean needsCopySource) {
		ColumnNodeEdit newColumnEdit = new ColumnNodeEdit(parent, this, needsCopySource);
		newColumnEdit.addEditToParentNodeIfPossible();
		
		return newColumnEdit;
	}
	
	InvalidCause getInputInvalidCause() {
		return m_inputInvalidCause;
	}

	int getInputColumnIndex() {
		return m_inputColumnIndex;
	}

	int getOutputColumnIndex() {
		return m_outputColumnIndex;
	}

	public HdfDataType getInputType() {
		return m_inputType;
	}

	long getInputRowCount() {
		return m_inputRowCount;
	}
	
	void setInputRowCount(long inputRowCount) {
		if (m_inputRowCount == UNKNOWN_ROW_COUNT) {
			m_inputRowCount = inputRowCount;
			if (((DataSetNodeEdit) getParent()).getInputRowCount() == UNKNOWN_ROW_COUNT) {
				((DataSetNodeEdit) getParent()).setInputRowCount(inputRowCount);
			}
		}
	}
	
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
				+ (m_inputRowCount != ColumnNodeEdit.UNKNOWN_ROW_COUNT ? ", rows: " + m_inputRowCount : "")
				+ ") " + super.getToolTipText();
	}
	
	@Override
	protected long getProgressToDoInEdit() {
		return 0L;
	}
	
	@Override
	void setDeletion(boolean isDelete) {
		if (isDelete && getParent() != null) {
			((DataSetNodeEdit) getParent()).disconsiderColumnNodeEdit(this);
		}
		super.setDeletion(isDelete);
		if (!isDelete && getParent() != null) {
			((DataSetNodeEdit) getParent()).considerColumnNodeEdit(this);
		}
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
		settings.addLong(SettingsKey.INPUT_ROW_COUNT.getKey(), m_inputRowCount);
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
		
		InvalidCause cause = getEditAction() != EditAction.DELETE && m_inputRowCount != UNKNOWN_ROW_COUNT
				&& m_inputRowCount != parent.getInputRowCount() ? InvalidCause.ROW_COUNT : null;
		
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
					if (m_inputRowCount == dataSet.numberOfRows()) {
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
	
	public boolean isMaybeInvalid() {
		EditDataType parentDataType = ((DataSetNodeEdit) getParent()).getEditDataType();
		return !m_inputType.getAlwaysConvertibleHdfTypes().contains(parentDataType.getOutputType())
				|| !parentDataType.getOutputType().isNumber() && parentDataType.isFixed();
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return false;
	}
	
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

	@Override
	protected void createAction(Map<String, FlowVariable> flowVariables, ExecutionContext exec, long totalProgressToDo) {
	}
	
	@Override
	protected void copyAction(ExecutionContext exec, long totalProgressToDo) {
	}

	@Override
	protected void deleteAction() {
	}

	@Override
	protected void modifyAction(ExecutionContext exec, long totalProgressToDo) {
	}
	
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
}

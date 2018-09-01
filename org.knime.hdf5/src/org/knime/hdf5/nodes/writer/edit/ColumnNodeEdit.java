package org.knime.hdf5.nodes.writer.edit;

import java.util.Map;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

public class ColumnNodeEdit extends TreeNodeEdit {

	public static final long UNKNOWN_ROW_COUNT = -1;
	
	private static final int NO_COLUMN_INDEX = -1;
	
	private final HdfDataType m_inputType;
	
	private long m_inputRowCount;

	private final int m_inputColumnIndex;

	private ColumnNodeEdit(DataSetNodeEdit parent, ColumnNodeEdit copyColumn) {
		this(parent, copyColumn.getInputPathFromFileWithName(), copyColumn.getName(), copyColumn.getInputType(),
				copyColumn.getInputRowCount(), copyColumn.getInputColumnIndex(),
				copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
		copyAdditionalPropertiesFrom(copyColumn);
		if (getEditAction() == EditAction.COPY) {
			copyColumn.getParent().addIncompleteCopy(parent);
		}
	}

	public ColumnNodeEdit(DataSetNodeEdit parent, DataColumnSpec columnSpec) {
		this(parent, columnSpec.getName(), columnSpec.getName(), HdfDataType.getHdfDataType(columnSpec.getType()),
				UNKNOWN_ROW_COUNT, NO_COLUMN_INDEX, EditAction.CREATE);
		if (parent.getEditAction() == EditAction.COPY) {
			parent.setEditAction(EditAction.CREATE);
		}
	}
	
	ColumnNodeEdit(DataSetNodeEdit parent, String name, HdfDataType inputType, long inputRowCount, int inputColumnIndex) {
		this(parent, parent.getInputPathFromFileWithName(), name, inputType, inputRowCount, inputColumnIndex, EditAction.NO_ACTION);
	}
	
	ColumnNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, String name, HdfDataType inputType,
			long inputRowCount, int inputColumnIndex, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_inputType = inputType;
		m_inputRowCount = inputRowCount;
		m_inputColumnIndex = inputColumnIndex;
		parent.addColumnNodeEdit(this);
	}
	
	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent) {
		return new ColumnNodeEdit(parent, this);
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
	
	int getInputColumnIndex() {
		return m_inputColumnIndex;
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		// nothing to do here
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
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);

		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		settings.addLong(SettingsKey.INPUT_ROW_COUNT.getKey(), m_inputRowCount);
		settings.addInt(SettingsKey.INPUT_COLUMN_INDEX.getKey(), m_inputColumnIndex);
	}
	
	@Override
	protected void loadSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		// nothing to do here
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		InvalidCause cause = m_inputRowCount == UNKNOWN_ROW_COUNT || m_inputRowCount == ((DataSetNodeEdit) getParent()).getInputRowCount()
				? null : InvalidCause.ROW_COUNT;
		
		if (cause == null) {
			
		}
		
		return cause;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return false;
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		return false;
	}
	
	@Override
	protected boolean copyAction() {
		return false;
	}

	@Override
	protected boolean deleteAction() {
		return false;
	}

	@Override
	protected boolean modifyAction() {
		return false;
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
        	if (parentEdit.getTreeNode().getChildCount() == 0) {
        		parentOfVisible = parentEdit.getParent();
				parentEdit.setDeletion(true);
        	}

            parentOfVisible.reloadTreeWithEditVisible(true);
		}
	}
}

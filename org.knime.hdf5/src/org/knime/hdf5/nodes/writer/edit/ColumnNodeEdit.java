package org.knime.hdf5.nodes.writer.edit;

import java.util.Map;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;

public class ColumnNodeEdit extends TreeNodeEdit {

	public static final long UNKNOWN_ROW_COUNT = -1;
	
	private static final int NO_COLUMN_INDEX = -1;
	
	private final DataColumnSpec m_columnSpec;
	
	private long m_inputRowCount;

	private final int m_inputColumnIndex;

	private ColumnNodeEdit(ColumnNodeEdit copyColumn, DataSetNodeEdit parent) {
		this(copyColumn.getEditAction() == EditAction.CREATE ? copyColumn.getName() :
				((DataSetNodeEdit) copyColumn.getParent()).getInputPathFromFileWithName(),
				copyColumn.getColumnSpec(), parent, copyColumn.getInputRowCount(), copyColumn.getInputColumnIndex(),
				copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
		copyAdditionalPropertiesFrom(copyColumn);
		if (getEditAction() == EditAction.COPY) {
			copyColumn.getParent().addIncompleteCopy(parent);
		}
	}

	public ColumnNodeEdit(DataColumnSpec columnSpec, DataSetNodeEdit parent) {
		this(columnSpec.getName(), columnSpec, parent, UNKNOWN_ROW_COUNT, NO_COLUMN_INDEX, EditAction.CREATE);
		if (parent.getEditAction() == EditAction.COPY) {
			parent.setEditAction(EditAction.CREATE);
		}
	}
	
	ColumnNodeEdit(DataColumnSpec columnSpec, DataSetNodeEdit parent, long inputRowCount, int inputColumnIndex) {
		this(columnSpec.getName(), columnSpec, parent, inputRowCount, inputColumnIndex, EditAction.NO_ACTION);
	}
	
	ColumnNodeEdit(String inputPathFromFileWithName, DataColumnSpec columnSpec, DataSetNodeEdit parent,
			long inputRowCount, int inputColumnIndex, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), columnSpec.getName(), editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_columnSpec = columnSpec;
		m_inputRowCount = inputRowCount;
		m_inputColumnIndex = inputColumnIndex;
		parent.addColumnNodeEdit(this);
	}
	
	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent) {
		return new ColumnNodeEdit(this, parent);
	}

	DataColumnSpec getColumnSpec() {
		return m_columnSpec;
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
		
		settings.addDataType(SettingsKey.COLUMN_SPEC_TYPE.getKey(), m_columnSpec.getType());
		settings.addLong(SettingsKey.INPUT_ROW_COUNT.getKey(), m_inputRowCount);
		settings.addInt(SettingsKey.INPUT_COLUMN_INDEX.getKey(), m_inputColumnIndex);
	}
	
	@Override
	protected void loadSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		// nothing to do here
	}
	
	@Override
	protected InvalidCause validateEditInternal() {
		return m_inputRowCount == UNKNOWN_ROW_COUNT || m_inputRowCount == ((DataSetNodeEdit) getParent()).getInputRowCount()
				? null : InvalidCause.ROW_COUNT;
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

package org.knime.hdf5.nodes.writer.edit;

import java.util.Map;

import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;

public class ColumnNodeEdit extends TreeNodeEdit {
	
	private final DataColumnSpec m_columnSpec;
	
	private int m_inputColumnIndex = -1;

	private ColumnNodeEdit(ColumnNodeEdit copyColumn, DataSetNodeEdit parent) {
		this(copyColumn.getEditAction() == EditAction.CREATE ? copyColumn.getName() :
				((DataSetNodeEdit) copyColumn.getParent()).getInputPathFromFileWithName(), copyColumn.getColumnSpec(), parent,
				copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
		m_inputColumnIndex = copyColumn.getInputColumnIndex();
		if (getEditAction() == EditAction.COPY) {
			copyColumn.getParent().addIncompleteCopy(parent);
		}
	}

	public ColumnNodeEdit(DataColumnSpec columnSpec, DataSetNodeEdit parent) {
		this(columnSpec.getName(), columnSpec, parent, EditAction.CREATE);
		if (parent.getEditAction() == EditAction.COPY) {
			parent.setEditAction(EditAction.CREATE);
		}
	}
	
	public ColumnNodeEdit(DataColumnSpec columnSpec, DataSetNodeEdit parent, int inputColumnIndex) {
		this(columnSpec.getName(), columnSpec, parent, EditAction.NO_ACTION);
		m_inputColumnIndex = inputColumnIndex;
	}
	
	ColumnNodeEdit(String inputPathFromFileWithName, DataColumnSpec columnSpec, DataSetNodeEdit parent, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), columnSpec.getName(), editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_columnSpec = columnSpec;
		parent.addColumnNodeEdit(this);
	}
	
	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent) {
		return new ColumnNodeEdit(this, parent);
	}

	DataColumnSpec getColumnSpec() {
		return m_columnSpec;
	}
	
	int getInputColumnIndex() {
		return m_inputColumnIndex;
	}

	@Override
	protected void removeFromParent() {
		((DataSetNodeEdit) getParent()).removeColumnNodeEdit(this);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		settings.addDataType(SettingsKey.COLUMN_SPEC_TYPE.getKey(), m_columnSpec.getType());
		settings.addInt(SettingsKey.INPUT_COLUMN_INDEX.getKey(), m_inputColumnIndex);
	}
	
	@Override
	protected void loadSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		m_inputColumnIndex = settings.getInt(SettingsKey.INPUT_COLUMN_INDEX.getKey());
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		parentNode.add(m_treeNode);
	}
	
	@Override
	protected boolean getValidation() {
		return true;
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
			edit.setDeletion(edit.getEditAction() != EditAction.DELETE);

        	DataSetNodeEdit parentEdit = (DataSetNodeEdit) edit.getParent();
        	if (parentEdit.getTreeNode().getChildCount() == 0) {
				parentEdit.setDeletion(true);
        	}
        	
        	edit.reloadTreeWithEditVisible();
		}
	}
}

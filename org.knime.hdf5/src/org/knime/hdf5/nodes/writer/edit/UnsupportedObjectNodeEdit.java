package org.knime.hdf5.nodes.writer.edit;

import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Class for unsupported types of hdf objects which is only there to avoid naming conflicts.
 */
public class UnsupportedObjectNodeEdit extends TreeNodeEdit {

	UnsupportedObjectNodeEdit(GroupNodeEdit parent, String name) {
		super(null, !(parent instanceof FileNodeEdit) ? parent.getOutputPathFromFileWithName() : "",
				name, EditOverwritePolicy.NONE, EditAction.NO_ACTION);
		parent.addUnsupportedObjectNodeEdit(this);
		setUnsupportedCause("Unsupported type of object");
	}
	
	@Override
	protected boolean havePropertiesChanged(Object hdfSource) {
		return false;
	}

	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
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
		((GroupNodeEdit) getParent()).removeUnsupportedObjectNodeEdit(this);
	}

	@Override
	protected void loadSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
	}

	@Override
	protected InvalidCause validateEditInternal() {
		return null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return !(edit instanceof ColumnNodeEdit) && !(edit instanceof AttributeNodeEdit) && this != edit && getName().equals(edit.getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE;
	}

	@Override
	protected EditSuccess createAction(Map<String, FlowVariable> flowVariables) {
		return null;
	}

	@Override
	protected EditSuccess copyAction() {
		return null;
	}

	@Override
	protected EditSuccess deleteAction() {
		return null;
	}

	@Override
	protected EditSuccess modifyAction() {
		return null;
	}
}

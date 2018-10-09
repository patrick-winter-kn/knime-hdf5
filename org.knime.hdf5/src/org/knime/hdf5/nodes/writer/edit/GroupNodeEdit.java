package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.JComboBox;
import javax.swing.JTextField;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

public class GroupNodeEdit extends TreeNodeEdit {
	
	private final List<GroupNodeEdit> m_groupEdits = new ArrayList<>();

	private final List<DataSetNodeEdit> m_dataSetEdits = new ArrayList<>();
	
	private final List<UnsupportedObjectNodeEdit> m_unsupportedObjectEdits = new ArrayList<>();
	
	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();

	public GroupNodeEdit(GroupNodeEdit parent, String name) {
		this(parent, null, name, EditOverwritePolicy.NONE, EditAction.CREATE);
	}
	
	private GroupNodeEdit(GroupNodeEdit parent, GroupNodeEdit copyGroup, boolean noAction) {
		this(parent, copyGroup.getInputPathFromFileWithName(), copyGroup.getName(), copyGroup.getEditOverwritePolicy(),
				noAction ? copyGroup.getEditAction() : (copyGroup.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY));
		if (getEditAction() == EditAction.COPY) {
			copyGroup.addIncompleteCopy(this);
		}
	}
	
	public GroupNodeEdit(GroupNodeEdit parent, Hdf5Group group) {
		this(parent, group.getPathFromFileWithName(), group.getName(), EditOverwritePolicy.NONE, EditAction.NO_ACTION);
		setHdfObject(group);
	}
	
	protected GroupNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name, EditOverwritePolicy editOverwritePolicy, EditAction editAction) {
		super(inputPathFromFileWithName, parent != null && !(parent instanceof FileNodeEdit)
				? parent.getOutputPathFromFileWithName() : "", name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new GroupNodeMenu());
		if (parent != null) {
			parent.addGroupNodeEdit(this);
		}
	}
	
	public GroupNodeEdit copyGroupEditTo(GroupNodeEdit parent, boolean copyWithoutChildren) throws IllegalStateException {
		if (isEditDescendant(parent)) {
			throw new IllegalStateException("Cannot add group to ifself");
		}
		
		GroupNodeEdit newGroupEdit = new GroupNodeEdit(parent, this, copyWithoutChildren);
		newGroupEdit.addEditToParentNode();
		
		for (GroupNodeEdit groupEdit : getGroupNodeEdits()) {
			if (!copyWithoutChildren || groupEdit.getEditAction().isCreateOrCopyAction()) {
				groupEdit.copyGroupEditTo(newGroupEdit, false);
			}
		}
		
		for (DataSetNodeEdit dataSetEdit : getDataSetNodeEdits()) {
			if (!copyWithoutChildren || dataSetEdit.getEditAction().isCreateOrCopyAction()) {
				dataSetEdit.copyDataSetEditTo(newGroupEdit, false);
			}
		}
		
		for (AttributeNodeEdit attributeEdit : getAttributeNodeEdits()) {
			if (!copyWithoutChildren || attributeEdit.getEditAction().isCreateOrCopyAction()) {
				attributeEdit.copyAttributeEditTo(newGroupEdit, false);
			}
		}
		
		return newGroupEdit;
	}

	public GroupNodeEdit[] getGroupNodeEdits() {
		return m_groupEdits.toArray(new GroupNodeEdit[] {});
	}
	
	public DataSetNodeEdit[] getDataSetNodeEdits() {
		return m_dataSetEdits.toArray(new DataSetNodeEdit[] {});
	}
	
	public AttributeNodeEdit[] getAttributeNodeEdits() {
		return m_attributeEdits.toArray(new AttributeNodeEdit[] {});
	}

	public void addGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.add(edit);
		edit.setParent(this);
	}

	public void addDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.add(edit);
		edit.setParent(this);
	}
	
	public void addUnsupportedObjectNodeEdit(UnsupportedObjectNodeEdit edit) {
		m_unsupportedObjectEdits.add(edit);
		edit.setParent(this);
	}
	
	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
		edit.setParent(this);
	}
	
	GroupNodeEdit getGroupNodeEdit(String inputPathFromFileWithName) {
		if (inputPathFromFileWithName != null) {
			for (GroupNodeEdit groupEdit : m_groupEdits) {
				if (inputPathFromFileWithName.equals(groupEdit.getInputPathFromFileWithName())
						&& !groupEdit.getEditAction().isCreateOrCopyAction()) {
					return groupEdit;
				}
			}
		}
		return null;
	}
	
	DataSetNodeEdit getDataSetNodeEdit(String inputPathFromFileWithName) {
		if (inputPathFromFileWithName != null) {
			for (DataSetNodeEdit dataSetEdit : m_dataSetEdits) {
				if (inputPathFromFileWithName.equals(dataSetEdit.getInputPathFromFileWithName())
						&& !dataSetEdit.getEditAction().isCreateOrCopyAction()) {
					return dataSetEdit;
				}
			}
		}
		return null;
	}
	
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

	public void removeGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		edit.setParent(null);
	}

	public void removeDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		edit.setParent(null);
	}
	
	public void removeUnsupportedObjectNodeEdit(UnsupportedObjectNodeEdit edit) {
		m_unsupportedObjectEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		edit.setParent(null);
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		edit.setParent(null);
	}
	
	/**
	 * so far with overwrite of properties
	 */
	void integrate(GroupNodeEdit copyEdit, long inputRowCount, boolean lastValidationBeforeExecution) {
		for (GroupNodeEdit copyGroupEdit : copyEdit.getGroupNodeEdits()) {
			if (copyGroupEdit.getEditAction() != EditAction.NO_ACTION) {
				if (lastValidationBeforeExecution && useOverwritePolicy(copyGroupEdit, inputRowCount)) {
					continue;
				}
				GroupNodeEdit groupEdit = getGroupNodeEdit(copyGroupEdit.getInputPathFromFileWithName());
				boolean isCreateOrCopyAction = copyGroupEdit.getEditAction().isCreateOrCopyAction();
				if (groupEdit != null && !isCreateOrCopyAction) {
					if (copyGroupEdit.getEditAction() != EditAction.MODIFY_CHILDREN_ONLY) {
						groupEdit.copyPropertiesFrom(copyGroupEdit);
					}
					groupEdit.integrate(copyGroupEdit, inputRowCount, lastValidationBeforeExecution);
					
				} else {
					copyGroupEdit.copyGroupEditTo(this, !isCreateOrCopyAction);
				}
			}
		}
		
		for (DataSetNodeEdit copyDataSetEdit : copyEdit.getDataSetNodeEdits()) {
			if (copyDataSetEdit.getEditAction() != EditAction.NO_ACTION) {
				if (lastValidationBeforeExecution && useOverwritePolicy(copyDataSetEdit, inputRowCount)) {
					continue;
				}
				DataSetNodeEdit dataSetEdit = getDataSetNodeEdit(copyDataSetEdit.getInputPathFromFileWithName());
				boolean isCreateOrCopyAction = copyDataSetEdit.getEditAction().isCreateOrCopyAction();
				if (dataSetEdit != null && !isCreateOrCopyAction) {
					if (copyDataSetEdit.getEditAction() != EditAction.MODIFY_CHILDREN_ONLY) {
						dataSetEdit.copyPropertiesFrom(copyDataSetEdit);
					}
					dataSetEdit.integrate(copyDataSetEdit, inputRowCount, lastValidationBeforeExecution);
					
				} else {
					copyDataSetEdit.copyDataSetEditTo(this, !isCreateOrCopyAction);
					for (ColumnNodeEdit copyColumnEdit : copyDataSetEdit.getColumnNodeEdits()) {
						if (copyColumnEdit.getEditAction() == EditAction.CREATE) {
							copyColumnEdit.setInputRowCount(inputRowCount);
						}
					}
				}
			}
		}
		
		for (AttributeNodeEdit copyAttributeEdit : copyEdit.getAttributeNodeEdits()) {
			if (copyAttributeEdit.getEditAction() != EditAction.NO_ACTION) {
				if (lastValidationBeforeExecution && useOverwritePolicy(copyAttributeEdit, inputRowCount)) {
					continue;
				}
				AttributeNodeEdit attributeEdit = getAttributeNodeEdit(copyAttributeEdit.getInputPathFromFileWithName(), copyAttributeEdit.getEditAction());
				boolean isCreateOrCopyAction = copyAttributeEdit.getEditAction().isCreateOrCopyAction();
				if (attributeEdit != null && !isCreateOrCopyAction) {
					attributeEdit.copyPropertiesFrom(copyAttributeEdit);
				} else {
					copyAttributeEdit.copyAttributeEditTo(this, !isCreateOrCopyAction);
				}
			}
		}
	}
	
	@Override
	protected boolean havePropertiesChanged() {
		return false;
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		// nothing to do here
	}
	
	@Override
	protected long getProgressToDoInEdit() {
		return getEditAction() != EditAction.NO_ACTION && getEditAction() != EditAction.MODIFY_CHILDREN_ONLY && getEditState() != EditState.SUCCESS ? 147L : 0L;
	}
	
	@Override
	protected TreeNodeEdit[] getAllChildren() {
		List<TreeNodeEdit> children = new ArrayList<>();
		
		children.addAll(m_groupEdits);
		children.addAll(m_dataSetEdits);
		children.addAll(m_unsupportedObjectEdits);
		children.addAll(m_attributeEdits);
		
		return children.toArray(new TreeNodeEdit[0]);
	}
	
	@Override
	protected void removeFromParent() {
		if (this instanceof FileNodeEdit) {
			throw new IllegalStateException("Cannot remove a FileNodeEdit from a parent.");
		}
		((GroupNodeEdit) getParent()).removeGroupNodeEdit(this);
	}
	
	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);
		
        NodeSettingsWO groupSettings = settings.addNodeSettings(SettingsKey.GROUPS.getKey());
        NodeSettingsWO dataSetSettings = settings.addNodeSettings(SettingsKey.DATA_SETS.getKey());
        NodeSettingsWO attributeSettings = settings.addNodeSettings(SettingsKey.ATTRIBUTES.getKey());
        
        for (GroupNodeEdit edit : m_groupEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
    	        NodeSettingsWO editSettings = groupSettings.addNodeSettings("" + edit.hashCode());
    			edit.saveSettingsTo(editSettings);
        	}
		}
		
		for (DataSetNodeEdit edit : m_dataSetEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
		        NodeSettingsWO editSettings = dataSetSettings.addNodeSettings("" + edit.hashCode());
				edit.saveSettingsTo(editSettings);
        	}
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
		/*try {
			if (!getEditAction().isCreateOrCopyAction() && !(this instanceof FileNodeEdit)) {
				Hdf5Group parent = (Hdf5Group) getParent().getHdfObject();
				if (parent != null) {
			        setHdfObject(parent.getGroup(getName()));
				}
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			// nothing to do here: edit will be invalid anyway
		}*/
		
		NodeSettingsRO groupSettings = settings.getNodeSettings(SettingsKey.GROUPS.getKey());
        Enumeration<NodeSettingsRO> groupEnum = groupSettings.children();
        while (groupEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = groupEnum.nextElement();
        	GroupNodeEdit edit = new GroupNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()), 
        			editSettings.getString(SettingsKey.NAME.getKey()), EditOverwritePolicy.values()[editSettings.getInt(SettingsKey.EDIT_OVERWRITE_POLICY.getKey())],
        			EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
            edit.loadSettingsFrom(editSettings);
        }
        
        NodeSettingsRO dataSetSettings = settings.getNodeSettings(SettingsKey.DATA_SETS.getKey());
        Enumeration<NodeSettingsRO> dataSetEnum = dataSetSettings.children();
        while (dataSetEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = dataSetEnum.nextElement();
        	DataSetNodeEdit edit = new DataSetNodeEdit(this, editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()),
        			editSettings.getString(SettingsKey.NAME.getKey()), EditOverwritePolicy.values()[editSettings.getInt(SettingsKey.EDIT_OVERWRITE_POLICY.getKey())],
        			EditAction.get(editSettings.getString(SettingsKey.EDIT_ACTION.getKey())));
    		edit.loadSettingsFrom(editSettings);
        }
        
        NodeSettingsRO attributeSettings = settings.getNodeSettings(SettingsKey.ATTRIBUTES.getKey());
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
		Hdf5Group group = (Hdf5Group) getHdfObject();
		
    	try {
    		List<String> groupNames = group.loadGroupNames();
    		for (String groupName : groupNames) {
    			Hdf5Group child = group.getGroup(groupName);
    			GroupNodeEdit childEdit = new GroupNodeEdit(this, child);
    			childEdit.addEditToParentNode();
    			childEdit.loadChildrenOfHdfObject();
    		}

    		List<String> dataSetNames = group.loadDataSetNames();
    		for (String dataSetName : dataSetNames) {
    			DataSetNodeEdit childEdit = null;
    			try {
        			Hdf5DataSet<?> child = group.updateDataSet(dataSetName);
        			if (child.getDimensions().length <= 2) {
            			childEdit = new DataSetNodeEdit(this, child);
            			childEdit.addEditToParentNode();
            			childEdit.loadChildrenOfHdfObject();
        			} else {
            			childEdit = new DataSetNodeEdit(this, child.getName(), "More than 2 dimensions");
            			childEdit.addEditToParentNode();
        			}
    			} catch (UnsupportedDataTypeException udte) {
    				// for unsupported dataSets
        			childEdit = new DataSetNodeEdit(this, dataSetName, "Unsupported data type");
        			childEdit.addEditToParentNode();
    			}
    		}
    		
    		List<String> otherObjectNames = group.loadObjectNames();
    		otherObjectNames.removeAll(groupNames);
    		otherObjectNames.removeAll(dataSetNames);
    		for (String otherObjectName : otherObjectNames) {
    			UnsupportedObjectNodeEdit childEdit = new UnsupportedObjectNodeEdit(this, otherObjectName);
    			childEdit.addEditToParentNode();
    		}
    		
    		for (String attributeName : group.loadAttributeNames()) {
    			AttributeNodeEdit childEdit = null;
    			try {
        			Hdf5Attribute<?> child = group.updateAttribute(attributeName);
        			childEdit = new AttributeNodeEdit(this, child);
        			childEdit.addEditToParentNode();
        			
    			} catch (UnsupportedDataTypeException udte) {
    				// for unsupported attributes
        			childEdit = new AttributeNodeEdit(this, attributeName, "Unsupported data type");
        			childEdit.addEditToParentNode();
    			}
    		}
    	} catch (NullPointerException npe) {
    		throw new IOException(npe.getMessage(), npe);
    	}
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		return getName().contains("/") ? InvalidCause.NAME_CHARS : null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return !(edit instanceof ColumnNodeEdit) && !(edit instanceof AttributeNodeEdit) && this != edit && getName().equals(edit.getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE
				&& (!(edit instanceof GroupNodeEdit) || getEditOverwritePolicy() == EditOverwritePolicy.NONE && edit.getEditOverwritePolicy() == EditOverwritePolicy.NONE
						|| getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE && edit.getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE);
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) {
		Hdf5Group group = null;
		
		try {
			group = ((Hdf5Group) getOpenedHdfObjectOfParent()).createGroupFromEdit(this);
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		setHdfObject(group);
		return group != null;
	}
	
	@Override
	protected boolean copyAction(ExecutionContext exec, long totalProgressToDo) {
		return createAction(null, null, false, exec, totalProgressToDo);
	}

	@Override
	protected boolean deleteAction() {
		boolean success = false;
		
		String name = Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1];
		try {
			success = ((Hdf5Group) getOpenedHdfObjectOfParent()).deleteObject(name) >= 0;
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		if (success) {
			setHdfObject((Hdf5Group) null);
		}
		return success;
	}

	@Override
	protected boolean modifyAction(ExecutionContext exec, long totalProgressToDo) {
		String oldName = Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1];
		
		boolean success = oldName.equals(getName());
		if (!success) {
			Hdf5Group newGroup = null;
			
			Hdf5Group parent = (Hdf5Group) getOpenedHdfObjectOfParent();
			try {
				newGroup = (Hdf5Group) parent.moveObject(oldName, parent, getName());
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
			
			setHdfObject(newGroup);
			success = newGroup != null;
		}
		
		return success;
	}
	
	public class GroupNodeMenu extends TreeNodeMenu {

    	private static final long serialVersionUID = -7709804406752499090L;
    	
		private GroupNodeMenu() {
			super(!(GroupNodeEdit.this instanceof FileNodeEdit), true, !(GroupNodeEdit.this instanceof FileNodeEdit));
    	}

		@Override
		protected PropertiesDialog getPropertiesDialog() {
			return new GroupPropertiesDialog();
		}

		@Override
		protected void onCreateGroup() {
			GroupNodeEdit edit = GroupNodeEdit.this;
			String newName = getUniqueName(edit, GroupNodeEdit.class, "group");
			GroupNodeEdit newEdit = new GroupNodeEdit(edit, newName);
            newEdit.addEditToParentNode();
            newEdit.reloadTreeWithEditVisible();
		}

		@Override
		protected void onDelete() {
			GroupNodeEdit edit = GroupNodeEdit.this;
			TreeNodeEdit parentOfVisible = edit.getParent();
        	edit.setDeletion(edit.getEditAction() != EditAction.DELETE);
            parentOfVisible.reloadTreeWithEditVisible(true);
		}
		
		private class GroupPropertiesDialog extends PropertiesDialog {
	    	
	    	private static final long serialVersionUID = 1254593831386973543L;
	    	
			private JTextField m_nameField = new JTextField(15);
			private JComboBox<EditOverwritePolicy> m_overwriteField = new JComboBox<>(EditOverwritePolicy.values());
	    	
			private GroupPropertiesDialog() {
				super(GroupNodeMenu.this, "Group properties");

				addProperty("Name: ", m_nameField);
				addProperty("Overwrite: ", m_overwriteField);
				
				pack();
			}
			
			@Override
			protected void loadFromEdit() {
				GroupNodeEdit edit = GroupNodeEdit.this;
				m_nameField.setText(edit.getName());
				m_overwriteField.setSelectedItem(edit.getEditOverwritePolicy());
			}

			@Override
			protected void saveToEdit() {
				GroupNodeEdit edit = GroupNodeEdit.this;
				edit.setName(m_nameField.getText());
				edit.setEditOverwritePolicy((EditOverwritePolicy) m_overwriteField.getSelectedItem());
				edit.setEditAction(EditAction.MODIFY);

				edit.reloadTreeWithEditVisible();
			}
		}
    }
}

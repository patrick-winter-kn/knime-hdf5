package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class GroupNodeEdit extends TreeNodeEdit {

	private final GroupNodeMenu m_groupNodeMenu;
	
	private final List<GroupNodeEdit> m_groupEdits = new ArrayList<>();
	
	private final List<DataSetNodeEdit> m_dataSetEdits = new ArrayList<>();
	
	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();

	public GroupNodeEdit(GroupNodeEdit parent, String name) {
		this((String) null, parent, name);
		setEditAction(EditAction.CREATE);
	}
	
	public GroupNodeEdit(GroupNodeEdit copyGroup, GroupNodeEdit parent) {
		this(copyGroup.getInputPathFromFileWithName(), parent, copyGroup.getName());
		setEditAction(copyGroup.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
		// TODO add elements in this group
	}
	
	public GroupNodeEdit(Hdf5Group group, GroupNodeEdit parent) {
		this(group.getPathFromFileWithName(), parent, group.getName());
		setEditAction(EditAction.NO_ACTION);
		setHdfObject(group);
	}
	
	protected GroupNodeEdit(String inputPathFromFileWithName, GroupNodeEdit parent, String name) {
		super(inputPathFromFileWithName, parent != null ? parent.getOutputPathFromFileWithName() : null, name);
		m_groupNodeMenu = new GroupNodeMenu();
		if (parent != null) {
			parent.addGroupNodeEdit(this);
		}
	}
	
	public GroupNodeMenu getGroupEditMenu() {
		return m_groupNodeMenu;
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
		edit.updateParentEditAction();
		edit.validate();
	}
	
	public void addDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.add(edit);
		edit.setParent(this);
		edit.updateParentEditAction();
		edit.validate();
	}
	
	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
		edit.setParent(this);
		edit.updateParentEditAction();
		edit.validate();
	}

	public boolean existsGroupNodeEdit(GroupNodeEdit edit) {
		for (GroupNodeEdit groupEdit : m_groupEdits) {
			if (edit.getName().equals(groupEdit.getName())) {
				return true;
			}
		}
		return false;
	}
	
	public boolean existsDataSetNodeEdit(DataSetNodeEdit edit) {
		for (DataSetNodeEdit dataSetEdit : m_dataSetEdits) {
			if (edit.getName().equals(dataSetEdit.getName())) {
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
	
	public GroupNodeEdit getGroupNodeEdit(String name) {
		for (GroupNodeEdit groupEdit : m_groupEdits) {
			if (name.equals(groupEdit.getName())) {
				return groupEdit;
			}
		}
		return null;
	}
	
	public DataSetNodeEdit getDataSetNodeEdit(String name) {
		for (DataSetNodeEdit dataSetEdit : m_dataSetEdits) {
			if (name.equals(dataSetEdit.getName())) {
				return dataSetEdit;
			}
		}
		return null;
	}
	
	public AttributeNodeEdit getAttributeNodeEdit(String name) {
		for (AttributeNodeEdit attributeEdit : m_attributeEdits) {
			if (name.equals(attributeEdit.getName())) {
				return attributeEdit;
			}
		}
		return null;
	}

	public void removeGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		// checkValidation(); for invalid siblings
	}
	
	public void removeDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		// checkValidation(); for invalid siblings
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
		// checkValidation(); for invalid siblings
	}
	
	/**
	 * so far with overwrite of properties
	 */
	public void integrate(GroupNodeEdit copyEdit) {
		for (GroupNodeEdit copyGroupEdit : copyEdit.getGroupNodeEdits()) {
			if (copyGroupEdit.getEditAction().isCreateOrCopyAction()) {
				addGroupNodeEdit(copyGroupEdit);
				copyGroupEdit.addEditToNode(getTreeNode());
			} else {
				getGroupNodeEdit(copyGroupEdit.getName()).integrate(copyGroupEdit);
			}
		}
		
		for (DataSetNodeEdit copyDataSetEdit : copyEdit.getDataSetNodeEdits()) {
			if (copyDataSetEdit.getEditAction().isCreateOrCopyAction()) {
				addDataSetNodeEdit(copyDataSetEdit);
				copyDataSetEdit.addEditToNode(getTreeNode());
			} else {
				getDataSetNodeEdit(copyDataSetEdit.getName()).integrate(copyDataSetEdit);
			}
		}
		
		for (AttributeNodeEdit copyAttributeEdit : copyEdit.getAttributeNodeEdits()) {
			if (!copyAttributeEdit.getEditAction().isCreateOrCopyAction()) {
				removeAttributeNodeEdit(getAttributeNodeEdit(copyAttributeEdit.getName()));
			}
			addAttributeNodeEdit(copyAttributeEdit);
			copyAttributeEdit.addEditToNode(getTreeNode());
		}
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
        NodeSettingsWO groupSettings = settings.addNodeSettings(SettingsKey.GROUPS.getKey());
        NodeSettingsWO dataSetSettings = settings.addNodeSettings(SettingsKey.DATA_SETS.getKey());
        NodeSettingsWO attributeSettings = settings.addNodeSettings(SettingsKey.ATTRIBUTES.getKey());
        
        for (GroupNodeEdit edit : m_groupEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
    	        NodeSettingsWO editSettings = groupSettings.addNodeSettings(edit.getName());
    			edit.saveSettings(editSettings);
        	}
		}
		
		for (DataSetNodeEdit edit : m_dataSetEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
		        NodeSettingsWO editSettings = dataSetSettings.addNodeSettings(edit.getName());
				edit.saveSettings(editSettings);
        	}
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
        	if (edit.getEditAction() != EditAction.NO_ACTION) {
		        NodeSettingsWO editSettings = attributeSettings.addNodeSettings(edit.getName());
				edit.saveSettings(editSettings);
        	}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		super.loadSettings(settings);

		if (!getEditAction().isCreateOrCopyAction()) {
			try {
				if (this instanceof FileNodeEdit) {
					setHdfObject(Hdf5File.openFile(((FileNodeEdit) this).getFilePath(), Hdf5File.READ_ONLY_ACCESS));
				} else {
			        setHdfObject(((Hdf5Group) getParent().getHdfObject()).getGroup(getName()));
				}
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
		}
		
		NodeSettingsRO groupSettings = settings.getNodeSettings(SettingsKey.GROUPS.getKey());
        Enumeration<NodeSettingsRO> groupEnum = groupSettings.children();
        while (groupEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = groupEnum.nextElement();
        	GroupNodeEdit edit = new GroupNodeEdit(editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()), this, 
        			editSettings.getString(SettingsKey.NAME.getKey()));
            edit.loadSettings(editSettings);
        }
        
        NodeSettingsRO dataSetSettings = settings.getNodeSettings(SettingsKey.DATA_SETS.getKey());
        Enumeration<NodeSettingsRO> dataSetEnum = dataSetSettings.children();
        while (dataSetEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = dataSetEnum.nextElement();
        	DataSetNodeEdit edit = new DataSetNodeEdit(editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()), this,
        			editSettings.getString(SettingsKey.NAME.getKey()));
    		edit.loadSettings(editSettings);
        }
        
        NodeSettingsRO attributeSettings = settings.getNodeSettings(SettingsKey.ATTRIBUTES.getKey());
        Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
        while (attributeEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = attributeEnum.nextElement();
        	try {
    			AttributeNodeEdit edit = new AttributeNodeEdit(editSettings.getString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey()), this,
    					editSettings.getString(SettingsKey.NAME.getKey()),
    					Hdf5KnimeDataType.getKnimeDataType(editSettings.getDataType(SettingsKey.KNIME_TYPE.getKey())));
    			edit.loadSettings(editSettings);
    			
    		} catch (UnsupportedDataTypeException udte) {
    			throw new InvalidSettingsException(udte.getMessage());
    		}
        }
		
		if (this instanceof FileNodeEdit && getHdfObject() != null) {
			((Hdf5File) getHdfObject()).close();
		}
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		parentNode.add(m_treeNode);
		
		for (GroupNodeEdit edit : m_groupEdits) {
	        edit.addEditToNode(m_treeNode);
		}
		
		for (DataSetNodeEdit edit : m_dataSetEdits) {
	        edit.addEditToNode(m_treeNode);
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        edit.addEditToNode(m_treeNode);
		}
	}
	
	@Override
	protected boolean getValidation() {
		return super.getValidation() && !getName().contains("/");
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return (edit instanceof GroupNodeEdit || edit instanceof DataSetNodeEdit) && !edit.equals(this) && edit.getName().equals(getName());
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		try {
			Hdf5Group parent = (Hdf5Group) getParent().getHdfObject();
			if (!parent.isFile()) {
				parent.open();
			}
			Hdf5Group group = parent.createGroup(getName());
			setHdfObject(group);
			return group != null;
			
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			return false;
		}
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

	@Override
	public boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		boolean success = super.doAction(inputTable, flowVariables, saveColumnProperties);
		
		for (AttributeNodeEdit attributeEdit : getAttributeNodeEdits()) {
			success &= attributeEdit.doAction(inputTable, flowVariables, saveColumnProperties);
		}
		
		for (DataSetNodeEdit dataSetEdit : getDataSetNodeEdits()) {
			success &= dataSetEdit.doAction(inputTable, flowVariables, saveColumnProperties);
		}
		
		for (GroupNodeEdit groupEdit : getGroupNodeEdits()) {
			success &= groupEdit.doAction(inputTable, flowVariables, saveColumnProperties);
		}
		
		return success;
	}
	
	// TODO maybe try to add it by using setComponentPopupMenu() to the respective tree node; FileNodeEdit should upd the JTree
	public class GroupNodeMenu extends JPopupMenu {

    	private static final long serialVersionUID = -7709804406752499090L;

    	private GroupPropertiesDialog m_propertiesDialog;
    	
    	private JTree m_tree;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private GroupNodeMenu() {
			if (!(GroupNodeEdit.this instanceof FileNodeEdit)) {
	    		JMenuItem itemEdit = new JMenuItem("Edit group properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (m_propertiesDialog == null) {
							m_propertiesDialog = new GroupPropertiesDialog("Group properties");
						}
						
						m_propertiesDialog.initPropertyItems((GroupNodeEdit) m_node.getUserObject());
						m_propertiesDialog.setVisible(true);
					}
				});
	    		add(itemEdit);
			}
    		
    		JMenuItem itemCreate = new JMenuItem("Create new group");
    		itemCreate.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					String newName = getUniqueName(m_node, "group");
					GroupNodeEdit newEdit = new GroupNodeEdit((GroupNodeEdit) m_node.getUserObject(), newName);
                    newEdit.addEditToNode(m_node);
                    
    				((DefaultTreeModel) (m_tree.getModel())).reload();
    				m_tree.makeVisible(new TreePath(m_node.getPath()).pathByAddingChild(m_node.getFirstChild()));
				}
			});
    		add(itemCreate);

			if (!(GroupNodeEdit.this instanceof FileNodeEdit)) {
	    		JMenuItem itemDelete = new JMenuItem("Delete");
	    		itemDelete.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof GroupNodeEdit) {
							GroupNodeEdit edit = (GroupNodeEdit) userObject;
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
	                    	if (edit.getEditAction().isCreateOrCopyAction()) {
		                    	((GroupNodeEdit) parent.getUserObject()).removeGroupNodeEdit(edit);
	                    	} else {
	                    		edit.setEditAction(EditAction.DELETE);
	                    	}
	        				
	                    	((DefaultTreeModel) (m_tree.getModel())).reload();
	        				TreePath path = new TreePath(parent.getPath());
	        				path = parent.getChildCount() > 0 ? path.pathByAddingChild(parent.getFirstChild()) : path;
	        				m_tree.makeVisible(path);
						}
					}
				});
	    		add(itemDelete);
			}
    	}
		
		public void initMenu(JTree tree, DefaultMutableTreeNode node) {
			m_tree = tree;
			m_node = node;
		}
		
		private class GroupPropertiesDialog extends PropertiesDialog<GroupNodeEdit> {
	    	
	    	private static final long serialVersionUID = 1254593831386973543L;
	    	
			private JTextField m_nameField = new JTextField(15);
	    	
			private GroupPropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(250, 150));

				JPanel namePanel = new JPanel();
				namePanel.add(m_nameField, BorderLayout.CENTER);
				addProperty("Name: ", namePanel);
			}
			
			@Override
			protected void initPropertyItems(GroupNodeEdit edit) {
				m_nameField.setText(edit.getName());
			}

			@Override
			protected void editPropertyItems() {
				TreeNodeEdit edit = (TreeNodeEdit) m_node.getUserObject();
				edit.setName(m_nameField.getText());
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(m_node.getPath()));

				edit.setEditAction(EditAction.MODIFY);
				edit.validate();
			}
		}
    }
}

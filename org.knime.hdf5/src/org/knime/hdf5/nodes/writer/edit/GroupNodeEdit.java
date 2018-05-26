package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class GroupNodeEdit extends TreeNodeEdit {

	public static final GroupNodeMenu GROUP_EDIT_MENU = new GroupNodeMenu(true);
	
	public static final GroupNodeMenu GROUP_MENU = new GroupNodeMenu(false);
	
	private final List<GroupNodeEdit> m_groupEdits = new ArrayList<>();
	
	private final List<DataSetNodeEdit> m_dataSetEdits = new ArrayList<>();
	
	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	private GroupNodeEdit(DefaultMutableTreeNode parent, String name) {
		super(parent, name);
	}
	
	private GroupNodeEdit(String name) {
		super(name);
	}
	
	private GroupNodeEdit(String pathFromFile, String name) {
		super(pathFromFile, name);
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

	private void addGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.add(edit);
	}
	
	public void addDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.add(edit);
	}
	
	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
	}

	private void removeGroupNodeEdit(GroupNodeEdit edit) {
		m_groupEdits.remove(edit);
	}
	
	public void removeDataSetNodeEdit(DataSetNodeEdit edit) {
		m_dataSetEdits.remove(edit);
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
        NodeSettingsWO groupSettings = settings.addNodeSettings("groups");
        NodeSettingsWO dataSetSettings = settings.addNodeSettings("dataSets");
        NodeSettingsWO attributeSettings = settings.addNodeSettings("attributes");
        
        for (GroupNodeEdit edit : m_groupEdits) {
	        NodeSettingsWO editSettings = groupSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
		
		for (DataSetNodeEdit edit : m_dataSetEdits) {
	        NodeSettingsWO editSettings = dataSetSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        NodeSettingsWO editSettings = attributeSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static GroupNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		GroupNodeEdit edit = new GroupNodeEdit(settings.getString("pathFromFile"), settings.getString("name"));
		
        NodeSettingsRO groupSettings = settings.getNodeSettings("groups");
        Enumeration<NodeSettingsRO> groupEnum = groupSettings.children();
        while (groupEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = groupEnum.nextElement();
        	edit.addGroupNodeEdit(GroupNodeEdit.getEditFromSettings(editSettings));
        }
        
        NodeSettingsRO dataSetSettings = settings.getNodeSettings("dataSets");
        Enumeration<NodeSettingsRO> dataSetEnum = dataSetSettings.children();
        while (dataSetEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = dataSetEnum.nextElement();
        	edit.addDataSetNodeEdit(DataSetNodeEdit.getEditFromSettings(editSettings));
        }
        
        NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
        Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
        while (attributeEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = attributeEnum.nextElement();
        	edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		return edit;
	}

	@SuppressWarnings("unchecked")
	public static GroupNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		GroupNodeEdit edit = new GroupNodeEdit(settings.getString("name"));
		
        NodeSettingsRO groupSettings = settings.getNodeSettings("groups");
        Enumeration<NodeSettingsRO> groupEnum = groupSettings.children();
        while (groupEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = groupEnum.nextElement();
        	edit.addGroupNodeEdit(GroupNodeEdit.getEditFromSettings(editSettings));
        }
        
        NodeSettingsRO dataSetSettings = settings.getNodeSettings("dataSets");
        Enumeration<NodeSettingsRO> dataSetEnum = dataSetSettings.children();
        while (dataSetEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = dataSetEnum.nextElement();
        	edit.addDataSetNodeEdit(DataSetNodeEdit.getEditFromSettings(editSettings));
        }
        
        NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
        Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
        while (attributeEnum.hasMoreElements()) {
        	NodeSettingsRO editSettings = attributeEnum.nextElement();
        	edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		return edit;
	}
	
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		parentNode.add(node);
		
		for (GroupNodeEdit edit : m_groupEdits) {
	        edit.addEditToNode(node);
		}
		
		for (DataSetNodeEdit edit : m_dataSetEdits) {
	        edit.addEditToNode(node);
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        edit.addEditToNode(node);
		}
	}
	
	// TODO maybe try to add it by using setComponentPopupMenu() to the respective tree node
	
	public static class GroupNodeMenu extends JPopupMenu {

    	private static final long serialVersionUID = -7709804406752499090L;

    	private static GroupPropertiesDialog propertiesDialog;
    	
    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private GroupNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit group properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (propertiesDialog == null) {
							propertiesDialog = new GroupPropertiesDialog("Group properties");
						}
						
						propertiesDialog.initPropertyItems((GroupNodeEdit) m_node.getUserObject());
						propertiesDialog.setVisible(true);
					}
				});
	    		add(itemEdit);
    		}
    		
    		JMenuItem itemCreate = new JMenuItem("Create group");
    		itemCreate.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					String newName = "group[" + m_node.getChildCount() + "]";
					Object userObject = m_node.getUserObject();
					GroupNodeEdit newEdit = null;
					if (userObject instanceof GroupNodeEdit) {
						GroupNodeEdit edit = (GroupNodeEdit) userObject;
                		newEdit = new GroupNodeEdit(newName);
                		edit.addGroupNodeEdit(newEdit);
                		
            		} else {
                		newEdit = new GroupNodeEdit(m_node, newName);
                    	m_editTreeConfig.addGroupNodeEdit(newEdit);
            		}
                	m_node.add(new DefaultMutableTreeNode(newEdit));
    				((DefaultTreeModel) (m_tree.getModel())).reload();
    				m_tree.makeVisible(new TreePath(m_node.getPath()).pathByAddingChild(m_node.getFirstChild()));
				}
			});
    		add(itemCreate);
    		
    		if (fromTreeNodeEdit) {
        		JMenuItem itemDelete = new JMenuItem("Delete group");
        		itemDelete.addActionListener(new ActionListener() {
    				
    				@Override
    				public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof GroupNodeEdit) {
							GroupNodeEdit edit = (GroupNodeEdit) userObject;
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
	                    	Object parentObject = parent.getUserObject();
	                    	if (parentObject instanceof GroupNodeEdit) {
	    						((GroupNodeEdit) parentObject).removeGroupNodeEdit(edit);
	                    		
	                		} else {
		                    	m_editTreeConfig.removeGroupNodeEdit(edit);
	                		}
	                    	parent.remove(m_node);
	        				((DefaultTreeModel) (m_tree.getModel())).reload();
	        				m_tree.makeVisible(new TreePath(parent.getPath()));
						}
    				}
    			});
        		add(itemDelete);
    		}
    	}
		
		public void initMenu(JTree tree, EditTreeConfiguration editTreeConfig, DefaultMutableTreeNode node) {
			m_tree = tree;
			m_editTreeConfig = editTreeConfig;
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
				addProperty("Name: ", namePanel, false);
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
			}
		}
    }
}

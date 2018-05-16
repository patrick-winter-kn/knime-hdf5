package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
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
	
	public static class GroupNodeMenu extends JPopupMenu {

    	private static final long serialVersionUID = -7709804406752499090L;

    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private GroupNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit group properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof TreeNodeEdit) {
							TreeNodeEdit edit = (TreeNodeEdit) userObject;
							Frame parent = (Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree);
							JDialog dialog = new JDialog(parent, "Group properties");
				            dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
							dialog.setSize(200, 200);
							dialog.setLocation(400, 400);
							dialog.setVisible(true);
							JPanel panel = new JPanel(new BorderLayout());
							dialog.add(panel, BorderLayout.PAGE_START);
							JLabel nameLabel = new JLabel("Name: ");
							JTextField nameField = new JTextField(4);
							nameField.setText(edit.getName());
							panel.add(nameLabel, BorderLayout.WEST);
							panel.add(nameField, BorderLayout.CENTER);
							JButton okButton = new JButton("OK");
							okButton.addActionListener(new ActionListener() {
								
								@Override
								public void actionPerformed(ActionEvent e) {
									edit.setName(nameField.getText());
									dialog.setVisible(false);
				    				((DefaultTreeModel) (m_tree.getModel())).reload();
								}
							});
							dialog.add(okButton, BorderLayout.PAGE_END);
						}
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
	                    	m_editTreeConfig.removeGroupNodeEdit(edit);
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
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
}

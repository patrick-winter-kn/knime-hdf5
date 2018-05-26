package org.knime.hdf5.nodes.writer.edit;

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class AttributeNodeEdit extends TreeNodeEdit {

	public static final AttributeNodeMenu ATTRIBUTE_EDIT_MENU = new AttributeNodeMenu(true);
	
	public static final AttributeNodeMenu ATTRIBUTE_MENU = new AttributeNodeMenu(false);
	
	private final Hdf5KnimeDataType m_knimeType;
	
	private Hdf5HdfDataType m_hdfType;

	public AttributeNodeEdit(DefaultMutableTreeNode parent, FlowVariable var) {
		super(parent, var.getName());
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
	}

	public AttributeNodeEdit(FlowVariable var) {
		super(var.getName());
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
	}

	private AttributeNodeEdit(String pathFromFile, String name, Hdf5KnimeDataType knimetype) {
		super(pathFromFile, name);
		m_knimeType = knimetype;
	}
	
	private AttributeNodeEdit(String name, Hdf5KnimeDataType knimetype) {
		super(name);
		m_knimeType = knimetype;
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}

	public Hdf5HdfDataType getHdfType() {
		return m_hdfType;
	}

	private void setHdfType(Hdf5HdfDataType hdfType) {
		m_hdfType = hdfType;
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		try {
			settings.addDataType("knimeType", m_knimeType.getColumnDataType());
		} catch (UnsupportedDataTypeException udte) {
			// TODO exception
		}
	}

	public static AttributeNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			return new AttributeNodeEdit(settings.getString("pathFromFile"), settings.getString("name"), Hdf5KnimeDataType.getKnimeDataType(settings.getDataType("knimeType")));
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	public static AttributeNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			return new AttributeNodeEdit(settings.getString("name"), Hdf5KnimeDataType.getKnimeDataType(settings.getDataType("knimeType")));
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		parentNode.add(new DefaultMutableTreeNode(this));
	}
	
	public static class AttributeNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = -6418394582185524L;

		private static AttributePropertiesDialog propertiesDialog;
    	
    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private AttributeNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit attribute properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (propertiesDialog == null) {
							propertiesDialog = new AttributePropertiesDialog("Attribute properties");
						}
						
						propertiesDialog.initPropertyItems((AttributeNodeEdit) m_node.getUserObject());
						propertiesDialog.setVisible(true);
					}
				});
	    		add(itemEdit);
    		}
    		
    		if (fromTreeNodeEdit) {
        		JMenuItem itemDelete = new JMenuItem("Delete attribute");
        		itemDelete.addActionListener(new ActionListener() {
    				
    				@Override
    				public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof AttributeNodeEdit) {
							AttributeNodeEdit edit = (AttributeNodeEdit) userObject;
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
	                    	Object parentObject = parent.getUserObject();
	                    	if (parentObject instanceof DataSetNodeEdit) {
	    						((DataSetNodeEdit) parentObject).removeAttributeNodeEdit(edit);
	                    		
	                		} else if (parentObject instanceof GroupNodeEdit) {
	    						((GroupNodeEdit) parentObject).removeAttributeNodeEdit(edit);
	                    		
	                		} else {
		                    	m_editTreeConfig.removeAttributeNodeEdit(edit);
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
		
		private class AttributePropertiesDialog extends PropertiesDialog<AttributeNodeEdit> {
	    	
	    	private static final long serialVersionUID = 1254593831386973543L;
	    	
			private JTextField m_nameField = new JTextField(15);
	    	
			private AttributePropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(300, 300));

				addProperty("Name: ", m_nameField, false);

				JComboBox<HdfDataType> typeField = new JComboBox<>(((AttributeNodeEdit) m_node.getUserObject()).getKnimeType().getConvertibleTypes().toArray(new HdfDataType[] {}));
				addProperty("Type: ", typeField, false);

				JComboBox<String> endianField = new JComboBox<>(new String[] {"little endian", "big endian"});
				addProperty("Endian: ", endianField, false);
				
				JPanel sizeField = new JPanel();
				ButtonGroup sizeGroup = new ButtonGroup();
				JRadioButton auto = new JRadioButton("auto");
				sizeField.add(auto);
				sizeGroup.add(auto);
				auto.setSelected(true);
				JRadioButton fixed = new JRadioButton("fixed");
				sizeField.add(fixed);
				sizeGroup.add(fixed);
				JSpinner size = new JSpinner();
				sizeField.add(size);
				size.setEnabled(false);
				fixed.addChangeListener(new ChangeListener() {
					
					@Override
					public void stateChanged(ChangeEvent e) {
						size.setEnabled(fixed.isSelected());
					}
				});
				addProperty("Size: ", sizeField, false);

				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				JRadioButton no = new JRadioButton("no");
				overwriteField.add(no);
				overwriteGroup.add(no);
				no.setSelected(true);
				JRadioButton yes = new JRadioButton("yes");
				overwriteField.add(yes);
				overwriteGroup.add(yes);
				addProperty("Overwrite: ", overwriteField, false);
			}
			
			@Override
			protected void initPropertyItems(AttributeNodeEdit edit) {
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

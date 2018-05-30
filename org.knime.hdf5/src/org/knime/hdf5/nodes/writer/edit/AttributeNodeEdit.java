package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
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
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class AttributeNodeEdit extends TreeNodeEdit {

	private final AttributeNodeMenu m_attributeEditMenu;
	
	private final String m_flowVariableName; 
	
	private final Hdf5KnimeDataType m_knimeType;
	
	private HdfDataType m_hdfType;
	
	// TODO use an enum here
	private boolean m_littleEndian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private boolean m_overwrite;

	public AttributeNodeEdit(DefaultMutableTreeNode parent, FlowVariable var) {
		super(parent, var.getName());
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = var.getName();
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_hdfType = m_knimeType.getEquivalentHdfType();
	}

	public AttributeNodeEdit(FlowVariable var) {
		super(var.getName());
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = var.getName();
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_hdfType = m_knimeType.getEquivalentHdfType();
	}

	private AttributeNodeEdit(String pathFromFile, String name, String varName, Hdf5KnimeDataType knimetype) {
		super(pathFromFile, name);
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = varName;
		m_knimeType = knimetype;
	}
	
	private AttributeNodeEdit(String name, String varName, Hdf5KnimeDataType knimetype) {
		super(name);
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = varName;
		m_knimeType = knimetype;
	}
	
	public AttributeNodeMenu getAttributeEditMenu() {
		return m_attributeEditMenu;
	}

	public String getFlowVariableName() {
		return m_flowVariableName;
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}

	public HdfDataType getHdfType() {
		return m_hdfType;
	}

	private void setHdfType(HdfDataType hdfType) {
		m_hdfType = hdfType;
	}

	public boolean isLittleEndian() {
		return m_littleEndian;
	}

	private void setLittleEndian(boolean littleEndian) {
		m_littleEndian = littleEndian;
	}

	public boolean isFixed() {
		return m_fixed;
	}

	private void setFixed(boolean fixed) {
		m_fixed = fixed;
	}

	public int getStringLength() {
		return m_stringLength;
	}

	private void setStringLength(int stringLength) {
		m_stringLength = stringLength;
	}

	public boolean isOverwrite() {
		return m_overwrite;
	}

	private void setOverwrite(boolean overwrite) {
		m_overwrite = overwrite;
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		settings.addString("flowVariableName", m_flowVariableName);
		
		try {
			settings.addDataType("knimeType", m_knimeType.getColumnDataType());
		} catch (UnsupportedDataTypeException udte) {
			settings.addDataType("knimeType", null);
		}
		
		settings.addString("hdfType", m_hdfType.toString());
		settings.addBoolean("littleEndian", m_littleEndian);
		settings.addBoolean("fixed", m_fixed);
		settings.addInt("stringLength", m_stringLength);
		settings.addBoolean("overwrite", m_overwrite);
	}

	public static AttributeNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			AttributeNodeEdit edit = new AttributeNodeEdit(settings.getString("pathFromFile"), settings.getString("name"),
					settings.getString("flowVariableName"), Hdf5KnimeDataType.getKnimeDataType(settings.getDataType("knimeType")));
			
			edit.loadProperties(settings);
			
			return edit;
			
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	public static AttributeNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			AttributeNodeEdit edit = new AttributeNodeEdit(settings.getString("name"), settings.getString("flowVariableName"),
					Hdf5KnimeDataType.getKnimeDataType(settings.getDataType("knimeType")));
			
			edit.loadProperties(settings);
			
			return edit;
			
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	private void loadProperties(final NodeSettingsRO settings) throws InvalidSettingsException {
		setHdfType(HdfDataType.valueOf(settings.getString("hdfType")));
		setLittleEndian(settings.getBoolean("littleEndian"));
		setFixed(settings.getBoolean("fixed"));
		setStringLength(settings.getInt("stringLength"));
		setOverwrite(settings.getBoolean("overwrite"));
	}
	
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		parentNode.add(new DefaultMutableTreeNode(this));
	}
	
	public class AttributeNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = -6418394582185524L;

		private AttributePropertiesDialog m_propertiesDialog;
    	
    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private AttributeNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit attribute properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (m_propertiesDialog == null) {
							m_propertiesDialog = new AttributePropertiesDialog("Attribute properties");
						}
						
						m_propertiesDialog.initPropertyItems((AttributeNodeEdit) m_node.getUserObject());
						m_propertiesDialog.setVisible(true);
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
	    	
			private static final long serialVersionUID = 9201153080744087510L;
	    	
			private JTextField m_nameField = new JTextField(15);
			private JComboBox<HdfDataType> m_typeField = new JComboBox<>(((AttributeNodeEdit) m_node.getUserObject()).getKnimeType().getConvertibleHdfTypes().toArray(new HdfDataType[] {}));
			// TODO this should also be an enum
			private JComboBox<String> m_endianField = new JComboBox<>(new String[] {"little endian", "big endian"});
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner();
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			
			private AttributePropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(300, 300));

				addProperty("Name: ", m_nameField, false);
				addProperty("Type: ", m_typeField, false);
				addProperty("Endian: ", m_endianField, false);
				
				JPanel stringLengthField = new JPanel();
				ButtonGroup stringLengthGroup = new ButtonGroup();
				stringLengthField.add(m_stringLengthAuto, BorderLayout.WEST);
				stringLengthGroup.add(m_stringLengthAuto);
				m_stringLengthAuto.setSelected(true);
				stringLengthField.add(m_stringLengthFixed, BorderLayout.CENTER);
				stringLengthGroup.add(m_stringLengthFixed);
				stringLengthField.add(m_stringLengthSpinner, BorderLayout.EAST);
				m_stringLengthSpinner.setEnabled(false);
				m_stringLengthFixed.addChangeListener(new ChangeListener() {
					
					@Override
					public void stateChanged(ChangeEvent e) {
						m_stringLengthSpinner.setEnabled(m_stringLengthFixed.isSelected());
					}
				});
				addProperty("String length: ", stringLengthField, false);

				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				overwriteField.add(m_overwriteNo);
				overwriteGroup.add(m_overwriteNo);
				m_overwriteNo.setSelected(true);
				overwriteField.add(m_overwriteYes);
				overwriteGroup.add(m_overwriteYes);
				addProperty("Overwrite: ", overwriteField, false);
			}
			
			@Override
			protected void initPropertyItems(AttributeNodeEdit edit) {
				m_nameField.setText(edit.getName());
				m_typeField.setSelectedItem(edit.getHdfType());
				m_endianField.setSelectedItem(edit.isLittleEndian() ? "little endian" : "big endian");
				m_stringLengthAuto.setSelected(!edit.isFixed());
				m_stringLengthFixed.setSelected(edit.isFixed());
				m_stringLengthSpinner.setValue(edit.getStringLength());
				m_overwriteNo.setSelected(!edit.isOverwrite());
				m_overwriteYes.setSelected(edit.isOverwrite());
			}

			@Override
			protected void editPropertyItems() {
				AttributeNodeEdit edit = (AttributeNodeEdit) m_node.getUserObject();
				edit.setName(m_nameField.getText());
				edit.setHdfType((HdfDataType) m_typeField.getSelectedItem());
				edit.setLittleEndian(m_endianField.getSelectedItem().equals("little endian"));
				edit.setFixed(m_stringLengthFixed.isSelected());
				edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				edit.setOverwrite(m_overwriteYes.isSelected());
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(m_node.getPath()));
			}
		}
    }
}

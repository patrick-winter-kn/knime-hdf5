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
import javax.swing.SpinnerNumberModel;
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
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class AttributeNodeEdit extends TreeNodeEdit {

	private final AttributeNodeMenu m_attributeEditMenu;
	
	private final String m_flowVariableName; 
	
	private final Hdf5KnimeDataType m_knimeType;
	
	private HdfDataType m_hdfType;
	
	private Endian m_endian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private boolean m_overwrite;

	public AttributeNodeEdit(DefaultMutableTreeNode parent, FlowVariable var) {
		super(parent, var.getName());
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = var.getName();
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_hdfType = m_knimeType.getEquivalentHdfType();
		m_endian = Endian.LITTLE_ENDIAN;
		m_stringLength = var.getValueAsString().length();
	}

	public AttributeNodeEdit(FlowVariable var) {
		super(var.getName());
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = var.getName();
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_hdfType = m_knimeType.getEquivalentHdfType();
		m_endian = Endian.LITTLE_ENDIAN;
		m_stringLength = var.getValueAsString().length();
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

	public Endian getEndian() {
		return m_endian;
	}

	private void setEndian(Endian endian) {
		m_endian = endian;
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
		settings.addString(SettingsKey.FLOW_VARIABLE_NAME.getKey(), m_flowVariableName);
		
		try {
			settings.addDataType(SettingsKey.KNIME_TYPE.getKey(), m_knimeType.getColumnDataType());
		} catch (UnsupportedDataTypeException udte) {
			settings.addDataType(SettingsKey.KNIME_TYPE.getKey(), null);
		}
		
		settings.addString(SettingsKey.HDF_TYPE.getKey(), m_hdfType.toString());
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addBoolean(SettingsKey.FIXED.getKey(), m_fixed);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
		settings.addBoolean(SettingsKey.OVERWRITE.getKey(), m_overwrite);
	}

	public static AttributeNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			AttributeNodeEdit edit = new AttributeNodeEdit(settings.getString(SettingsKey.PATH_FROM_FILE.getKey()), settings.getString(SettingsKey.NAME.getKey()),
					settings.getString(SettingsKey.FLOW_VARIABLE_NAME.getKey()), Hdf5KnimeDataType.getKnimeDataType(settings.getDataType(SettingsKey.KNIME_TYPE.getKey())));
			
			edit.loadProperties(settings);
			
			return edit;
			
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	public static AttributeNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			AttributeNodeEdit edit = new AttributeNodeEdit(settings.getString(SettingsKey.NAME.getKey()), settings.getString(SettingsKey.FLOW_VARIABLE_NAME.getKey()),
					Hdf5KnimeDataType.getKnimeDataType(settings.getDataType(SettingsKey.KNIME_TYPE.getKey())));
			
			edit.loadProperties(settings);
			
			return edit;
			
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	private void loadProperties(final NodeSettingsRO settings) throws InvalidSettingsException {
		setHdfType(HdfDataType.valueOf(settings.getString(SettingsKey.HDF_TYPE.getKey())));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		setOverwrite(settings.getBoolean(SettingsKey.OVERWRITE.getKey()));
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		parentNode.add(node);
		node.setAllowsChildren(false);
		m_treeNode = node;
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
		
		public void initMenu(JTree tree, EditTreeConfiguration editTreeConfig, DefaultMutableTreeNode node) {
			m_tree = tree;
			m_editTreeConfig = editTreeConfig;
			m_node = node;
		}
		
		private class AttributePropertiesDialog extends PropertiesDialog<AttributeNodeEdit> {
	    	
			private static final long serialVersionUID = 9201153080744087510L;
	    	
			private JTextField m_nameField = new JTextField(15);
			private JComboBox<HdfDataType> m_typeField = new JComboBox<>(((AttributeNodeEdit) m_node.getUserObject()).getKnimeType().getConvertibleHdfTypes().toArray(new HdfDataType[] {}));
			private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			
			private AttributePropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(400, 300));

				addProperty("Name: ", m_nameField);
				
				m_typeField.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						boolean isString = (HdfDataType) m_typeField.getSelectedItem() == HdfDataType.STRING;
						m_endianField.setEnabled(!isString);
						m_stringLengthAuto.setEnabled(isString);
						m_stringLengthFixed.setEnabled(isString);
						m_stringLengthSpinner.setEnabled(isString && m_stringLengthFixed.isSelected());
					}
				});
				addProperty("Type: ", m_typeField);
				
				m_endianField.setSelectedItem(Endian.LITTLE_ENDIAN);
				addProperty("Endian: ", m_endianField);
				
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
				addProperty("String length: ", stringLengthField);

				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				overwriteField.add(m_overwriteNo);
				overwriteGroup.add(m_overwriteNo);
				m_overwriteNo.setSelected(true);
				overwriteField.add(m_overwriteYes);
				overwriteGroup.add(m_overwriteYes);
				addProperty("Overwrite: ", overwriteField);
			}
			
			@Override
			protected void initPropertyItems(AttributeNodeEdit edit) {
				m_nameField.setText(edit.getName());
				m_typeField.setSelectedItem(edit.getHdfType());
				m_endianField.setSelectedItem(edit.getEndian());
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
				edit.setEndian((Endian) m_endianField.getSelectedItem());
				edit.setFixed(m_stringLengthFixed.isSelected());
				edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				edit.setOverwrite(m_overwriteYes.isSelected());
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(m_node.getPath()));
			}
		}
    }
}

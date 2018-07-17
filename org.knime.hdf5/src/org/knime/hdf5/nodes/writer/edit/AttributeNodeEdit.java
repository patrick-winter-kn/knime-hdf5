package org.knime.hdf5.nodes.writer.edit;

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
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
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class AttributeNodeEdit extends TreeNodeEdit {

	private final AttributeNodeMenu m_attributeEditMenu;
	
	private final String m_flowVariableName; 
	
	private final Hdf5KnimeDataType m_knimeType;
	
	private HdfDataType m_hdfType;

	private boolean m_compoundAsArrayPossible;
	
	private boolean m_compoundAsArrayUsed;
	
	private Endian m_endian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private int m_compoundItemStringLength;
	
	private boolean m_overwrite;

	public AttributeNodeEdit(DefaultMutableTreeNode parent, FlowVariable var) {
		super(parent, var.getName().replaceAll("\\\\/", "/"));
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = var.getName();
		Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_endian = Endian.LITTLE_ENDIAN;
		m_stringLength = var.getValueAsString().length();
		try {
			Object[] values = Hdf5Attribute.getFlowVariableValues(var);
			knimeType = Hdf5KnimeDataType.getKnimeDataType(values);
			for (Object value : values) {
				int newStringLength = value.toString().length();
				m_compoundItemStringLength = newStringLength > m_compoundItemStringLength ? newStringLength : m_compoundItemStringLength;;
			}
		} catch (UnsupportedDataTypeException e) {
			m_compoundItemStringLength = m_stringLength;
		}
		m_knimeType = knimeType;
		m_hdfType = knimeType.getEquivalentHdfType();
		m_compoundAsArrayPossible = m_compoundItemStringLength != m_stringLength;
		m_compoundAsArrayUsed = m_compoundAsArrayPossible;
	}

	public AttributeNodeEdit(FlowVariable var) {
		this(null, var);
	}

	private AttributeNodeEdit(String pathFromFile, String name, String varName, Hdf5KnimeDataType knimetype) {
		super(pathFromFile, name);
		m_attributeEditMenu = new AttributeNodeMenu(true);
		m_flowVariableName = varName;
		m_knimeType = knimetype;
	}
	
	private AttributeNodeEdit(String name, String varName, Hdf5KnimeDataType knimetype) {
		this(null, name, varName, knimetype);
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
	
	private void setCompoundAsArrayPossible(boolean compoundAsArrayPossible) {
		m_compoundAsArrayPossible = compoundAsArrayPossible;
	}

	public boolean isCompoundAsArrayUsed() {
		return m_compoundAsArrayUsed;
	}
	
	private void setCompoundAsArrayUsed(boolean compoundAsArrayUsed) {
		m_compoundAsArrayUsed = compoundAsArrayUsed;
	}

	public Endian getEndian() {
		return m_endian;
	}

	private void setEndian(Endian endian) {
		m_endian = endian;
	}

	private boolean isFixed() {
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

	public int getCompoundItemStringLength() {
		return m_compoundItemStringLength;
	}

	private void setCompoundItemStringLength(int compoundItemStringLength) {
		m_compoundItemStringLength = compoundItemStringLength;
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
		settings.addBoolean(SettingsKey.COMPOUND_AS_ARRAY_POSSIBLE.getKey(), m_compoundAsArrayPossible);
		settings.addBoolean(SettingsKey.COMPOUND_AS_ARRAY_USED.getKey(), m_compoundAsArrayUsed);
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addBoolean(SettingsKey.FIXED.getKey(), m_fixed);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
		settings.addInt(SettingsKey.COMPOUND_ITEM_STRING_LENGTH.getKey(), m_compoundItemStringLength);
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
		setCompoundAsArrayPossible(settings.getBoolean(SettingsKey.COMPOUND_AS_ARRAY_POSSIBLE.getKey()));
		setCompoundAsArrayUsed(settings.getBoolean(SettingsKey.COMPOUND_AS_ARRAY_USED.getKey()));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		setCompoundItemStringLength(settings.getInt(SettingsKey.COMPOUND_ITEM_STRING_LENGTH.getKey()));
		setOverwrite(settings.getBoolean(SettingsKey.OVERWRITE.getKey()));
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		parentNode.add(node);
		node.setAllowsChildren(false);
		m_treeNode = node;
	}

	@Override
	protected boolean getValidation() {
		return super.getValidation();
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof AttributeNodeEdit && !edit.equals(this) && edit.getName().equals(getName());
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
			private JCheckBox m_compoundAsArrayField = new JCheckBox();
			private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			
			private AttributePropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(450, 300));

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
				
				if (m_compoundAsArrayPossible) {
					m_compoundAsArrayField.addChangeListener(new ChangeListener() {
						
						@Override
						public void stateChanged(ChangeEvent e) {
							boolean selected = m_compoundAsArrayField.isSelected();
							m_typeField.setEnabled(selected);
							if (!selected) {
								m_typeField.setSelectedItem(HdfDataType.STRING);
							}
							m_stringLengthSpinner.setValue(selected ? m_compoundItemStringLength : m_stringLength);
						}
					});
				} else {
					m_compoundAsArrayField.setEnabled(false);
				}
				addProperty("Use values from flowVariable array", m_compoundAsArrayField);
				
				m_endianField.setSelectedItem(Endian.LITTLE_ENDIAN);
				addProperty("Endian: ", m_endianField);
				
				JPanel stringLengthField = new JPanel();
				ButtonGroup stringLengthGroup = new ButtonGroup();
				stringLengthField.setLayout(new GridBagLayout());
				GridBagConstraints constraints = new GridBagConstraints();
				constraints.fill = GridBagConstraints.HORIZONTAL;
				constraints.gridx = 0;
				constraints.gridy = 0;
				constraints.weightx = 0.0;
				stringLengthField.add(m_stringLengthAuto, constraints);
				stringLengthGroup.add(m_stringLengthAuto);
				constraints.gridx++;
				stringLengthField.add(m_stringLengthFixed, constraints);
				stringLengthGroup.add(m_stringLengthFixed);
				constraints.gridx++;
				constraints.weightx = 1.0;
				stringLengthField.add(m_stringLengthSpinner, constraints);
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
				overwriteField.add(m_overwriteYes);
				overwriteGroup.add(m_overwriteYes);
				addProperty("Overwrite: ", overwriteField);
			}
			
			@Override
			protected void initPropertyItems(AttributeNodeEdit edit) {
				m_nameField.setText(edit.getName());
				m_typeField.setSelectedItem(edit.getHdfType());
				m_compoundAsArrayField.setSelected(edit.isCompoundAsArrayUsed());
				m_endianField.setSelectedItem(edit.getEndian());
				m_stringLengthAuto.setSelected(!edit.isFixed());
				m_stringLengthFixed.setSelected(edit.isFixed());
				m_stringLengthSpinner.setValue(edit.isCompoundAsArrayUsed() ? edit.getCompoundItemStringLength() : edit.getStringLength());
				m_overwriteNo.setSelected(!edit.isOverwrite());
				m_overwriteYes.setSelected(edit.isOverwrite());
			}

			@Override
			protected void editPropertyItems() {
				AttributeNodeEdit edit = (AttributeNodeEdit) m_node.getUserObject();
				edit.setName(m_nameField.getText());
				edit.setHdfType((HdfDataType) m_typeField.getSelectedItem());
				edit.setCompoundAsArrayUsed(m_compoundAsArrayField.isSelected());
				edit.setEndian((Endian) m_endianField.getSelectedItem());
				edit.setFixed(m_stringLengthFixed.isSelected());
				if (edit.isCompoundAsArrayUsed()) {
					edit.setCompoundItemStringLength((Integer) m_stringLengthSpinner.getValue());
				} else {
					edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				}
				edit.setOverwrite(m_overwriteYes.isSelected());
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(m_node.getPath()));
				
				edit.validate();
			}
		}
    }
}

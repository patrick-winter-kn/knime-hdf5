package org.knime.hdf5.nodes.writer.edit;

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class AttributeNodeEdit extends TreeNodeEdit {
	
	private Hdf5KnimeDataType m_knimeType;
	
	private HdfDataType m_hdfType;

	private boolean m_compoundAsArrayPossible;
	
	private boolean m_compoundAsArrayUsed;
	
	private Endian m_endian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private int m_compoundItemStringLength;
	
	private boolean m_overwrite;

	public AttributeNodeEdit(FlowVariable var, TreeNodeEdit parent) {
		this(var.getName(), parent, var.getName().replaceAll("\\\\/", "/"), Hdf5KnimeDataType.getKnimeDataType(var.getType()));
		updatePropertiesFromFlowVariable(var);
		setEditAction(EditAction.CREATE);
	}

	private AttributeNodeEdit(AttributeNodeEdit copyAttribute, TreeNodeEdit parent) {
		this(copyAttribute.getInputPathFromFileWithName(), parent, copyAttribute.getName(), copyAttribute.getKnimeType());
		m_hdfType = copyAttribute.getHdfType();
		m_compoundAsArrayPossible = copyAttribute.isCompoundAsArrayPossible();
		m_compoundAsArrayUsed = copyAttribute.isCompoundAsArrayUsed();
		m_endian = copyAttribute.getEndian();
		m_fixed = copyAttribute.isFixed();
		m_stringLength = copyAttribute.getStringLength();
		m_compoundItemStringLength = copyAttribute.getCompoundItemStringLength();
		m_overwrite = copyAttribute.isOverwrite();
		setEditAction(copyAttribute.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
	}

	public AttributeNodeEdit(Hdf5Attribute<?> attribute, TreeNodeEdit parent) {
		this(attribute.getPathFromFile() + attribute.getName().replaceAll("/", "\\\\/"), parent, attribute.getName(), attribute.getType().getKnimeType());
		m_hdfType = m_knimeType.getEquivalentHdfType();
		m_endian = Endian.LITTLE_ENDIAN;
		
		if (attribute.getType().isHdfType(HdfDataType.STRING)) {
			m_stringLength = (int) attribute.getType().getHdfType().getStringLength();
		} else {
			Object[] values = attribute.getValue() == null ? attribute.read() : attribute.getValue();
			for (Object value : values) {
				int newStringLength = value.toString().length();
				m_stringLength = newStringLength > m_stringLength ? newStringLength : m_stringLength;
			}
		}
		
		setEditAction(EditAction.NO_ACTION);
		setHdfObject(attribute);
	}

	private AttributeNodeEdit(String inputPathFromFileWithName, TreeNodeEdit parent, String name, Hdf5KnimeDataType knimetype) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_knimeType = knimetype;
		if (parent instanceof GroupNodeEdit) {
			((GroupNodeEdit) parent).addAttributeNodeEdit(this);
			
		} else if (parent instanceof DataSetNodeEdit) {
			((DataSetNodeEdit) parent).addAttributeNodeEdit(this);
			
		} else {
			throw new IllegalArgumentException("Error for \"" + getOutputPathFromFileWithName()
					+ "\": AttributeNodeEdits can only exist in GroupNodeEdits or DataSetNodeEdits");
		}
	}
	
	AttributeNodeEdit(String inputPathFromFileWithName, GroupNodeEdit parent, String name, Hdf5KnimeDataType knimetype) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_knimeType = knimetype;
		parent.addAttributeNodeEdit(this);
	}
	
	AttributeNodeEdit(String inputPathFromFileWithName, DataSetNodeEdit parent, String name, Hdf5KnimeDataType knimetype) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_knimeType = knimetype;
		parent.addAttributeNodeEdit(this);
	}
	
	public AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent) {
		return new AttributeNodeEdit(this, parent);
	}
	
	private void updatePropertiesFromFlowVariable(FlowVariable var) {
		Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_endian = Endian.LITTLE_ENDIAN;
		m_stringLength = var.getValueAsString().length();
		try {
			Object[] values = Hdf5Attribute.getFlowVariableValues(var);
			knimeType = Hdf5KnimeDataType.getKnimeDataType(values);
			for (Object value : values) {
				int newStringLength = value.toString().length();
				m_compoundItemStringLength = newStringLength > m_compoundItemStringLength ? newStringLength : m_compoundItemStringLength;
			}
		} catch (UnsupportedDataTypeException e) {
			m_compoundItemStringLength = m_stringLength;
		}
		m_knimeType = knimeType;
		m_hdfType = knimeType.getEquivalentHdfType();
		m_compoundAsArrayPossible = m_compoundItemStringLength != m_stringLength;
		m_compoundAsArrayUsed = m_compoundAsArrayPossible;
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
	
	private boolean isCompoundAsArrayPossible() {
		return m_compoundAsArrayPossible;
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

	@Override
	protected void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		super.loadSettings(settings);
		
    	try {
	        if (!getEditAction().isCreateOrCopyAction()) {
	        	setHdfObject(((Hdf5TreeElement) getParent().getHdfObject()).getAttribute(
	        			Hdf5Attribute.getPathAndName(getInputPathFromFileWithName())[1]));
	        }
		} catch (IOException ioe) {
			// nothing to do here: edit will be invalid anyway
		}
		
		setHdfType(HdfDataType.valueOf(settings.getString(SettingsKey.HDF_TYPE.getKey())));
		setCompoundAsArrayPossible(settings.getBoolean(SettingsKey.COMPOUND_AS_ARRAY_POSSIBLE.getKey()));
		setCompoundAsArrayUsed(settings.getBoolean(SettingsKey.COMPOUND_AS_ARRAY_USED.getKey()));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		setCompoundItemStringLength(settings.getInt(SettingsKey.COMPOUND_ITEM_STRING_LENGTH.getKey()));
		setOverwrite(settings.getBoolean(SettingsKey.OVERWRITE.getKey()));

		validate();
	}
	
	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		parentNode.add(m_treeNode);
		
		validate();
	}

	@Override
	protected boolean getValidation() {
		return super.getValidation();
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof AttributeNodeEdit && !edit.equals(this) && edit.getName().equals(getName());
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		
		Hdf5Attribute<?> newAttribute = null;
		FlowVariable var = flowVariables.get(getInputPathFromFileWithName());
		try {
			newAttribute = parent.createAndWriteAttribute(this, var);
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		setHdfObject(newAttribute);
		
		return newAttribute != null;
	}

	@Override
	protected boolean copyAction() {
		Hdf5Attribute<?> copyAttribute = null;
		try {
			copyAttribute = ((Hdf5File) getRoot().getHdfObject()).getAttributeByPath(getInputPathFromFileWithName());
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}

		Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		
		Hdf5Attribute<?> newAttribute = null;
		try {
			newAttribute = parent.createAndWriteAttribute(this, copyAttribute);
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		setHdfObject(newAttribute);
		
		return newAttribute != null;
	}

	@Override
	protected boolean deleteAction() {
		Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		
		boolean success = parent.deleteAttribute(Hdf5Attribute.getPathAndName(getInputPathFromFileWithName())[1]) >= 0;
		if (success) {
			setHdfObject((Hdf5Attribute<?>) null);
		}
		
		return success;
	}

	@Override
	protected boolean modifyAction() {
		// TODO maybe find a way without deleting and recreating it
		Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
		if (!parent.isFile()) {
			parent.open();
		}
		
		String oldName = Hdf5Attribute.getPathAndName(getInputPathFromFileWithName())[1];
		Hdf5Attribute<?> tempAttribute = null;
		try {
			tempAttribute = parent.copyAttribute(TreeNodeEdit.getUniqueName(parent.loadAttributeNames(), getName() + "(1)"), parent.getAttribute(oldName));
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		boolean success = tempAttribute != null;
		success &= parent.deleteAttribute(oldName) >= 0;
		
		try {
			Hdf5Attribute<?> newAttribute = parent.createAndWriteAttribute(this, tempAttribute);
			setHdfObject(newAttribute);
			setInputPathFromFileWithName(newAttribute.getPathFromFileWithName());
			success &= newAttribute != null;
			
		} catch (IOException ioe) {
			success = false;
			try {
				Hdf5Attribute<?> changedNameAttribute = parent.copyAttribute(TreeNodeEdit.getUniqueName(parent.loadAttributeNames(), getName()), tempAttribute);
				setHdfObject(changedNameAttribute);
				setInputPathFromFileWithName(changedNameAttribute.getPathFromFileWithName());
				
			} catch (IOException ioe2) {
				NodeLogger.getLogger(getClass()).error(ioe2.getMessage(), ioe2);
			}
		}

		success &= parent.deleteAttribute(tempAttribute.getName()) >= 0;
		
		return success;
	}
	
	public class AttributeNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = -6418394582185524L;

		private AttributePropertiesDialog m_propertiesDialog;
    	
		private AttributeNodeMenu() {
			super(true, false, true);
    	}
		
		@Override
		protected void onEdit() {
			if (m_propertiesDialog == null) {
				m_propertiesDialog = new AttributePropertiesDialog("Attribute properties");
			}
			
			m_propertiesDialog.initPropertyItems();
			m_propertiesDialog.setVisible(true);
		}

		@Override
		protected void onDelete() {
			AttributeNodeEdit edit = AttributeNodeEdit.this;
        	TreeNodeEdit parentEdit = edit.getParent();
        	if (edit.getEditAction().isCreateOrCopyAction() || edit.getHdfObject() == null) {
            	if (parentEdit instanceof DataSetNodeEdit) {
					((DataSetNodeEdit) parentEdit).removeAttributeNodeEdit(edit);
        		} else if (parentEdit instanceof GroupNodeEdit) {
					((GroupNodeEdit) parentEdit).removeAttributeNodeEdit(edit);	
        		}
        	} else {
        		edit.setDeletion(edit.getEditAction() != EditAction.DELETE);
        	}
        	edit.reloadTreeWithEditVisible();
		}
		
		private class AttributePropertiesDialog extends PropertiesDialog<AttributeNodeEdit> {
	    	
			private static final long serialVersionUID = 9201153080744087510L;
	    	
			private JTextField m_nameField = new JTextField(15);
			private JComboBox<HdfDataType> m_typeField = new JComboBox<>(getKnimeType().getConvertibleHdfTypes().toArray(new HdfDataType[] {}));
			private JCheckBox m_compoundAsArrayField = new JCheckBox();
			private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			
			private AttributePropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, AttributeNodeMenu.this), title);
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
			protected void initPropertyItems() {
				AttributeNodeEdit edit = AttributeNodeEdit.this;
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
				AttributeNodeEdit edit = AttributeNodeEdit.this;
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
				
				edit.setEditAction(EditAction.MODIFY);
				edit.validate();

				edit.reloadTreeWithEditVisible();
			}
		}
    }
}

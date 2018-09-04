package org.knime.hdf5.nodes.writer.edit;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

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
	
	private HdfDataType m_inputType;
	
	private HdfDataType m_outputType;
	
	private List<HdfDataType> m_possibleOutputTypes;
	
	private Endian m_endian;
	
	private boolean m_fixed;
	
	private int m_stringLength;

	private boolean m_compoundAsArrayPossible;
	
	private boolean m_compoundAsArrayUsed;
	
	private int m_compoundItemStringLength;
	
	private boolean m_overwrite;

	public AttributeNodeEdit(TreeNodeEdit parent, FlowVariable var) {
		this(parent, var.getName(), var.getName().replaceAll("\\\\/", "/"),
				HdfDataType.getHdfDataType(var.getType()), EditAction.CREATE);
		updatePropertiesFromFlowVariable(var);
	}

	private AttributeNodeEdit(TreeNodeEdit parent, AttributeNodeEdit copyAttribute) {
		this(parent, copyAttribute.getInputPathFromFileWithName(), copyAttribute.getName(), copyAttribute.getInputType(),
				copyAttribute.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
		copyAdditionalPropertiesFrom(copyAttribute);
		if (getEditAction() == EditAction.COPY) {
			copyAttribute.addIncompleteCopy(this);
		}
	}

	public AttributeNodeEdit(TreeNodeEdit parent, Hdf5Attribute<?> attribute) {
		this(parent, attribute.getPathFromFile() + attribute.getName().replaceAll("/", "\\\\/"), attribute.getName(),
				attribute.getType().getHdfType().getType(), EditAction.NO_ACTION);
		m_outputType = m_inputType;
		m_possibleOutputTypes = HdfDataType.getConvertibleTypes(attribute);
		m_endian = attribute.getType().getHdfType().getEndian();
		
		if (attribute.getType().isHdfType(HdfDataType.STRING)) {
			m_stringLength = (int) attribute.getType().getHdfType().getStringLength();
		} else {
			Object[] values = attribute.getValue() == null ? attribute.read() : attribute.getValue();
			for (Object value : values) {
				int newStringLength = value.toString().length();
				m_stringLength = newStringLength > m_stringLength ? newStringLength : m_stringLength;
			}
		}
		
		setHdfObject(attribute);
	}

	private AttributeNodeEdit(TreeNodeEdit parent, String inputPathFromFileWithName, String name, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		if (parent instanceof GroupNodeEdit) {
			((GroupNodeEdit) parent).addAttributeNodeEdit(this);
			
		} else if (parent instanceof DataSetNodeEdit) {
			((DataSetNodeEdit) parent).addAttributeNodeEdit(this);
			
		} else {
			throw new IllegalArgumentException("Error for \"" + getOutputPathFromFileWithName()
					+ "\": AttributeNodeEdits can only exist in GroupNodeEdits or DataSetNodeEdits");
		}
	}
	
	AttributeNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		// TODO test if this really works
		m_possibleOutputTypes = m_inputType.getPossiblyConvertibleHdfTypes();
		parent.addAttributeNodeEdit(this);
	}
	
	AttributeNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, String name, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		// TODO test if this really works
		m_possibleOutputTypes = m_inputType.getPossiblyConvertibleHdfTypes();
		parent.addAttributeNodeEdit(this);
	}
	
	public AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent) {
		return new AttributeNodeEdit(parent, this);
	}
	
	private void updatePropertiesFromFlowVariable(FlowVariable var) {
		// TODO maybe work directly with HdfDataType for inputType
		Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
		m_endian = Endian.LITTLE_ENDIAN;
		m_stringLength = var.getValueAsString().length();
		Object[] values = Hdf5Attribute.getFlowVariableValues(var);
		knimeType = Hdf5KnimeDataType.getKnimeDataType(values);
		m_inputType = knimeType.getEquivalentHdfType();
		m_outputType = m_inputType;
		m_possibleOutputTypes = HdfDataType.getConvertibleTypes(var);
		
		for (Object value : values) {
			int newStringLength = value.toString().length();
			m_compoundItemStringLength = newStringLength > m_compoundItemStringLength ? newStringLength : m_compoundItemStringLength;
		}
		m_compoundAsArrayPossible = m_compoundItemStringLength != m_stringLength;
		m_compoundAsArrayUsed = m_compoundAsArrayPossible;
	}
	
	public HdfDataType getInputType() {
		return m_inputType;
	}

	public HdfDataType getOutputType() {
		return m_outputType;
	}

	private void setOutputType(HdfDataType outputType) {
		m_outputType = outputType;
	}

	private HdfDataType[] getPossibleOutputTypes() {
		return m_possibleOutputTypes.toArray(new HdfDataType[0]);
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
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		if (copyEdit instanceof AttributeNodeEdit) {
			AttributeNodeEdit copyAttributeEdit = (AttributeNodeEdit) copyEdit;
			m_outputType = copyAttributeEdit.getOutputType();
			m_possibleOutputTypes = Arrays.asList(copyAttributeEdit.getPossibleOutputTypes());
			m_compoundAsArrayPossible = copyAttributeEdit.isCompoundAsArrayPossible();
			m_compoundAsArrayUsed = copyAttributeEdit.isCompoundAsArrayUsed();
			m_endian = copyAttributeEdit.getEndian();
			m_fixed = copyAttributeEdit.isFixed();
			m_stringLength = copyAttributeEdit.getStringLength();
			m_compoundItemStringLength = copyAttributeEdit.getCompoundItemStringLength();
			m_overwrite = copyAttributeEdit.isOverwrite();
		}
	}
	
	@Override
	public String getToolTipText() {
		return "(" + m_outputType.toString() + ") " + super.getToolTipText();
	}
	
	@Override
	protected TreeNodeEdit[] getAllChildren() {
		return new TreeNodeEdit[0];
	}

	@Override
	protected void removeFromParent() {
    	TreeNodeEdit parentEdit = getParent();
		if (parentEdit instanceof DataSetNodeEdit) {
			((DataSetNodeEdit) parentEdit).removeAttributeNodeEdit(this);
		} else if (parentEdit instanceof GroupNodeEdit) {
			((GroupNodeEdit) parentEdit).removeAttributeNodeEdit(this);	
		}
		setParent(null);
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);

		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		settings.addInt(SettingsKey.OUTPUT_TYPE.getKey(), m_outputType.getTypeId());
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addBoolean(SettingsKey.FIXED.getKey(), m_fixed);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
		settings.addBoolean(SettingsKey.COMPOUND_AS_ARRAY_POSSIBLE.getKey(), m_compoundAsArrayPossible);
		settings.addBoolean(SettingsKey.COMPOUND_AS_ARRAY_USED.getKey(), m_compoundAsArrayUsed);
		settings.addInt(SettingsKey.COMPOUND_ITEM_STRING_LENGTH.getKey(), m_compoundItemStringLength);
		settings.addBoolean(SettingsKey.OVERWRITE.getKey(), m_overwrite);
	}

	@Override
	protected void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
	        if (!getEditAction().isCreateOrCopyAction()) {
	        	Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
	        	if (parent != null) {
		        	setHdfObject(parent.getAttribute(Hdf5Attribute.getPathAndName(getInputPathFromFileWithName())[1]));
	        	}
	        }
		} catch (IOException ioe) {
			// nothing to do here: edit will be invalid anyway
		}
		
		setOutputType(HdfDataType.get(settings.getInt(SettingsKey.OUTPUT_TYPE.getKey())));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		setCompoundAsArrayPossible(settings.getBoolean(SettingsKey.COMPOUND_AS_ARRAY_POSSIBLE.getKey()));
		setCompoundAsArrayUsed(settings.getBoolean(SettingsKey.COMPOUND_AS_ARRAY_USED.getKey()));
		setCompoundItemStringLength(settings.getInt(SettingsKey.COMPOUND_ITEM_STRING_LENGTH.getKey()));
		setOverwrite(settings.getBoolean(SettingsKey.OVERWRITE.getKey()));
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		return null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof AttributeNodeEdit && !edit.equals(this) && edit.getName().equals(getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE;
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
    	
		private AttributeNodeMenu() {
			super(true, false, true);
    	}
		
		@Override
		protected PropertiesDialog getPropertiesDialog() {
			return new AttributePropertiesDialog();
		}

		@Override
		protected void onDelete() {
			AttributeNodeEdit edit = AttributeNodeEdit.this;
			TreeNodeEdit parentOfVisible = edit.getParent();
        	edit.setDeletion(edit.getEditAction() != EditAction.DELETE);
            parentOfVisible.reloadTreeWithEditVisible(true);
		}
		
		private class AttributePropertiesDialog extends PropertiesDialog {
	    	
			private static final long serialVersionUID = 9201153080744087510L;
	    	
			private JTextField m_nameField = new JTextField(15);
			private JComboBox<HdfDataType> m_typeField = new JComboBox<>(getPossibleOutputTypes());
			private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
			private JCheckBox m_compoundAsArrayField = new JCheckBox();
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			
			private AttributePropertiesDialog() {
				super(AttributeNodeMenu.this, "Attribute properties");
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
				m_typeField.setSelectedItem(edit.getOutputType());
				m_endianField.setSelectedItem(edit.getEndian());
				m_stringLengthAuto.setSelected(!edit.isFixed());
				m_stringLengthFixed.setSelected(edit.isFixed());
				m_stringLengthSpinner.setValue(edit.isCompoundAsArrayUsed() ? edit.getCompoundItemStringLength() : edit.getStringLength());
				m_compoundAsArrayField.setSelected(edit.isCompoundAsArrayUsed());
				m_overwriteNo.setSelected(!edit.isOverwrite());
				m_overwriteYes.setSelected(edit.isOverwrite());
			}

			@Override
			protected void editPropertyItems() {
				AttributeNodeEdit edit = AttributeNodeEdit.this;
				edit.setName(m_nameField.getText());
				edit.setOutputType((HdfDataType) m_typeField.getSelectedItem());
				edit.setEndian((Endian) m_endianField.getSelectedItem());
				edit.setFixed(m_stringLengthFixed.isSelected());
				edit.setCompoundAsArrayUsed(m_compoundAsArrayField.isSelected());
				if (edit.isCompoundAsArrayUsed()) {
					edit.setCompoundItemStringLength((Integer) m_stringLengthSpinner.getValue());
				} else {
					edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				}
				edit.setOverwrite(m_overwriteYes.isSelected());
				edit.setEditAction(EditAction.MODIFY);

				edit.reloadTreeWithEditVisible();
			}
		}
    }
}

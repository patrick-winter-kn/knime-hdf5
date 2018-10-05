package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
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
import org.knime.hdf5.nodes.writer.edit.EditDataType.DataTypeChooser;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

public class AttributeNodeEdit extends TreeNodeEdit {
	
	private HdfDataType m_inputType;
	
	private List<HdfDataType> m_possibleOutputTypes;
	
	private EditDataType m_editDataType = new EditDataType();
	
	private int m_totalStringLength;
	
	private int m_itemStringLength;

	private boolean m_flowVariableArrayPossible;
	
	private boolean m_flowVariableArrayUsed;
	
	public AttributeNodeEdit(TreeNodeEdit parent, FlowVariable var) {
		this(parent, var.getName(), var.getName().replaceAll("\\\\/", "/"), EditOverwritePolicy.NONE,
				HdfDataType.getHdfDataType(var.getType()), EditAction.CREATE);
		updatePropertiesFromFlowVariable(var);
	}

	private AttributeNodeEdit(TreeNodeEdit parent, AttributeNodeEdit copyAttribute, boolean noAction) {
		this(parent, copyAttribute.getInputPathFromFileWithName(), copyAttribute.getName(), copyAttribute.getEditOverwritePolicy(), copyAttribute.getInputType(),
				noAction ? copyAttribute.getEditAction() : (copyAttribute.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY));
		copyAdditionalPropertiesFrom(copyAttribute);
		if (getEditAction() == EditAction.COPY) {
			copyAttribute.addIncompleteCopy(this);
		}
	}
	
	AttributeNodeEdit(TreeNodeEdit parent, String name, String unsupportedCause) {
		this(parent, null, name, EditOverwritePolicy.NONE, null, EditAction.NO_ACTION);
		setUnsupportedCause(unsupportedCause != null ? unsupportedCause : "");
		// remove menu to consider the edit is unsupported
		setTreeNodeMenu(null);
	}

	public AttributeNodeEdit(TreeNodeEdit parent, Hdf5Attribute<?> attribute) {
		this(parent, attribute.getPathFromFile() + attribute.getName().replaceAll("/", "\\\\/"), attribute.getName(), EditOverwritePolicy.NONE,
				attribute.getType().getHdfType().getType(), EditAction.NO_ACTION);
		
		if (attribute.getType().isHdfType(HdfDataType.STRING)) {
			m_itemStringLength = (int) attribute.getType().getHdfType().getStringLength();
		} else {
			Object[] values = attribute.getValue() == null ? attribute.read() : attribute.getValue();
			for (Object value : values) {
				int newStringLength = value.toString().length();
				m_itemStringLength = newStringLength > m_itemStringLength ? newStringLength : m_itemStringLength;
			}
		}
		m_editDataType.setValues(m_inputType, attribute.getType().getHdfType().getEndian(), Rounding.DOWN, false, m_itemStringLength);
		m_totalStringLength = (int) Math.max(attribute.getDimension(), 1) * m_itemStringLength;
		m_possibleOutputTypes = HdfDataType.getConvertibleTypes(attribute);
		
		setHdfObject(attribute);
	}

	private AttributeNodeEdit(TreeNodeEdit parent, String inputPathFromFileWithName, String name, EditOverwritePolicy editOverwritePolicy, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, !(parent instanceof FileNodeEdit) ? parent.getOutputPathFromFileWithName() : "", name, editOverwritePolicy, editAction);
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
	
	AttributeNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name, EditOverwritePolicy editOverwritePolicy, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, !(parent instanceof FileNodeEdit) ? parent.getOutputPathFromFileWithName() : "", name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		parent.addAttributeNodeEdit(this);
	}
	
	AttributeNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, String name, EditOverwritePolicy editOverwritePolicy, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		parent.addAttributeNodeEdit(this);
	}
	
	public AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent, boolean copyWithoutChildren) {
		AttributeNodeEdit newAttributeEdit = new AttributeNodeEdit(parent, this, copyWithoutChildren);
		newAttributeEdit.addEditToParentNode();
		
		return newAttributeEdit;
	}
	
	private void updatePropertiesFromFlowVariable(FlowVariable var) {
		Object[] values = Hdf5Attribute.getFlowVariableValues(var);
		m_inputType = Hdf5KnimeDataType.getKnimeDataType(values).getEquivalentHdfType();
		m_possibleOutputTypes = HdfDataType.getConvertibleTypes(m_inputType, values);
		m_totalStringLength = var.getValueAsString().length();
		
		for (Object value : values) {
			int newStringLength = value.toString().length();
			m_itemStringLength = newStringLength > m_itemStringLength ? newStringLength : m_itemStringLength;
		}
		m_flowVariableArrayPossible = m_itemStringLength != m_totalStringLength;
		m_flowVariableArrayUsed = m_flowVariableArrayPossible;
		
		m_editDataType.setValues(m_inputType, Endian.LITTLE_ENDIAN, Rounding.DOWN, false, m_itemStringLength);
	}
	
	public HdfDataType getInputType() {
		return m_inputType;
	}

	private HdfDataType[] getPossibleOutputTypes() {
		return m_possibleOutputTypes.toArray(new HdfDataType[0]);
	}

	public EditDataType getEditDataType() {
		return m_editDataType;
	}
	
	private int getTotalStringLength() {
		return m_totalStringLength;
	}

	public int getItemStringLength() {
		return m_itemStringLength;
	}

	private void setItemStringLength(int itemStringLength) {
		m_itemStringLength = itemStringLength;
	}
	
	private boolean isFlowVariableArrayPossible() {
		return m_flowVariableArrayPossible;
	}
	
	private void setFlowVariableArrayPossible(boolean flowVariableArrayPossible) {
		m_flowVariableArrayPossible = flowVariableArrayPossible;
	}

	public boolean isFlowVariableArrayUsed() {
		return m_flowVariableArrayUsed;
	}
	
	private void setFlowVariableArrayUsed(boolean flowVariableArrayUsed) {
		m_flowVariableArrayUsed = flowVariableArrayUsed;
	}

	private boolean havePropertiesChanged() {
		boolean propertiesChanged = true;
		
		if (getInputPathFromFileWithName() != null) {
			try {
				Hdf5Attribute<?> copyAttribute = ((Hdf5File) getRoot().getHdfObject()).getAttributeByPath(getInputPathFromFileWithName());
				
				propertiesChanged = m_inputType != m_editDataType.getOutputType()
						|| copyAttribute.getType().getHdfType().getEndian() != m_editDataType.getEndian()
						|| m_itemStringLength != m_editDataType.getStringLength();
				
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
		}
		
		return propertiesChanged;
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		if (copyEdit instanceof AttributeNodeEdit) {
			AttributeNodeEdit copyAttributeEdit = (AttributeNodeEdit) copyEdit;
			m_possibleOutputTypes = Arrays.asList(copyAttributeEdit.getPossibleOutputTypes());
			m_editDataType.setValues(copyAttributeEdit.getEditDataType());
			m_totalStringLength = copyAttributeEdit.getTotalStringLength();
			m_itemStringLength = copyAttributeEdit.getItemStringLength();
			m_flowVariableArrayPossible = copyAttributeEdit.isFlowVariableArrayPossible();
			m_flowVariableArrayUsed = copyAttributeEdit.isFlowVariableArrayUsed();
		}
	}
	
	@Override
	public String getToolTipText() {
		return (isSupported() ? "(" + m_editDataType.getOutputType().toString() + ") " : "") + super.getToolTipText();
	}
	
	@Override
	protected long getProgressToDoInEdit() {
		long totalToDo = 0L;
		
		if (getEditAction() != EditAction.NO_ACTION && getEditState() != EditState.SUCCESS) {
			totalToDo += 71L;
			if (havePropertiesChanged()) {
				HdfDataType dataType = m_editDataType.getOutputType();
				long dataTypeToDo = dataType.isNumber() ? dataType.getSize()/8 : m_editDataType.getStringLength();
				totalToDo += m_totalStringLength / m_itemStringLength * dataTypeToDo;
			}
		}
		
		return totalToDo;
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
	}

	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);

		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		
		int[] typeIds = new int[m_possibleOutputTypes.size()];
		for (int i = 0; i < typeIds.length; i++) {
			typeIds[i] = m_possibleOutputTypes.get(i).getTypeId();
		}
		settings.addIntArray(SettingsKey.POSSIBLE_OUTPUT_TYPES.getKey(), typeIds);
		
		m_editDataType.saveSettingsTo(settings);
		settings.addInt(SettingsKey.TOTAL_STRING_LENGTH.getKey(), m_totalStringLength);
		settings.addInt(SettingsKey.ITEM_STRING_LENGTH.getKey(), m_itemStringLength);
		settings.addBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_POSSIBLE.getKey(), m_flowVariableArrayPossible);
		settings.addBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_USED.getKey(), m_flowVariableArrayUsed);
	}

	@Override
	protected void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		/*try {
	        if (!getEditAction().isCreateOrCopyAction()) {
	        	Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
	        	if (parent != null) {
		        	setHdfObject(parent.getAttribute(Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1]));
	        	}
	        }
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			// nothing to do here: edit will be invalid anyway
		}*/

		setEditOverwritePolicy(EditOverwritePolicy.values()[settings.getInt(SettingsKey.EDIT_OVERWRITE_POLICY.getKey())]);
		
		m_possibleOutputTypes = new ArrayList<>();
		int[] typeIds = settings.getIntArray(SettingsKey.POSSIBLE_OUTPUT_TYPES.getKey());
		for (int typeId : typeIds) {
			m_possibleOutputTypes.add(HdfDataType.get(typeId));
		}
		
		m_editDataType.loadSettingsFrom(settings);
		m_totalStringLength = settings.getInt(SettingsKey.TOTAL_STRING_LENGTH.getKey());
		setItemStringLength(settings.getInt(SettingsKey.ITEM_STRING_LENGTH.getKey()));
		setFlowVariableArrayPossible(settings.getBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_POSSIBLE.getKey()));
		setFlowVariableArrayUsed(settings.getBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_USED.getKey()));
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		return null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof AttributeNodeEdit && this != edit && getName().equals(edit.getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE
				&& (getEditOverwritePolicy() == EditOverwritePolicy.NONE && edit.getEditOverwritePolicy() == EditOverwritePolicy.NONE
						|| getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE && edit.getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE);
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) {
		Hdf5Attribute<?> newAttribute = null;
		
		FlowVariable var = flowVariables.get(getInputPathFromFileWithName());
		try {
			newAttribute = getOpenedHdfObjectOfParent().createAndWriteAttribute(this, var);
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		setHdfObject(newAttribute);
		return newAttribute != null;
	}

	@Override
	protected boolean copyAction(ExecutionContext exec, long totalProgressToDo) {
		Hdf5Attribute<?> newAttribute = null;

		// TODO maybe find a way to use this: Hdf5Attribute<?> copyAttribute = (Hdf5Attribute<?>) getCopyEdit().getHdfObject();
		try {
			Hdf5Attribute<?> copyAttribute = ((Hdf5File) getRoot().getHdfObject()).getAttributeByPath(getInputPathFromFileWithName());
			if (havePropertiesChanged()) {
				newAttribute = getOpenedHdfObjectOfParent().createAndWriteAttribute(this, copyAttribute);
			} else {
				newAttribute = getOpenedHdfObjectOfParent().copyAttribute(getName(), copyAttribute);
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		setHdfObject(newAttribute);
		return newAttribute != null;
	}

	@Override
	protected boolean deleteAction() {
		boolean success = false;

		String name = Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1];
		try {
			success = getOpenedHdfObjectOfParent().deleteAttribute(name) >= 0;
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
		}
		
		if (success) {
			setHdfObject((Hdf5Attribute<?>) null);
		}
		return success;
	}

	@Override
	protected boolean modifyAction(ExecutionContext exec, long totalProgressToDo) {
		boolean success = true;
		
		Hdf5TreeElement parent = getOpenedHdfObjectOfParent();
		String oldName = Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1];
		
		if (!havePropertiesChanged()/* && getOutputPathFromFile().equals(pathAndName[0])*/) {
			if (!oldName.equals(getName())) {
				success = parent.renameAttribute(oldName, getName());
			}
		} else {
			Hdf5Attribute<?> tempAttribute = null;
			try {
				tempAttribute = parent.copyAttribute(TreeNodeEdit.getUniqueName(parent.loadAttributeNames(), getName() + "(1)"), parent.getAttribute(oldName));
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
			success = tempAttribute != null;
			try {
				success &= parent.deleteAttribute(oldName) >= 0;
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
			
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
			
			try {
				success &= parent.deleteAttribute(tempAttribute.getName()) >= 0;
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			}
		}
		
		return success;
	}
	
	public class AttributeNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = -6418394582185524L;
    	
		private AttributeNodeMenu() {
			super(AttributeNodeEdit.this.isSupported(), false, true);
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
			private JComboBox<EditOverwritePolicy> m_overwriteField = new JComboBox<>(EditOverwritePolicy.getValuesWithoutIntegrate());
			private DataTypeChooser m_dataTypeChooser = m_editDataType.new DataTypeChooser(false);
			private JCheckBox m_flowVariableArrayField = new JCheckBox();
			
			private AttributePropertiesDialog() {
				super(AttributeNodeMenu.this, "Attribute properties");

				addProperty("Name: ", m_nameField);
				addProperty("Overwrite: ", m_overwriteField);
				m_dataTypeChooser.addToPropertiesDialog(this);
				
				if (m_flowVariableArrayPossible) {
					m_flowVariableArrayField.addChangeListener(new ChangeListener() {
						
						@Override
						public void stateChanged(ChangeEvent e) {
							boolean selected = m_flowVariableArrayField.isSelected();
							m_dataTypeChooser.setOnlyStringSelectable(!selected,
									selected ? m_itemStringLength : m_totalStringLength);
						}
					});
				} else {
					m_flowVariableArrayField.setEnabled(false);
				}
				addProperty("Use values from flowVariable array: ", m_flowVariableArrayField);
				
				pack();
			}
			
			@Override
			protected void loadFromEdit() {
				AttributeNodeEdit edit = AttributeNodeEdit.this;
				m_nameField.setText(edit.getName());
				m_overwriteField.setSelectedItem(edit.getEditOverwritePolicy());
				m_dataTypeChooser.loadFromDataType(m_possibleOutputTypes, m_inputType == HdfDataType.FLOAT32);
				m_flowVariableArrayField.setSelected(edit.isFlowVariableArrayUsed());
			}

			@Override
			protected void saveToEdit() {
				AttributeNodeEdit edit = AttributeNodeEdit.this;
				edit.setName(m_nameField.getText());
				edit.setEditOverwritePolicy((EditOverwritePolicy) m_overwriteField.getSelectedItem());
				m_dataTypeChooser.saveToDataType();
				edit.setFlowVariableArrayUsed(m_flowVariableArrayField.isSelected());
				edit.setEditAction(EditAction.MODIFY);

				edit.reloadTreeWithEditVisible();
			}
		}
    }
}

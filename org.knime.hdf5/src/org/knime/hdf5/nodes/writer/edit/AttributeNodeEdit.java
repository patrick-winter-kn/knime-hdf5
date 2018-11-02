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
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.DataTypeChooser;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

public class AttributeNodeEdit extends TreeNodeEdit {
	
	private InvalidCause m_inputInvalidCause;
	
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

	private AttributeNodeEdit(TreeNodeEdit parent, AttributeNodeEdit copyAttribute, boolean needsCopySource) {
		this(parent, copyAttribute.getInputPathFromFileWithName(), copyAttribute.getName(), copyAttribute.getEditOverwritePolicy(), copyAttribute.getInputType(),
				needsCopySource ? (copyAttribute.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY) : copyAttribute.getEditAction());
		copyAdditionalPropertiesFrom(copyAttribute);
		if (needsCopySource && getEditAction() == EditAction.COPY) {
			setCopyEdit(copyAttribute);
		}
	}
	
	AttributeNodeEdit(TreeNodeEdit parent, String name, String unsupportedCause) {
		this(parent, null, name, EditOverwritePolicy.NONE, null, EditAction.NO_ACTION);
		setUnsupportedCause(unsupportedCause != null ? unsupportedCause : "");
		// remove menu to consider the edit is unsupported
		setTreeNodeMenu(null);
	}

	public AttributeNodeEdit(TreeNodeEdit parent, Hdf5Attribute<?> attribute) throws IOException {
		this(parent, attribute.getPathFromFile() + attribute.getName().replaceAll("/", "\\\\/"), attribute.getName(), EditOverwritePolicy.NONE,
				attribute.getType().getHdfType().getType(), EditAction.NO_ACTION);

		Object[] values = attribute.getValue() == null ? attribute.read() : attribute.getValue();
		if (attribute.getType().isHdfType(HdfDataType.STRING)) {
			m_itemStringLength = (int) attribute.getType().getHdfType().getStringLength();
		} else {
			for (Object value : values) {
				int newStringLength = value.toString().length();
				m_itemStringLength = newStringLength > m_itemStringLength ? newStringLength : m_itemStringLength;
			}
		}
		m_editDataType.setValues(m_inputType, attribute.getType().getHdfType().getEndian(), Rounding.DOWN, false, m_itemStringLength);
		m_totalStringLength = (int) Math.max(attribute.getDimension(), 1) * m_itemStringLength;
		m_possibleOutputTypes = HdfDataType.getConvertibleTypes(m_inputType, values);
		
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
	
	public AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent) {
		return copyAttributeEditTo(parent, true);
	}
	
	AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent, boolean needsCopySource) {
		AttributeNodeEdit newAttributeEdit = new AttributeNodeEdit(parent, this, needsCopySource);
		newAttributeEdit.addEditToParentNodeIfPossible();
		
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
	
	InvalidCause getInputInvalidCause() {
		return m_inputInvalidCause;
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

	@Override
	protected boolean havePropertiesChanged(Object hdfSource) {
		boolean propertiesChanged = true;
		
		Hdf5Attribute<?> copyAttribute = (Hdf5Attribute<?>) hdfSource;
		
		if (hdfSource != null) {
			propertiesChanged = m_inputType != m_editDataType.getOutputType()
					|| copyAttribute.getType().getHdfType().getEndian() != m_editDataType.getEndian()
					|| m_itemStringLength != m_editDataType.getStringLength();
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
			if (havePropertiesChanged(getEditAction() != EditAction.CREATE ? findCopySource() : null)) {
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
		return getName().startsWith(BACKUP_PREFIX) && !getOutputPathFromFileWithName(true).equals(getInputPathFromFileWithName())
				? InvalidCause.NAME_BACKUP_PREFIX : null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		boolean conflictPossible = edit instanceof AttributeNodeEdit && this != edit;
		boolean inConflict = conflictPossible && getName().equals(edit.getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE;
		
		return inConflict ? !avoidsOverwritePolicyNameConflict(edit) : conflictPossible && willModifyActionBeAbortedOnEdit(edit);
	}
	
	void validateCreateAction(Map<String, FlowVariable> flowVariables) {
		for (String name : flowVariables.keySet()) {
			if (name.equals(getInputPathFromFileWithName())) {
				AttributeNodeEdit testEdit = new AttributeNodeEdit(getParent(), flowVariables.get(name));
				m_inputInvalidCause = m_inputType == testEdit.getInputType() ? null : InvalidCause.INPUT_DATA_TYPE;
				if (m_inputInvalidCause == null) {
					m_possibleOutputTypes = testEdit.m_possibleOutputTypes;
					m_inputInvalidCause = m_possibleOutputTypes.contains(m_editDataType.getOutputType()) ? null : InvalidCause.OUTPUT_DATA_TYPE;
					// TODO (if necessary) test for more errors like different string lengths
				}
				testEdit.removeFromParent();
				return;
			}
		}
		
		m_inputInvalidCause = InvalidCause.NO_COPY_SOURCE;
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws IOException {
		FlowVariable var = flowVariables.get(getInputPathFromFileWithName());
		setHdfObject(getOpenedHdfObjectOfParent().createAndWriteAttribute(this, var));
		
		return getHdfObject() != null;
	}

	@Override
	protected boolean copyAction(ExecutionContext exec, long totalProgressToDo) throws IOException {
		Hdf5Attribute<?> copyAttribute = (Hdf5Attribute<?>) findCopySource();
		Hdf5TreeElement parent = getOpenedHdfObjectOfParent();
		if (havePropertiesChanged(copyAttribute)) {
			setHdfObject(parent.createAndWriteAttribute(this, copyAttribute));
		} else {
			setHdfObject(copyAttribute.getParent().copyAttribute(copyAttribute, parent, getName()));
		}
		
		return getHdfObject() != null;
	}

	@Override
	protected boolean deleteAction() throws IOException {
		Hdf5Attribute<?> attribute = (Hdf5Attribute<?>) getHdfObject();
		if (getOpenedHdfObjectOfParent().deleteAttribute(attribute.getName()) >= 0) {
			setHdfObject((Hdf5Attribute<?>) null);
		}
		
		return getHdfObject() == null;
	}

	@Override
	protected boolean modifyAction(BufferedDataTable inputTable, boolean saveColumnProperties,
			ExecutionContext exec, long totalProgressToDo) throws IOException {
		Hdf5Attribute<?> oldAttribute = (Hdf5Attribute<?>) getHdfSource();
		Hdf5TreeElement parent = getOpenedHdfObjectOfParent();
		if (havePropertiesChanged(oldAttribute)) {
			setHdfObject(parent.createAndWriteAttribute(this, oldAttribute));
			//throw new NullPointerException("exception test");
			/*if (oldAttribute != getHdfBackup()) {
				parent.deleteAttribute(oldAttribute.getName());
			}*/
		} else {
			if (!oldAttribute.getName().equals(getName())) {
				if (oldAttribute == getHdfBackup()) {
					setHdfObject(oldAttribute.getParent().copyAttribute(oldAttribute, parent, getName()));
				} else {
					setHdfObject(parent.renameAttribute(oldAttribute.getName(), getName()));
				}
			}
		}
		
		return getHdfObject() != null;
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
			private JComboBox<EditOverwritePolicy> m_overwriteField = new JComboBox<>(EditOverwritePolicy.getAvailableValuesForEdit(AttributeNodeEdit.this));
			private DataTypeChooser m_dataTypeChooser = m_editDataType.new DataTypeChooser(false);
			private JCheckBox m_flowVariableArrayField = new JCheckBox();
			
			private AttributePropertiesDialog() {
				super(AttributeNodeMenu.this, "Attribute properties");

				addProperty("Name: ", m_nameField);
				// TODO avoid that the user changes the overwritePolicy without changing the name (for NO_ACTION and MODIFY)
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

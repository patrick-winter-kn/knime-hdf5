package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

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

/**
 * Class for edits on attributes in an hdf file. The respective hdf
 * source is an {@linkplain Hdf5Attribute}.
 */
public class AttributeNodeEdit extends TreeNodeEdit {
	
	private InvalidCause m_inputInvalidCause;
	
	private HdfDataType m_inputType;
	
	private EditDataType m_editDataType = new EditDataType();
	
	private int m_totalStringLength;
	
	private int m_itemStringLength;

	private boolean m_flowVariableArrayPossible;
	
	private boolean m_flowVariableArrayUsed;

	/**
	 * Copies the attribute edit {@code copyAttribute} to {@code parent} with all
	 * properties.
	 * <br>
	 * <br>
	 * If {@code needsCopySource} is {@code true}, the action of this edit
	 * will be set to COPY, except if {@code copyAttribute}'s edit action is CREATE.
	 * In all other cases, the action of this edit is the same as of {@code copyAttribute}.
	 * 
	 * @param parent the parent of this edit
	 * @param copyAttribute the attribute edit to copy from
	 * @param needsCopySource if the {@code copyAttribute} is needed to execute a COPY action
	 * 	with this edit later
	 */
	private AttributeNodeEdit(TreeNodeEdit parent, AttributeNodeEdit copyAttribute, boolean needsCopySource) {
		this(parent, copyAttribute.getInputPathFromFileWithName(), copyAttribute.getName(), copyAttribute.getEditOverwritePolicy(), copyAttribute.getInputType(),
				needsCopySource ? (copyAttribute.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY) : copyAttribute.getEditAction());
		copyAdditionalPropertiesFrom(copyAttribute);
		if (needsCopySource && getEditAction() == EditAction.COPY) {
			setCopyEdit(copyAttribute);
		}
	}

	/**
	 * Initializes a new attribute edit with the input knime flow variable {@code var}.
	 * The edit action is set to CREATE.
	 * 
	 * @param parent the parent of this edit
	 * @param var the knime flow variable for this edit
	 */
	public AttributeNodeEdit(TreeNodeEdit parent, FlowVariable var) {
		this(parent, var.getName(), var.getName().replaceAll("\\\\/", "/"), EditOverwritePolicy.NONE,
				HdfDataType.getHdfDataType(var.getType()), EditAction.CREATE);
		updatePropertiesFromFlowVariable(var);
	}

	/**
	 * Creates a new placeholder for an invalid attribute with the cause
	 * {@code unsupportedCause}.
	 * 
	 * @param parent the parent of this edit
	 * @param name the name of this edit
	 */
	AttributeNodeEdit(TreeNodeEdit parent, String name, String unsupportedCause) {
		this(parent, null, name, EditOverwritePolicy.NONE, null, EditAction.NO_ACTION);
		setUnsupportedCause(unsupportedCause != null ? unsupportedCause : "Unknown cause");
		// remove menu to consider the edit is unsupported
		setTreeNodeMenu(null);
	}

	/**
	 * Initializes a new attribute edit for the input hdf {@code attribute}.
	 * The edit action is set to NO_ACTION.
	 * 
	 * @param parent the parent of this edit
	 * @param attribute the hdf attribute for this edit
	 * @throws IOException if the hdf attribute could not be read
	 */
	public AttributeNodeEdit(TreeNodeEdit parent, Hdf5Attribute<?> attribute) throws IOException {
		this(parent, attribute.getPathFromFile() + attribute.getName().replaceAll("/", "\\\\/"), attribute.getName(), EditOverwritePolicy.NONE,
				attribute.getType().getHdfType().getType(), EditAction.NO_ACTION);

		m_itemStringLength = (int) attribute.getMaxStringLengthOfValues();

		Object[] values = attribute.getValue() == null ? attribute.read() : attribute.getValue();
		List<HdfDataType> possibleOutputTypes = HdfDataType.getConvertibleTypes(m_inputType, values);
		
		m_editDataType.setValues(m_inputType, possibleOutputTypes, attribute.getType().getHdfType().getEndian(),
				Rounding.DOWN, m_inputType.isFloat(), false, m_itemStringLength);
		m_totalStringLength = (int) Math.max(attribute.getDimension(), 1) * m_itemStringLength;
		
		setHdfObject(attribute);
	}

	/**
	 * Initializes a new attribute edit with all core settings.
	 * 
	 * @param parent the parent of this edit
	 * @param inputPathFromFileWithName the path of this edit's hdf attribute
	 * @param name the output name of this edit
	 * @param editOverwritePolicy the overwrite policy for this edit
	 * @param inputType the input data type of this edit
	 * @param editAction the action of this edit
	 * @throws IllegalArgumentException if {@code parent} is neither a
	 * 	{@linkplain GroupNodeEdit} nor {@linkplain DataSetNodeEdit}
	 */
	private AttributeNodeEdit(TreeNodeEdit parent, String inputPathFromFileWithName, String name,
			EditOverwritePolicy editOverwritePolicy, HdfDataType inputType, EditAction editAction) throws IllegalArgumentException{
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

	/**
	 * Initializes a new attribute edit with all core settings.
	 * 
	 * @param parent the parent group of this edit
	 * @param inputPathFromFileWithName the path of this edit's hdf attribute
	 * @param name the output name of this edit
	 * @param editOverwritePolicy the overwrite policy for this edit
	 * @param inputType the input data type of this edit
	 * @param editAction the action of this edit
	 */
	AttributeNodeEdit(GroupNodeEdit parent, String inputPathFromFileWithName, String name,
			EditOverwritePolicy editOverwritePolicy, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, !(parent instanceof FileNodeEdit) ? parent.getOutputPathFromFileWithName() : "", name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		parent.addAttributeNodeEdit(this);
	}


	/**
	 * Initializes a new attribute edit with all core settings.
	 * 
	 * @param parent the parent dataSet of this edit
	 * @param inputPathFromFileWithName the path of this edit's hdf attribute
	 * @param name the output name of this edit
	 * @param editOverwritePolicy the overwrite policy for this edit
	 * @param inputType the input data type of this edit
	 * @param editAction the action of this edit
	 */
	AttributeNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, String name,
			EditOverwritePolicy editOverwritePolicy, HdfDataType inputType, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editOverwritePolicy, editAction);
		setTreeNodeMenu(new AttributeNodeMenu());
		m_inputType = inputType;
		parent.addAttributeNodeEdit(this);
	}

	/**
	 * Copies this edit to {@code parent}.
	 * 
	 * @param parent the destination of the new copy
	 * @return the new copy
	 */
	public AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent) {
		return copyAttributeEditTo(parent, true);
	}

	/**
	 * Copies this edit to {@code parent}.
	 * 
	 * @param parent the destination of the new copy
	 * @param needsCopySource if the information about this edit is needed for
	 * 	the new edit
	 * @return the new copy
	 */
	AttributeNodeEdit copyAttributeEditTo(TreeNodeEdit parent, boolean needsCopySource) {
		AttributeNodeEdit newAttributeEdit = new AttributeNodeEdit(parent, this, needsCopySource);
		newAttributeEdit.addEditToParentNodeIfPossible();
		
		return newAttributeEdit;
	}

	/**
	 * @return if the input flow variables contradict with this edit
	 */
	InvalidCause getInputInvalidCause() {
		return m_inputInvalidCause;
	}

	/**
	 * @return the input type of the source (either the knime flow variable or
	 * 	the hdf attribute)
	 */
	public HdfDataType getInputType() {
		return m_inputType;
	}

	/**
	 * @return information on the output data type of this attribute edit
	 */
	public EditDataType getEditDataType() {
		return m_editDataType;
	}
	
	/**
	 * @return the string length of the flow variable value as string or the
	 * 	product of the dimension of the hdf attribute and its string length
	 * 	(of each item)
	 */
	private int getTotalStringLength() {
		return m_totalStringLength;
	}

	/**
	 * @return the string length of each item in the hdf attribute
	 */
	public int getItemStringLength() {
		return m_itemStringLength;
	}
	
	/**
	 * @return if the flow variable has the structure of a flow variable array
	 * @see Hdf5Attribute#getFlowVariableValues(FlowVariable)
	 */
	private boolean isFlowVariableArrayPossible() {
		return m_flowVariableArrayPossible;
	}

	/**
	 * @return if the structure of a flow variable array is used
	 * @see Hdf5Attribute#getFlowVariableValues(FlowVariable)
	 */
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
			m_inputInvalidCause = copyAttributeEdit.getInputInvalidCause();
			m_editDataType.setValues(copyAttributeEdit.getEditDataType());
			m_totalStringLength = copyAttributeEdit.getTotalStringLength();
			m_itemStringLength = copyAttributeEdit.getItemStringLength();
			m_flowVariableArrayPossible = copyAttributeEdit.isFlowVariableArrayPossible();
			m_flowVariableArrayUsed = copyAttributeEdit.isFlowVariableArrayUsed();
		}
	}
	
	/**
	 * Updates this edit's properties using the value and data type of
	 * {@code var}.
	 * 
	 * @param var the knime flow variable
	 */
	private void updatePropertiesFromFlowVariable(FlowVariable var) {
		Object[] values = Hdf5Attribute.getFlowVariableValues(var);
		m_inputType = Hdf5KnimeDataType.getKnimeDataType(values).getEquivalentHdfType();
		m_totalStringLength = var.getValueAsString().length();
		
		m_itemStringLength = 1;
		for (Object value : values) {
			int newStringLength = value.toString().length();
			m_itemStringLength = newStringLength > m_itemStringLength ? newStringLength : m_itemStringLength;
		}
		m_flowVariableArrayPossible = m_itemStringLength != m_totalStringLength;
		m_flowVariableArrayUsed = m_flowVariableArrayPossible;

		List<HdfDataType> possibleOutputTypes = HdfDataType.getConvertibleTypes(m_inputType, values);
		m_editDataType.setValues(m_inputType, possibleOutputTypes, Endian.LITTLE_ENDIAN,
				Rounding.DOWN, m_inputType.isFloat(), false, m_itemStringLength);
	}
	
	@Override
	public String getToolTipText() {
		return (isSupported() ? "(" + getDataTypeInfo() + ") " : "") + super.getToolTipText();
	}

	/**
	 * @return the information about the input and output data type
	 */
	private String getDataTypeInfo() {
		String info = "";
		
		if (m_inputType != null && m_inputType != m_editDataType.getOutputType()) {
			info += m_inputType.toString() + " -> ";
		}
		
		return info + m_editDataType.getOutputType().toString();
	}
	
	@Override
	protected long getProgressToDoInEdit() throws IOException {
		long totalToDo = 0L;
		
		if (getEditAction() != EditAction.NO_ACTION && getEditState() != EditState.SUCCESS) {
			totalToDo += 71L;
			if (havePropertiesChanged(getEditAction() != EditAction.CREATE ? findCopySource() : null)) {
				HdfDataType dataType = m_editDataType.getOutputType();
				long dataTypeToDo = dataType.isNumber() ? dataType.getSize()/8 : m_editDataType.getStringLength();
				totalToDo += m_totalStringLength / Math.max(1, m_itemStringLength) * dataTypeToDo;
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

		if (m_inputInvalidCause != null) {
			settings.addInt(SettingsKey.INPUT_INVALID_CAUSE.getKey(), m_inputInvalidCause.ordinal());
		}
		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		
		m_editDataType.saveSettingsTo(settings);
		settings.addInt(SettingsKey.TOTAL_STRING_LENGTH.getKey(), m_totalStringLength);
		settings.addInt(SettingsKey.ITEM_STRING_LENGTH.getKey(), m_itemStringLength);
		settings.addBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_POSSIBLE.getKey(), m_flowVariableArrayPossible);
		settings.addBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_USED.getKey(), m_flowVariableArrayUsed);
	}

	@Override
	protected void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey(SettingsKey.INPUT_INVALID_CAUSE.getKey())) {
			m_inputInvalidCause = InvalidCause.values()[settings.getInt(SettingsKey.INPUT_INVALID_CAUSE.getKey())];
		}
		
		m_editDataType.loadSettingsFrom(settings);
		m_totalStringLength = settings.getInt(SettingsKey.TOTAL_STRING_LENGTH.getKey());
		m_itemStringLength = settings.getInt(SettingsKey.ITEM_STRING_LENGTH.getKey());
		m_flowVariableArrayPossible = settings.getBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_POSSIBLE.getKey());
		setFlowVariableArrayUsed(settings.getBoolean(SettingsKey.FLOW_VARIABLE_ARRAY_USED.getKey()));
	}
	
	@Override
	protected InvalidCause validateEditInternal() {
		return getName().isEmpty() ? InvalidCause.NAME_CHARS :
				getName().startsWith(BACKUP_PREFIX) && !getOutputPathFromFileWithName(true).equals(getInputPathFromFileWithName())
				? InvalidCause.NAME_BACKUP_PREFIX : null;
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		boolean conflictPossible = edit instanceof AttributeNodeEdit && this != edit;
		boolean inConflict = conflictPossible && getName().equals(edit.getName())
				&& getEditAction() != EditAction.DELETE && edit.getEditAction() != EditAction.DELETE;
		
		return inConflict ? !avoidsOverwritePolicyNameConflict(edit) : conflictPossible && willBeNameConflictWithIgnoredEdit(edit);
	}

	/**
	 * Validates the CREATE action of this edit based on the knime input
	 * {@code flowVariables} (not based on the data) such that it will be
	 * noticed if some knime input is different from the one when the
	 * settings were defined.
	 * 
	 * @param flowVariables the knime flow variables
	 */
	void validateCreateAction(Map<String, FlowVariable> flowVariables) {
		m_inputInvalidCause = null;
		for (String name : flowVariables.keySet()) {
			if (name.equals(getInputPathFromFileWithName())) {
				AttributeNodeEdit testEdit = new AttributeNodeEdit(getParent(), flowVariables.get(name));
				testEdit.removeFromParent();
				
				m_inputInvalidCause = m_inputType == testEdit.getInputType() ? null : InvalidCause.INPUT_DATA_TYPE;
				if (m_inputInvalidCause == null) {
					List<HdfDataType> possibleOutputTypes = Arrays.asList(testEdit.getEditDataType().getPossibleOutputTypes());
					m_inputInvalidCause = possibleOutputTypes.contains(m_editDataType.getOutputType()) ? null : InvalidCause.OUTPUT_DATA_TYPE;
				}
				if (m_inputInvalidCause == null) {
					m_totalStringLength = testEdit.getTotalStringLength();
					m_itemStringLength = testEdit.getItemStringLength();
				}
				return;
			}
		}
		
		m_inputInvalidCause = InvalidCause.NO_COPY_SOURCE;
	}

	@Override
	protected void createAction(Map<String, FlowVariable> flowVariables, ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			FlowVariable var = flowVariables.get(getInputPathFromFileWithName());
			setHdfObject(getOpenedHdfObjectOfParent().createAndWriteAttribute(this, var));
		} finally {
			setEditSuccess(getHdfObject() != null);
		}
	}

	@Override
	protected void copyAction(ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			Hdf5Attribute<?> copyAttribute = (Hdf5Attribute<?>) findCopySource();
			Hdf5TreeElement parent = getOpenedHdfObjectOfParent();
			if (havePropertiesChanged(copyAttribute)) {
				setHdfObject(parent.copyAttribute(this, copyAttribute));
			} else {
				setHdfObject(parent.copyAttribute(copyAttribute, getName()));
			}
		} finally {
			setEditSuccess(getHdfObject() != null);
		}
	}

	@Override
	protected void deleteAction() throws IOException {
		try {
			Hdf5Attribute<?> attribute = (Hdf5Attribute<?>) getHdfObject();
			if (getOpenedHdfObjectOfParent().deleteAttribute(attribute.getName())) {
				setHdfObject((Hdf5Attribute<?>) null);
			}
		} finally {
			setEditSuccess(getHdfObject() == null);
		}
	}

	@Override
	protected void modifyAction(ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			Hdf5Attribute<?> oldAttribute = (Hdf5Attribute<?>) getHdfSource();
			Hdf5TreeElement parent = getOpenedHdfObjectOfParent();
			if (havePropertiesChanged(oldAttribute)) {
				setHdfObject(parent.copyAttribute(this, oldAttribute));
				//throw new NullPointerException("exception test");
				/*if (oldAttribute != getHdfBackup()) {
					parent.deleteAttribute(oldAttribute.getName());
				}*/
			} else {
				if (!oldAttribute.getName().equals(getName())) {
					if (oldAttribute == getHdfBackup()) {
						setHdfObject(parent.copyAttribute(oldAttribute, getName()));
					} else {
						setHdfObject(parent.renameAttribute(oldAttribute.getName(), getName()));
					}
				}
			}
		} finally {
			setEditSuccess(getHdfObject() != null);
		}
	}
	/**
	 * The class for the {@linkplain JPopupMenu} which can be accessed through
	 * a right mouse click on this attribute edit.
	 */
	public class AttributeNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = -6418394582185524L;
    	
		private AttributeNodeMenu() {
			super(AttributeNodeEdit.this.isSupported(), false, true);
    	}

		/**
		 * Returns the dialog to modify the properties of this attribute edit.
		 * 
		 * @return the properties dialog
		 */
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

		/**
		 * A {@linkplain JDialog} to set all properties of this attribute edit.
		 */
		private class AttributePropertiesDialog extends PropertiesDialog {
	    	
			private static final long serialVersionUID = 9201153080744087510L;
	    	
			private JTextField m_nameField = new JTextField(15);
			private JComboBox<EditOverwritePolicy> m_overwriteField = new JComboBox<>(EditOverwritePolicy.getAvailableValuesForEdit(AttributeNodeEdit.this));
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
				m_dataTypeChooser.loadFromDataType();
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
	
	@Override
	public String toString() {
		return "{ input=" + getInputPathFromFileWithName() + ",output=" + getOutputPathFromFileWithName()
				+ ",action=" + getEditAction() + ",state=" + getEditState()
				+ ",overwrite=" + getEditOverwritePolicy() + ",valid=" + isValid()
				+ ",inputType=" + m_inputType + ",editDataType=" + m_editDataType
				+ ",flowVariableArrayUsed=" + m_flowVariableArrayUsed
				+ ",attribute=" + getHdfObject() + ",backup=" + getHdfBackup() + " }";
	}
}

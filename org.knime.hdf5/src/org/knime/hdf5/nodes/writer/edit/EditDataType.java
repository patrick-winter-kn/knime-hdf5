package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.PropertiesDialog;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.SettingsKey;

/**
 * Holds the information of how to edit a data type of a
 * {@linkplain DataSetNodeEdit} or {@linkplain AttributeNodeEdit}.
 */
public class EditDataType {

	/**
	 * Defines the rounding mode from FLOAT64/double to INT64/long.
	 */
	public static enum Rounding {
		DOWN, UP, FLOOR, CEIL, MATH;
		
		public long round(double value) {
			switch (this) {
			case UP:
				return (long) (value > 0 ? Math.ceil(value) : Math.floor(value));
			case FLOOR:
				return (long) Math.floor(value);
			case CEIL:
				return (long) Math.ceil(value);
			case MATH:
				return Math.round(value);
			default:
				return (long) value;
			}
		}
	}
	
	private HdfDataType m_outputType;
	
	private final List<HdfDataType> m_possibleOutputTypes = new ArrayList<>();

	private Endian m_endian = Endian.LITTLE_ENDIAN;
	
	private Rounding m_rounding = Rounding.DOWN;

	/**
	 * Stores if the rounding is possible based on the input type
	 * (which should be float for that).
	 */
	private boolean m_roundingPossible;
	
	private boolean m_fixedStringLength;
	
	private int m_stringLength;
	
	private Object m_standardValue;
	
	/**
	 * @return the data type that should be used for the output
	 */
	public HdfDataType getOutputType() {
		return m_outputType;
	}

	void setOutputType(HdfDataType outputType) {
		m_outputType = outputType;
	}

	/**
	 * @return the data types that are possible as output data type
	 */
	HdfDataType[] getPossibleOutputTypes() {
		return m_possibleOutputTypes.toArray(new HdfDataType[m_possibleOutputTypes.size()]);
	}

	public Endian getEndian() {
		return m_endian;
	}

	void setEndian(Endian endian) {
		m_endian = endian;
	}

	/**
	 * @return the rounding mode from float to int
	 */
	public Rounding getRounding() {
		return m_rounding;
	}

	/**
	 * @return if the string length is fixed or is set automatically when
	 * 	checking the string length
	 */
	public boolean isFixedStringLength() {
		return m_fixedStringLength;
	}

	public int getStringLength() {
		return m_stringLength;
	}

	public void setStringLength(int stringLength) {
		m_stringLength = stringLength;
	}
	
	/**
	 * @return the standard value for missing values or {@code null} if missing
	 * 	values are not allowed
	 */
	public Object getStandardValue() {
		return m_standardValue;
	}

	void setValues(EditDataType editDataType) {
		setValues(editDataType.getOutputType(), editDataType.m_possibleOutputTypes, editDataType.getEndian(), editDataType.getRounding(),
				editDataType.m_roundingPossible, editDataType.isFixedStringLength(), editDataType.getStringLength(), editDataType.getStandardValue());
	}
	
	void setValues(HdfDataType outputType, List<HdfDataType> possibleOutputTypes, Endian endian, Rounding rounding,
			boolean roundingPossible, boolean fixedStringLength, int stringLength) {
		setValues(outputType, possibleOutputTypes, endian, rounding, roundingPossible, fixedStringLength, stringLength, m_standardValue);
	}
	
	private void setValues(HdfDataType outputType, List<HdfDataType> possibleOutputTypes, Endian endian, Rounding rounding,
			boolean roundingPossible, boolean fixedStringLength, int stringLength, Object standardValue) {
		setValues(outputType, endian, rounding, fixedStringLength, stringLength);
		m_possibleOutputTypes.clear();
		m_possibleOutputTypes.addAll(possibleOutputTypes);
		Collections.sort(m_possibleOutputTypes);
		m_roundingPossible = roundingPossible;
		m_standardValue = standardValue;
	}

	private void setValues(HdfDataType outputType, Endian endian, Rounding rounding, boolean fixedStringLength,
			int stringLength) {
		setOutputType(outputType);
		setEndian(endian);
		m_rounding = rounding;
		m_fixedStringLength = fixedStringLength;
		setStringLength(stringLength);
	}
	
	void saveSettingsTo(NodeSettingsWO settings) {
		settings.addInt(SettingsKey.OUTPUT_TYPE.getKey(), m_outputType.getTypeId());
		
		int[] typeIds = new int[m_possibleOutputTypes.size()];
		for (int i = 0; i < typeIds.length; i++) {
			typeIds[i] = m_possibleOutputTypes.get(i).getTypeId();
		}
		settings.addIntArray(SettingsKey.POSSIBLE_OUTPUT_TYPES.getKey(), typeIds);
		
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addInt(SettingsKey.ROUNDING.getKey(), m_rounding.ordinal());
		settings.addBoolean(SettingsKey.ROUNDING_POSSIBLE.getKey(), m_roundingPossible);
		settings.addBoolean(SettingsKey.FIXED_STRING_LENGTH.getKey(), m_fixedStringLength);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
		settings.addString(SettingsKey.STANDARD_VALUE.getKey(), m_standardValue != null ? m_standardValue.toString() : null);
	}

	void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		setOutputType(HdfDataType.get(settings.getInt(SettingsKey.OUTPUT_TYPE.getKey())));

		m_possibleOutputTypes.clear();
		int[] typeIds = settings.getIntArray(SettingsKey.POSSIBLE_OUTPUT_TYPES.getKey(), new int[0]);
		for (int typeId : typeIds) {
			m_possibleOutputTypes.add(HdfDataType.get(typeId));
		}
		
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		m_rounding = Rounding.values()[settings.getInt(SettingsKey.ROUNDING.getKey())];
		m_roundingPossible = settings.getBoolean(SettingsKey.ROUNDING_POSSIBLE.getKey(), true);
		m_fixedStringLength = settings.getBoolean(SettingsKey.FIXED_STRING_LENGTH.getKey(), false);
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		
		String standardValueString = settings.getString(SettingsKey.STANDARD_VALUE.getKey());
		Object standardValue = null;
		if (standardValueString != null) {
			if (!m_outputType.isNumber()) {
				standardValue = standardValueString;
			} else if (m_outputType.isFloat()) {
				standardValue = Double.parseDouble(standardValueString);
			} else if (!m_outputType.fitMinMaxValuesIntoType(HdfDataType.INT32)) {
				standardValue = Long.parseLong(standardValueString);
			} else {
				standardValue = Integer.parseInt(standardValueString);
			}
		}
		m_standardValue = standardValue;
	}
	
	/**
	 * Holds the GUI for choosing the data type.
	 */
	protected class DataTypeChooser {
		
		private JComboBox<HdfDataType> m_typeField = new JComboBox<>();
		private JCheckBox m_unsignedField = new JCheckBox();
		private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
		private JComboBox<Rounding> m_roundingField = new JComboBox<>(Rounding.values());
		private JPanel m_stringLengthField = new JPanel();
		private JCheckBox m_standardValueCheckBox;
		private JPanel m_standardValueField = new JPanel(new BorderLayout());
		
		private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
		private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
		private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(1, 1, Integer.MAX_VALUE, 1));
		private JSpinner m_standardValueIntSpinner = new JSpinner(new SpinnerNumberModel((Long) 0L, (Long) Long.MIN_VALUE, (Long) Long.MAX_VALUE, (Long) 1L));
		private JSpinner m_standardValueFloatSpinner = new JSpinner(new SpinnerNumberModel(0.0, null, null, 0.1));
		private JTextField m_standardValueStringTextField = new JTextField(15);
		
		private final boolean m_standardValueEnabled;
		
		/**
		 * @param standardValueEnabled if standard values for missing values are allowed
		 */
		protected DataTypeChooser(boolean standardValueEnabled) {
			// define the field for the data type
			m_typeField.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					setSelectableFieldsEnabled();
				}
			});
			
			// define the field for the String length
			m_stringLengthField = new JPanel();
			ButtonGroup stringLengthGroup = new ButtonGroup();
			m_stringLengthField.setLayout(new GridBagLayout());
			GridBagConstraints constraints = new GridBagConstraints();
			constraints.fill = GridBagConstraints.HORIZONTAL;
			constraints.gridx = 0;
			constraints.gridy = 0;
			constraints.weightx = 0.0;
			m_stringLengthField.add(m_stringLengthAuto, constraints);
			stringLengthGroup.add(m_stringLengthAuto);
			m_stringLengthAuto.setSelected(true);
			constraints.gridx++;
			m_stringLengthField.add(m_stringLengthFixed, constraints);
			stringLengthGroup.add(m_stringLengthFixed);
			constraints.gridx++;
			constraints.weightx = 1.0;
			m_stringLengthField.add(m_stringLengthSpinner, constraints);
			m_stringLengthSpinner.setEnabled(false);
			m_stringLengthFixed.addChangeListener(new ChangeListener() {
				
				@Override
				public void stateChanged(ChangeEvent e) {
					m_stringLengthSpinner.setEnabled(m_stringLengthFixed.isSelected());
				}
			});
			
			// define the field for standard values
			m_standardValueEnabled = standardValueEnabled;
			if (m_standardValueEnabled) {
				m_standardValueField.add(m_standardValueIntSpinner, BorderLayout.NORTH);
				m_standardValueField.add(m_standardValueFloatSpinner, BorderLayout.CENTER);
				m_standardValueFloatSpinner.setEditor(new JSpinner.NumberEditor(m_standardValueFloatSpinner, "0.0##"));
				m_standardValueField.add(m_standardValueStringTextField, BorderLayout.SOUTH);
			}
		}
		
		/**
		 * Enables all selectable fields in the dialog.
		 */
		private void setSelectableFieldsEnabled() {
			HdfDataType selectedType = (HdfDataType) m_typeField.getSelectedItem();

			boolean signedSelectable = m_possibleOutputTypes.contains(selectedType.getSignedType());
			boolean unsignedSelectable = m_possibleOutputTypes.contains(selectedType.getUnsignedType());
			m_unsignedField.setEnabled(signedSelectable && unsignedSelectable);
			m_unsignedField.setSelected(!signedSelectable && unsignedSelectable);
			
			m_roundingField.setEnabled(m_roundingPossible && selectedType.isNumber() && !selectedType.isFloat());

			boolean isString = selectedType == HdfDataType.STRING;
			m_endianField.setEnabled(!isString);
			m_stringLengthAuto.setEnabled(isString);
			m_stringLengthFixed.setEnabled(isString);
			m_stringLengthSpinner.setEnabled(isString && m_stringLengthFixed.isSelected());
			
			if (m_standardValueEnabled) {
				m_standardValueCheckBox.setSelected(m_standardValue != null);
				
				boolean isFloat = selectedType.isFloat();
				boolean isInteger = selectedType.isNumber() && !isFloat;
				m_standardValueIntSpinner.setVisible(isInteger);
				m_standardValueFloatSpinner.setVisible(isFloat);
				m_standardValueStringTextField.setVisible(isString);
				enableStandardValueField(m_standardValueCheckBox.isSelected());
			}
		}
		
		/**
		 * Enables/disables the spinners and textField of the standard value.
		 * 
		 * @param enable if the field for standard values should be enabled
		 */
		private void enableStandardValueField(boolean enable) {
			m_standardValueIntSpinner.setEnabled(enable);
			m_standardValueFloatSpinner.setEnabled(enable);
			m_standardValueStringTextField.setEnabled(enable);
		}
		
		/**
		 * Makes only String data types selectable
		 * 
		 * @param onlyString if only String data types can be selectable
		 * @param stringLength the string length of the source for this
		 * 	dataSet/attribute
		 */
		void setOnlyStringSelectable(boolean onlyString, int stringLength) {
			m_typeField.setEnabled(!onlyString);
			if (onlyString) {
				m_typeField.setSelectedItem(HdfDataType.STRING);
			}
			if (!m_stringLengthFixed.isSelected()) {
				m_stringLengthSpinner.setValue(stringLength);
			}
		}
		
		/**
		 * Adds all the fields to the dialog
		 * 
		 * @param dialog the dialog where the properties should be added to
		 */
		void addToPropertiesDialog(PropertiesDialog dialog) {
			dialog.addProperty("Type: ", m_typeField);
			dialog.addProperty("Unsigned: ", m_unsignedField);
			dialog.addProperty("Endian: ", m_endianField);
			dialog.addProperty("Rounding: ", m_roundingField);
			dialog.addProperty("String length: ", m_stringLengthField);
			if (m_standardValueEnabled) {
				m_standardValueCheckBox = dialog.addProperty("Standard value for missing values: ", m_standardValueField, new ChangeListener() {

					@Override
					public void stateChanged(ChangeEvent e) {
						enableStandardValueField(m_standardValueCheckBox.isSelected());
					};
				});
			}
		}
		
		/**
		 * Updates the fields with the loaded values of this editDataType.
		 */
		void loadFromDataType() {
			List<HdfDataType> signedTypes = new ArrayList<>();
			for (HdfDataType type : m_possibleOutputTypes) {
				HdfDataType signedType = type.getSignedType();
				if (!signedTypes.contains(signedType)) {
					signedTypes.add(signedType);
				}
			}
			
			m_typeField.setModel(new DefaultComboBoxModel<HdfDataType>(signedTypes.toArray(new HdfDataType[signedTypes.size()])));
			m_typeField.setSelectedItem(getOutputType().getSignedType());
			m_unsignedField.setSelected(getOutputType().isUnsigned());
			m_endianField.setSelectedItem(getEndian());
			m_roundingField.setSelectedItem(getRounding());
			m_stringLengthAuto.setSelected(!isFixedStringLength());
			m_stringLengthFixed.setSelected(isFixedStringLength());
			m_stringLengthSpinner.setValue(getStringLength());
			
			if (m_standardValueEnabled && m_standardValue != null) {
				boolean isString = m_outputType == HdfDataType.STRING;
				boolean isFloat = m_outputType.isFloat();
				boolean isInteger = m_outputType.isNumber() && !isFloat;
				
				m_standardValueStringTextField.setText(isString ? (String) m_standardValue : "");
				m_standardValueFloatSpinner.setValue(isFloat ? (Double) m_standardValue : 0.0);
				m_standardValueIntSpinner.setValue(isInteger ? (!m_outputType.fitMinMaxValuesIntoType(HdfDataType.INT32)
						? (Long) m_standardValue : (long) (int) (Integer) m_standardValue) : 0L);
			}
			
			setSelectableFieldsEnabled();
		}
		
		/**
		 * Saves the values of the fields back in the editDataType.
		 */
		void saveToDataType() {
			setValues(HdfDataType.get(((HdfDataType) m_typeField.getSelectedItem()).getTypeId() + (m_unsignedField.isSelected() ? 1 : 0)),
					(Endian) m_endianField.getSelectedItem(), (Rounding) m_roundingField.getSelectedItem(), m_stringLengthFixed.isSelected(),
					(Integer) m_stringLengthSpinner.getValue());
			
			Object standardValue = null;
			if (m_standardValueEnabled && m_standardValueCheckBox.isSelected()) {
				if (!m_outputType.isNumber()) {
					standardValue = m_standardValueStringTextField.getText();
				} else if (m_outputType.isFloat()) {
					standardValue = (Double) m_standardValueFloatSpinner.getValue();
				} else if (!m_outputType.fitMinMaxValuesIntoType(HdfDataType.INT32)) {
					standardValue = (Long) m_standardValueIntSpinner.getValue();
				} else {
					standardValue = (Integer) (int) (long) (Long) m_standardValueIntSpinner.getValue();
				}
			}
			m_standardValue = standardValue;
		}
	}
	
	@Override
	public String toString() {
		return "{ outputType=" + m_outputType + ",endian=" + m_endian + ",standardValue=" + m_standardValue
				+ (m_outputType.isNumber() && !m_outputType.isFloat() ? ",rounding=" + m_rounding : "")
				+ (!m_outputType.isNumber() ? (m_fixedStringLength ? ",stringLength=" + m_stringLength : ",autoStringLength=true") : "")
				+ ",possibleOutputTypes=" + m_possibleOutputTypes + " }";
	}
}

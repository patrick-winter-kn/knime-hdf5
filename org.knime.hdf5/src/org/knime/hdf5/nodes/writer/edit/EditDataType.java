package org.knime.hdf5.nodes.writer.edit;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
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

public class EditDataType {

	private static enum Sign {
		SIGNED, UNSIGNED;
		
		private int value() {
			return ordinal() + 1;
		}
	}
	
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
	
	private Map<HdfDataType, Integer> m_possibleHdfTypes = new LinkedHashMap<>();

	private Endian m_endian;
	
	private Rounding m_rounding;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	public HdfDataType getOutputType() {
		return m_outputType;
	}

	void setOutputType(HdfDataType outputType) {
		m_outputType = outputType;
	}

	public Endian getEndian() {
		return m_endian;
	}

	void setEndian(Endian endian) {
		m_endian = endian;
	}

	public Rounding getRounding() {
		return m_rounding;
	}

	void setRounding(Rounding rounding) {
		m_rounding = rounding;
	}

	public boolean isFixed() {
		return m_fixed;
	}

	void setFixed(boolean fixed) {
		m_fixed = fixed;
	}

	public int getStringLength() {
		return m_stringLength;
	}

	void setStringLength(int stringLength) {
		m_stringLength = stringLength;
	}

	void setValues(EditDataType editDataType) {
		setValues(editDataType.getOutputType(), editDataType.getEndian(), editDataType.getRounding(), editDataType.isFixed(), editDataType.getStringLength());
	}
	
	void setValues(HdfDataType outputType, Endian endian, Rounding rounding, boolean fixed, int stringLength) {
		setOutputType(outputType);
		setEndian(endian);
		setRounding(rounding);
		setFixed(fixed);
		setStringLength(stringLength);
	}
	
	void saveSettingsTo(NodeSettingsWO settings) {
		settings.addInt(SettingsKey.OUTPUT_TYPE.getKey(), m_outputType.getTypeId());
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addInt(SettingsKey.ROUNDING.getKey(), m_rounding.ordinal());
		settings.addBoolean(SettingsKey.FIXED.getKey(), m_fixed);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
	}

	void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		setOutputType(HdfDataType.get(settings.getInt(SettingsKey.OUTPUT_TYPE.getKey())));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setRounding(Rounding.values()[settings.getInt(SettingsKey.ROUNDING.getKey())]);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
	}
	
	protected class DataTypeChooser {
		
		private JComboBox<HdfDataType> m_typeField = new JComboBox<>();
		private JCheckBox m_unsignedField = new JCheckBox();
		private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
		private JComboBox<Rounding> m_roundingField = new JComboBox<>(Rounding.values());
		private JPanel m_stringLengthField = new JPanel();
		
		private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
		private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
		private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
		
		private boolean m_roundingEnabled;
		
		protected DataTypeChooser() {
			m_typeField.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					HdfDataType selectedType = (HdfDataType) m_typeField.getSelectedItem();
					
					int possibleSigns = m_possibleHdfTypes.get(selectedType.getSignedType());
					m_unsignedField.setEnabled(possibleSigns == Sign.UNSIGNED.value() + Sign.SIGNED.value());
					if (possibleSigns != Sign.UNSIGNED.value() + Sign.SIGNED.value()) {
						m_unsignedField.setSelected(possibleSigns == Sign.UNSIGNED.value());
					}
					m_roundingField.setEnabled(m_roundingEnabled && selectedType.isNumber() && !selectedType.isFloat());
					
					boolean isString = selectedType == HdfDataType.STRING;
					m_endianField.setEnabled(!isString);
					m_stringLengthAuto.setEnabled(isString);
					m_stringLengthFixed.setEnabled(isString);
					m_stringLengthSpinner.setEnabled(isString && m_stringLengthFixed.isSelected());
				}
			});
			
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
		}
		
		void setOnlyStringSelectable(boolean onlyString, int stringLength) {
			m_typeField.setEnabled(!onlyString);
			if (onlyString) {
				m_typeField.setSelectedItem(HdfDataType.STRING);
			}
			if (!m_stringLengthFixed.isSelected()) {
				m_stringLengthSpinner.setValue(stringLength);
			}
		}
		
		void addToPropertiesDialog(PropertiesDialog dialog) {
			dialog.addProperty("Type: ", m_typeField);
			dialog.addProperty("Unsigned: ", m_unsignedField);
			dialog.addProperty("Endian: ", m_endianField);
			dialog.addProperty("Rounding: ", m_roundingField);
			dialog.addProperty("String length: ", m_stringLengthField);
		}
		
		void loadFromDataType(List<HdfDataType> possibleHdfTypes, boolean roundingEnabled) {
			m_possibleHdfTypes.clear();
			Collections.sort(possibleHdfTypes);
			for (HdfDataType type : possibleHdfTypes) {
				HdfDataType signedType = type.getSignedType();
				Integer possibleSigns = m_possibleHdfTypes.get(signedType);
				m_possibleHdfTypes.put(signedType,
						(possibleSigns == null ? 0 : possibleSigns) + (type.isUnsigned() ? Sign.UNSIGNED.value() : Sign.SIGNED.value()));
			}
			
			m_typeField.setModel(new DefaultComboBoxModel<HdfDataType>(m_possibleHdfTypes.keySet().toArray(new HdfDataType[m_possibleHdfTypes.size()])));
			m_typeField.setSelectedItem(getOutputType().getSignedType());
			m_unsignedField.setSelected(getOutputType().isUnsigned());
			m_endianField.setSelectedItem(getEndian());
			m_roundingField.setSelectedItem(m_rounding);
			m_stringLengthAuto.setSelected(!isFixed());
			m_stringLengthFixed.setSelected(isFixed());
			m_stringLengthSpinner.setValue(getStringLength());
			
			m_roundingEnabled = roundingEnabled;
		}
		
		void saveToDataType() {
			setValues(HdfDataType.get(((HdfDataType) m_typeField.getSelectedItem()).getTypeId() + (m_unsignedField.isSelected() ? 1 : 0)),
					(Endian) m_endianField.getSelectedItem(), (Rounding) m_roundingField.getSelectedItem(), m_stringLengthFixed.isSelected(),
					(Integer) m_stringLengthSpinner.getValue());
		}
	}
}

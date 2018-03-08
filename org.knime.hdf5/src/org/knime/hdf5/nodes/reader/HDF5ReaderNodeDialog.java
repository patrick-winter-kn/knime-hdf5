package org.knime.hdf5.nodes.reader;

import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.JFileChooser;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5File;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

	private final DataColumnSpecFilterPanel m_dataSetFilterPanel;

	private final DataColumnSpecFilterPanel m_attributeFilterPanel;

	private SettingsModelString m_filePathSettings;

	private SettingsModelBoolean m_failIfRowSizeDiffersSettings;

	/**
	 * Creates a new {@link NodeDialogPane} for the column filter in order to
	 * set the desired columns.
	 */
	public HDF5ReaderNodeDialog() {
		createFileChooser();

		m_dataSetFilterPanel = new DataColumnSpecFilterPanel();
		addTab("Data Sets", m_dataSetFilterPanel);

		m_attributeFilterPanel = new DataColumnSpecFilterPanel();
		addTab("Attributes", m_attributeFilterPanel);

		m_failIfRowSizeDiffersSettings = SettingsFactory.createFailIfRowSizeDiffersSettings();
		DialogComponentBoolean failIfRowSizeDiffers = new DialogComponentBoolean(m_failIfRowSizeDiffersSettings,
				"Fail if row size differs");
		failIfRowSizeDiffers.getComponentPanel()
				.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Advanced settings:"));
		addDialogComponent(failIfRowSizeDiffers);
	}

	private void createFileChooser() {
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "filePathHistory",
				JFileChooser.OPEN_DIALOG, false, filePathFvm, ".h5|.hdf5");
		fileChooser.setBorderTitle("Input file:");
		addDialogComponent(fileChooser);
		fileChooser.getModel().addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				updateConfigs(m_filePathSettings.getStringValue(), null);
			}
		});
	}

	private void updateConfigs(String filePath, final NodeSettingsRO settings) {
		updateDataSetConfig(filePath, settings);
		updateAttributeConfig(filePath, settings);
	}

	private void updateDataSetConfig(String filePath, NodeSettingsRO settings) {
		DataColumnSpecFilterConfiguration config = SettingsFactory.createDataSetFilterConfiguration();
		if (settings == null) {
			NodeSettings tempSettings = new NodeSettings("temp");
			m_dataSetFilterPanel.saveConfiguration(config);
			config.saveConfiguration(tempSettings);
			settings = tempSettings;
		}
		
		DataTableSpec spec = null;
		try {
			Hdf5File file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
			try {
				spec = file.createSpecOfDataSets();
			} finally {
				file.close();
			}
			
		} catch (IOException ioe) {
			spec = new DataTableSpec();
		}
		
		config.loadConfigurationInDialog(settings, spec);
		m_dataSetFilterPanel.loadConfiguration(config, spec);
	}

	private void updateAttributeConfig(String filePath, NodeSettingsRO settings) {
		DataColumnSpecFilterConfiguration config = SettingsFactory.createAttributeFilterConfiguration();
		if (settings == null) {
			NodeSettings tempSettings = new NodeSettings("temp");
			m_attributeFilterPanel.saveConfiguration(config);
			config.saveConfiguration(tempSettings);
			settings = tempSettings;
		}
		
		DataTableSpec spec = null;
		try {
			Hdf5File file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
			try {
				spec = file.createSpecOfAttributes();
			} finally {
				file.close();
			}
			
		} catch (IOException ioe) {
			spec = new DataTableSpec();
		}
		
		config.loadConfigurationInDialog(settings, spec);
		m_attributeFilterPanel.loadConfiguration(config, spec);
	}

	/**
	 * Calls the update method of the underlying filter panel.
	 * 
	 * @param settings
	 *            the node settings to read from
	 * @param specs
	 *            the input specs
	 * @throws NotConfigurableException
	 *             if no columns are available for filtering
	 */
	@Override
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
			throws NotConfigurableException {
		updateConfigs(m_filePathSettings.getStringValue(), settings);
	}

	@Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
		DataColumnSpecFilterConfiguration dataSetConfig = SettingsFactory.createDataSetFilterConfiguration();
		m_dataSetFilterPanel.saveConfiguration(dataSetConfig);
		dataSetConfig.saveConfiguration(settings);

		DataColumnSpecFilterConfiguration attributeConfig = SettingsFactory.createAttributeFilterConfiguration();
		m_attributeFilterPanel.saveConfiguration(attributeConfig);
		attributeConfig.saveConfiguration(settings);
	}
}

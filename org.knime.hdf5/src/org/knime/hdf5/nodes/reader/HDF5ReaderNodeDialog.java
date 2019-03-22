package org.knime.hdf5.nodes.reader;

import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.JFileChooser;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5File;

/**
 * The {@link NodeDialogPane} for the hdf reader in order to
 * import hdf files.
 */
class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

	private final DataColumnSpecFilterPanel m_dataSetFilterPanel;

	private final DataColumnSpecFilterPanel m_attributeFilterPanel;

	private SettingsModelString m_filePathSettings;

	private SettingsModelBoolean m_failIfRowSizeDiffersSettings;

	public HDF5ReaderNodeDialog() {
		createFileChooser();

		m_failIfRowSizeDiffersSettings = SettingsFactory.createFailIfRowSizeDiffersSettings();
		DialogComponentBoolean failIfRowSizeDiffers = new DialogComponentBoolean(m_failIfRowSizeDiffersSettings,
				"Fail if row size differs");
		failIfRowSizeDiffers.getComponentPanel()
				.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Advanced settings:"));
		addDialogComponent(failIfRowSizeDiffers);
		
		m_dataSetFilterPanel = new DataColumnSpecFilterPanel();
		addTab("Data Sets", m_dataSetFilterPanel);

		m_attributeFilterPanel = new DataColumnSpecFilterPanel();
		addTab("Attributes", m_attributeFilterPanel);
	}

	private void createFileChooser() {
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "inputFilePathHistory",
				JFileChooser.OPEN_DIALOG, false, filePathFvm, ".h5|.hdf5");
		
		DialogComponentLabel fileInfoLabel = new DialogComponentLabel("");
		fileChooser.getModel().addChangeListener(new ChangeListener() {
			
			/**
			 * {@code true} if there are specs loaded at the moment
			 */
			private boolean m_specsExist = false;
			
			@Override
			public void stateChanged(ChangeEvent e) {
		    	String errorInfo = null;
		    	
				String urlPath = m_filePathSettings.getStringValue();
				try {
					String filePath = HDF5ReaderNodeModel.getFilePathFromUrlPath(urlPath, true);
					if (Hdf5File.hasHdf5FileEnding(filePath)) {
						fileInfoLabel.setText("Info: File " + (Hdf5File.existsHdf5File(filePath) ? "exists" : "does not exist"));
					} else {
						errorInfo = "Error: File ending is not valid";
					}
				} catch (InvalidSettingsException ise) {
					errorInfo = "Error: " + ise.getMessage();
				}
				
				if (errorInfo != null) {
					fileInfoLabel.setText("<html><font color=\"red\">" + errorInfo + "</font></html>");
				}
				
				if (errorInfo == null || m_specsExist) {
					m_specsExist = updateConfigs(null);
				}
			}
		});
		
        createNewGroup("Input file:");
		addDialogComponent(fileChooser);
		addDialogComponent(fileInfoLabel);
        closeCurrentGroup();
	}

	private boolean updateConfigs(final NodeSettingsRO settings) {
		return updateDataSetConfig(settings) | updateAttributeConfig(settings);
	}

	/**
	 * Updates the configuration for importing the hdf dataSets.
	 * 
	 * @param settings the node settings to read from
	 * @return if the specs are not empty
	 */
	private boolean updateDataSetConfig(NodeSettingsRO settings) {
		DataColumnSpecFilterConfiguration config = SettingsFactory.createDataSetFilterConfiguration();
		if (settings == null) {
			NodeSettings tempSettings = new NodeSettings("temp");
			m_dataSetFilterPanel.saveConfiguration(config);
			config.saveConfiguration(tempSettings);
			settings = tempSettings;
		}
		
		DataTableSpec spec = null;
		try {
			Hdf5File file = Hdf5File.openFile(HDF5ReaderNodeModel.getFilePathFromUrlPath(m_filePathSettings.getStringValue(), true), Hdf5File.READ_ONLY_ACCESS);
			try {
				spec = file.createSpecOfDataSets();
			} finally {
				file.close();
			}
		} catch (IOException | InvalidSettingsException ioise) {
			spec = new DataTableSpec();
		}
		
		config.loadConfigurationInDialog(settings, spec);
		m_dataSetFilterPanel.loadConfiguration(config, spec);
		
		return spec.getNumColumns() != 0;
	}

	/**
	 * Updates the configuration for importing the hdf attributes.
	 * 
	 * @param settings the node settings to read from
	 * @return if the specs are not empty
	 */
	private boolean updateAttributeConfig(NodeSettingsRO settings) {
		DataColumnSpecFilterConfiguration config = SettingsFactory.createAttributeFilterConfiguration();
		if (settings == null) {
			NodeSettings tempSettings = new NodeSettings("temp");
			m_attributeFilterPanel.saveConfiguration(config);
			config.saveConfiguration(tempSettings);
			settings = tempSettings;
		}
		
		DataTableSpec spec = null;
		try {
			Hdf5File file = Hdf5File.openFile(HDF5ReaderNodeModel.getFilePathFromUrlPath(m_filePathSettings.getStringValue(), true), Hdf5File.READ_ONLY_ACCESS);
			try {
				spec = file.createSpecOfAttributes();
			} finally {
				file.close();
			}
		} catch (IOException | InvalidSettingsException ioise) {
			spec = new DataTableSpec();
		}
		
		config.loadConfigurationInDialog(settings, spec);
		m_attributeFilterPanel.loadConfiguration(config, spec);

		return spec.getNumColumns() != 0;
	}

	/**
	 * Updates the dataSet and attribute configs.
	 * 
	 * @param settings the node settings to read from
	 * @param specs the input specs
	 */
	@Override
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs) {
		updateConfigs(settings);
	}

	/**
	 * Save the settings of the dataSet and attribute configs.
	 */
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

package org.knime.hdf5.nodes.reader;

import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

/**
 * Factory that defines the settings for the hdf reader.
 */
final class SettingsFactory {

	private static final String INPUT_CONFIG_KEY = "_Input";
	
	private static final String DATA_SET_FILTER_CONFIG_KEY = "dataSetFilter";

	private static final String ATTRIBUTE_FILTER_CONFIG_KEY = "attributeFilter";

	private SettingsFactory() {
		// Disables default constructor
	}

	/**
	 * Factory method for the source setting of the file chooser.
	 * 
	 * @return source {@code SettingsModel}
	 */
	static SettingsModelString createFilePathSettings() {
		return new SettingsModelString("filePath" + INPUT_CONFIG_KEY, "");
	}

	/**
	 * Factory method for the source setting of the CheckBox "fail if rowSize
	 * differs".
	 * 
	 * 
	 * @return source {@code SettingsModel}
	 */
	static SettingsModelBoolean createFailIfRowSizeDiffersSettings() {
		return new SettingsModelBoolean("failIfRowSizeDiffers" + INPUT_CONFIG_KEY, true);
	}

    /**
     * @return a new configuration to store the settings of the dataSet filter (also enables the type filter)
     */
    static final DataColumnSpecFilterConfiguration createDataSetFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration(DATA_SET_FILTER_CONFIG_KEY);
    }

    /** 
     * @return a new configuration to store the settings of the attribute filter (also enables the type filter)
     */
    static final DataColumnSpecFilterConfiguration createAttributeFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration(ATTRIBUTE_FILTER_CONFIG_KEY);
    }
}

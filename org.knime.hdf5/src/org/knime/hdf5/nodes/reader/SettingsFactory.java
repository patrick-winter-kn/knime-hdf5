package org.knime.hdf5.nodes.reader;

import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

final class SettingsFactory {

	static final String DATA_SET_FILTER_CONFIG_KEY = "dataSetFilter";

	static final String ATTRIBUTE_FILTER_CONFIG_KEY = "attributeFilter";

	private SettingsFactory() {
		// Disables default constructor
	}

	/**
	 * Factory method for the source setting of the file chooser.
	 * 
	 * 
	 * @return Source <code>SettingsModel</code>
	 */
	static SettingsModelString createFilePathSettings() {
		return new SettingsModelString("filePath", "");
	}

	/**
	 * Factory method for the source setting of the check box "fail if rowSize
	 * differs".
	 * 
	 * 
	 * @return Source <code>SettingsModel</code>
	 */
	static SettingsModelBoolean createFailIfRowSizeDiffersSettings() {
		return new SettingsModelBoolean("failIfRowSizeDiffers", true);
	}

    /** A new configuration to store the settings of the dataSet filter. Also enables the type filter.
     * @return ...
     */
    static final DataColumnSpecFilterConfiguration createDataSetFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration(DATA_SET_FILTER_CONFIG_KEY);
    }

    /** A new configuration to store the settings of the attribute filter. Also enables the type filter.
     * @return ...
     */
    static final DataColumnSpecFilterConfiguration createAttributeFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration(ATTRIBUTE_FILTER_CONFIG_KEY);
    }

}

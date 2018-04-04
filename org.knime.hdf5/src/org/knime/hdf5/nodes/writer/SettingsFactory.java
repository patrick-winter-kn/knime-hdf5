package org.knime.hdf5.nodes.writer;

import org.knime.core.node.defaultnodesettings.SettingsModelString;

final class SettingsFactory {

	private static final String OUTPUT_CONFIG_KEY = "_Output";
	
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
		return new SettingsModelString("filePath" + OUTPUT_CONFIG_KEY, "");
	}
	
	/**
	 * Factory method for the source setting of the file chooser.
	 * 
	 * 
	 * @return Source <code>SettingsModel</code>
	 */
	static SettingsModelString createGroupPathSettings() {
		return new SettingsModelString("groupPath" + OUTPUT_CONFIG_KEY, "");
	}
	
	/**
	 * Factory method for the source setting of the file chooser.
	 * 
	 * 
	 * @return Source <code>SettingsModel</code>
	 */
	static SettingsModelString createGroupNameSettings() {
		return new SettingsModelString("groupName" + OUTPUT_CONFIG_KEY, "");
	}
}

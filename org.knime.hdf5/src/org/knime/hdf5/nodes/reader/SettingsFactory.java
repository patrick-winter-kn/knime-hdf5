package org.knime.hdf5.nodes.reader;

import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

final class SettingsFactory {

	static final String DS_CONF_KEY = "dsFilterPanel";
	
	static final String ATTR_CONF_KEY = "attrFilterPanel";
	
    private SettingsFactory() {
        // Disables default constructor
    }

    /**
     * Factory method for the source setting of the file chooser.
     * 
     * 
     * @return Source <code>SettingsModel</code>
     */
    static SettingsModelString createFcSourceSettings() {
        return new SettingsModelString("fcSource", "");
    }
    
    /**
     * Factory method for the source setting of the check box "fail if rowSize differs".
     * 
     * 
     * @return Source <code>SettingsModel</code>
     */
    static SettingsModelBoolean createFirsdSourceSettings() {
        return new SettingsModelBoolean("firsdSource", true);
    }
    
}

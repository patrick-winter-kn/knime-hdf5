package org.knime.hdf5.nodes.reader;

import org.knime.core.node.defaultnodesettings.SettingsModelString;

final class SettingsFactory {

    private SettingsFactory() {
        // Disables default constructor
    }

    /**
     * Factory method for the source setting.
     * 
     * 
     * @return Source <code>SettingsModel</code>
     */
    static SettingsModelString createSourceSettings() {
        return new SettingsModelString("source", "");
    }

    /**
     * Factory method for the target directory setting.
     * 
     * 
     * @return Target directory <code>SettingsModel</code>
     */
    static SettingsModelString createTargetDirectorySettings() {
        return new SettingsModelString("targetdirectory", "");
    }

    /**
     * Factory method for the output setting.
     * 
     * 
     * @return Output <code>SettingsModel</code>
     */
    static SettingsModelString createOutputSettings() {
        return new SettingsModelString("output", OutputSelection.URI.getName());
    }

    /**
     * Factory method for the if exists setting.
     * 
     * 
     * @return If exists <code>SettingsModel</code>
     */
    static SettingsModelString createIfExistsSettings() {
        return new SettingsModelString("ifexists", OverwritePolicy.ABORT.getName());
    }

}

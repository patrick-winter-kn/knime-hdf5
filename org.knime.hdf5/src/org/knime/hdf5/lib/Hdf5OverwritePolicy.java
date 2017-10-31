package org.knime.hdf5.lib;

public enum Hdf5OverwritePolicy {

    /**
     * Overwrite old files.
     */
    OVERWRITE("Overwrite"),

    /**
     * Abort if a file exists.
     */
    ABORT("Abort"),
    
    /**
     * Change the name of the new element if a file exists.
     */
    KEEP_BOTH("Keep both");

    private final String m_name;

    /**
     * @param name Name of this policy
     */
    Hdf5OverwritePolicy(final String name) {
        m_name = name;
    }

    /**
     * @return Name of this policy
     */
    String getName() {
        return m_name;
    }

    /**
     * @return Array of all overwrite policy settings
     */
    static String[] getAllSettings() {
        return new String[]{OVERWRITE.getName(), ABORT.getName(), KEEP_BOTH.getName()};
    }

}


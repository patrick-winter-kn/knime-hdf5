package org.knime.hdf5.nodes.writer;

enum OverwritePolicy {

    /**
     * Overwrite old files.
     */
    OVERWRITE("Overwrite"),

    /**
     * Abort if a file exists.
     */
    ABORT("Abort"),
	
    /**
     * Insert if a file exists and put both variants together.
     */
    INSERT("Insert");

    private final String m_name;

    /**
     * @param name Name of this policy
     */
    OverwritePolicy(final String name) {
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
        return new String[] { OVERWRITE.getName(), ABORT.getName(), INSERT.getName() };
    }
}

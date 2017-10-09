package org.knime.hdf5.nodes.reader;

enum OutputSelection {

    /**
     * Location.
     */
    LOCATION("Location"),

    /**
     * URI.
     */
    URI("URI");

    private final String m_name;

    /**
     * @param name Name of this selection
     */
    OutputSelection(final String name) {
        m_name = name;
    }

    /**
     * @return Name of this selection
     */
    String getName() {
        return m_name;
    }

    /**
     * @return Array of all output selection settings
     */
    static String[] getAllSettings() {
        return new String[]{LOCATION.getName(), URI.getName()};
    }

}

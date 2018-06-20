package org.knime.hdf5.nodes.writer;

import java.util.HashMap;
import java.util.Map;

public enum HDF5OverwritePolicy {

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

	private static final Map<String, HDF5OverwritePolicy> LOOKUP = new HashMap<>();

	static {
		for (HDF5OverwritePolicy overwritePolicy : HDF5OverwritePolicy.values()) {
			LOOKUP.put(overwritePolicy.getName(), overwritePolicy);
		}
	}

    private final String m_name;

    /**
     * @param name Name of this policy
     */
    HDF5OverwritePolicy(final String name) {
        m_name = name;
    }

	public static HDF5OverwritePolicy get(String name) {
		return LOOKUP.get(name);
	}
	
    /**
     * @return Name of this policy
     */
    public String getName() {
        return m_name;
    }

    /**
     * @return Array of all overwrite policy settings
     */
    static String[] getAllSettings() {
        return new String[] { OVERWRITE.getName(), ABORT.getName(), INSERT.getName() };
    }
}

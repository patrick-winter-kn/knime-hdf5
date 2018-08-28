package org.knime.hdf5.nodes.writer.edit;

import java.util.HashMap;
import java.util.Map;

enum OverwritePolicy {

    /**
     * Overwrite old TreeNodeEdit.
     */
    OVERWRITE("Overwrite"),

    /**
     * Abort if a TreeNodeEdit exists.
     */
    ABORT("Abort"),
	
    /**
     * Insert if a TreeNodeEdit exists and put both variants together.
     */
    INSERT("Insert"),
	
    /**
     * Keep both TreeNodeEdits for checking their validation if their components
     * can be integrated without issues (like name duplicates).
     */
    KEEP_BOTH("Keep Both");

	private static final Map<String, OverwritePolicy> LOOKUP = new HashMap<>();

	static {
		for (OverwritePolicy overwritePolicy : OverwritePolicy.values()) {
			LOOKUP.put(overwritePolicy.getName(), overwritePolicy);
		}
	}

    private final String m_name;

    /**
     * @param name Name of this policy
     */
    OverwritePolicy(final String name) {
        m_name = name;
    }

	public static OverwritePolicy get(String name) {
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
        return new String[] { OVERWRITE.getName(), ABORT.getName(), INSERT.getName(), KEEP_BOTH.getName() };
    }
}

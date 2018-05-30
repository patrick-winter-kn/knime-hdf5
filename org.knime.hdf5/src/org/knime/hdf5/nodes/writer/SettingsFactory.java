package org.knime.hdf5.nodes.writer;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.node.defaultnodesettings.SettingsModelString;

final class SettingsFactory {

	private static final String OUTPUT_CONFIG_KEY = "_Output";
	
	private static final String OUTPUT_EDIT_TREE_CONFIG_KEY = "fileEditTree";
	
	static enum SpecInfo {
		COLUMN_SPECS("colSpecs"),
		FLOW_VARIABLE_SPECS("varSpecs");
		private static final Map<String, SpecInfo> LOOKUP = new HashMap<>();

		static {
			for (SpecInfo s : EnumSet.allOf(SpecInfo.class)) {
				LOOKUP.put(s.getSpecName(), s);
			}
		}
		
		private final String m_specName;
		
		private List<?> m_specList = new ArrayList<>();
		
		private SpecInfo(String specName) {
			m_specName = specName;
		}
		
		static SpecInfo get(String specName) {
			return LOOKUP.get(specName);
		}
		
		String getSpecName() {
			return m_specName;
		}

		List<?> getSpecList() {
			return m_specList;
		}

		void setSpecList(List<SpecInfo> specList) {
			m_specList = specList;
		}
	}
	
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
	
    /** A new configuration to store the settings of the attribute filter. Also enables the type filter.
     * @return ...
     */
    static final EditTreeConfiguration createEditTreeConfiguration() {
        return new EditTreeConfiguration(OUTPUT_EDIT_TREE_CONFIG_KEY);
    }
}

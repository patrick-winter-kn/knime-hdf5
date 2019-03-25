package org.knime.hdf5.nodes.writer.edit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enum that handles the overwrite policy for {@linkplain TreeNodeEdit}s.
 */
public enum EditOverwritePolicy {
	
    /**
     * Do nothing. Throw an error if a TreeNodeEdit exists.
     */
    NONE("none"),

    /**
     * Ignore the new TreeNodeEdit if an old one exists.
     */
    IGNORE("ignore"),
    
    /**
     * Overwrite the old TreeNodeEdit.
     */
    OVERWRITE("overwrite"),
	
    /**
     * Rename the new TreeNodeEdit.
     */
    RENAME("rename"),
	
    /**
     * Add all children of the new TreeNodeEdit to the old TreeNodeEdit.
     */
    INTEGRATE("integrate");
	private static final Map<String, EditOverwritePolicy> LOOKUP = new HashMap<>();

	static {
		for (EditOverwritePolicy policy : EditOverwritePolicy.values()) {
			LOOKUP.put(policy.getName(), policy);
		}
	}

	private String m_name;
	
	private EditOverwritePolicy(String name) {
		m_name = name;
	}
	
	String getName() {
		return m_name;
	}
	
	public static EditOverwritePolicy get(String name) {
		return LOOKUP.get(name);
	}
	
	/**
	 * @param edit the treeNodeEdit
	 * @return the reasonable policies for the input treeNodeEdit
	 */
	static EditOverwritePolicy[] getAvailablePoliciesForEdit(TreeNodeEdit edit) {
		List<EditOverwritePolicy> values = new ArrayList<>();
		
		if (edit instanceof GroupNodeEdit || edit instanceof DataSetNodeEdit || edit instanceof AttributeNodeEdit) {
			values.add(NONE);
			values.add(IGNORE);
			values.add(OVERWRITE);
			if (edit.getEditAction().isCreateOrCopyAction()) {
				values.add(RENAME);
			}
			if (edit instanceof GroupNodeEdit || edit instanceof DataSetNodeEdit) {
				values.add(INTEGRATE);
			}
		}
		
		return values.toArray(new EditOverwritePolicy[values.size()]);
	}

	/**
	 * @return the reasonable policies for any {@linkplain FileNodeEdit}
	 */
	public static EditOverwritePolicy[] getAvailablePoliciesForFile() {
		return new EditOverwritePolicy[] { INTEGRATE, OVERWRITE, RENAME };
	}
	
	/**
	 * @param policies the overwrite policies
	 * @return the name Strings for the input overwrite policies
	 */
	public static String[] getNames(EditOverwritePolicy[] policies) {
		String[] names = new String[policies.length];
		
		for (int i = 0; i < policies.length; i++) {
			names[i] = policies[i].getName();
		}
		
		return names;
	}
}

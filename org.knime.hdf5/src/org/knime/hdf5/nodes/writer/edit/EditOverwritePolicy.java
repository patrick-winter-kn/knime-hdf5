package org.knime.hdf5.nodes.writer.edit;

import java.util.ArrayList;
import java.util.List;

enum EditOverwritePolicy {
	
    /**
     * Do nothing. Throw an error if this case happens.
     */
    NONE,

    /**
     * Abort if a TreeNodeEdit exists.
     */
    ABORT,
    
    /**
     * Overwrite old TreeNodeEdit.
     */
    OVERWRITE,
	
    /**
     * Rename the new TreeNodeEdit.
     */
    RENAME,
	
    /**
     * Add all children of the new TreeNodeEdit to the old TreeNodeEdit.
     */
    INTEGRATE;
	
	static EditOverwritePolicy[] getAvailableValuesForEdit(TreeNodeEdit edit) {
		List<EditOverwritePolicy> values = new ArrayList<>();
		
		values.add(NONE);
		values.add(ABORT);
		values.add(OVERWRITE);
		if (edit.getEditAction().isCreateOrCopyAction()) {
			values.add(RENAME);
		}
		if (edit instanceof GroupNodeEdit || edit instanceof DataSetNodeEdit) {
			values.add(INTEGRATE);
		}
		
		return values.toArray(new EditOverwritePolicy[values.size()]);
	}
}

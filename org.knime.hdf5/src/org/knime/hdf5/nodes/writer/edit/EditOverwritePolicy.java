package org.knime.hdf5.nodes.writer.edit;

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
	
	static EditOverwritePolicy[] getValuesWithoutIntegrate() {
		return new EditOverwritePolicy[] { NONE, ABORT, OVERWRITE, RENAME };
	}
}

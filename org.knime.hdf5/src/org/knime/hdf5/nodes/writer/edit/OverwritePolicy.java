package org.knime.hdf5.nodes.writer.edit;

enum OverwritePolicy {
	
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
     * Keep both TreeNodeEdits for checking their validation if their components
     * can be integrated without issues (like name duplicates).
     */
    INTEGRATE;
}

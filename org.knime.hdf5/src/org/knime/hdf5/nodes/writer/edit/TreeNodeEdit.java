package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;

/**
 * Abstract class for all edits on objects in an hdf file. The respective hdf
 * source is an {@linkplain Hdf5TreeElement} or an {@linkplain Hdf5Attribute}.
 */
public abstract class TreeNodeEdit {

	/**
	 * Contains all the keys to save/load properties in the settings.
	 */
	protected static enum SettingsKey {
		NAME("name"),
		INPUT_PATH_FROM_FILE_WITH_NAME("inputPathFromFileWithName"),
		EDIT_OVERWRITE_POLICY("editOverwritePolicy"),
		EDIT_ACTION("editAction"),
		FILE_PATH("filePath"),
		INPUT_INVALID_CAUSE("inputInvalidCause"),
		INPUT_TYPE("inputType"),
		POSSIBLE_OUTPUT_TYPES("possibleOutputTypes"),
		OUTPUT_TYPE("outputType"),
		LITTLE_ENDIAN("littleEndian"),
		ROUNDING("rounding"),
		ROUNDING_POSSIBLE("roundingPossible"),
		FIXED_STRING_LENGTH("fixedStringLength"),
		STRING_LENGTH("stringLength"),
		STANDARD_VALUE("standardValue"),
		OVERWRITE_WITH_NEW_COLUMNS("overwriteWithNewColumns"),
		NUMBER_OF_DIMENSIONS("numberOfDimensions"),
		COMPRESSION("compression"),
		CHUNK_ROW_SIZE("chunkRowSize"),
		INPUT_ROW_SIZE("inputRowSize"),
		INPUT_COLUMN_INDEX("inputColumnIndex"),
		OUTPUT_COLUMN_INDEX("outputColumnIndex"),
		TOTAL_STRING_LENGTH("totalStringLength"),
		ITEM_STRING_LENGTH("itemStringLength"),
		FLOW_VARIABLE_ARRAY_POSSIBLE("flowVariableArrayPossible"),
		FLOW_VARIABLE_ARRAY_USED("flowVariableArrayUsed"),
		GROUPS("groups"),
		DATA_SETS("dataSets"),
		ATTRIBUTES("attributes"),
		COLUMNS("columns");

		private String m_key;

		private SettingsKey(String key) {
			m_key = key;
		}
		
		protected String getKey() {
			return m_key;
		}
	}
	
	/**
	 * Contains all types of actions that can be executed to the hdf object.
	 */
	public static enum EditAction {
		CREATE("create"),
		COPY("copy"),
		DELETE("delete"),
		MODIFY("modify"),
		MODIFY_CHILDREN_ONLY("modifyChildrenOnly"),
		NO_ACTION("noAction");
		
		private static final Map<String, EditAction> LOOKUP = new HashMap<>();

		static {
			for (EditAction editAction : EditAction.values()) {
				LOOKUP.put(editAction.getActionName(), editAction);
			}
		}

	    private final String m_actionName;

	    EditAction(final String actionName) {
	    	m_actionName = actionName;
	    }

		public static EditAction get(String actionName) {
			return LOOKUP.get(actionName);
		}

	    public String getActionName() {
	        return m_actionName;
	    }
	    
		public boolean isCreateOrCopyAction() {
			return this == CREATE || this == COPY;
		}
	    
		/**
		 * @return if this edit's or a descendant's action is a modify action
		 */
		public boolean isModifyAction() {
			return this == MODIFY || this == MODIFY_CHILDREN_ONLY;
		}
	}

	/**
	 * Contains all causes why this edit may be invalid, i.e. when executing
	 * the action of this edit, there would always be an error.
	 */
	public static enum InvalidCause {
		FILE_EXTENSION("file extension is not .h5 or .hdf5"),
		FILE_ALREADY_EXISTS("file already exists"),
		NO_DIR_FOR_FILE("no directory for file exists"),
		NAME_DUPLICATE("name duplicate"),
		NO_HDF_SOURCE("no hdf source available"),
		NO_COPY_SOURCE("no source available to copy from"),
		PARENT_DELETE("parent is getting deleted so this also has to be deleted"),
		NAME_CHARS("name contains invalid characters or is empty"),
		NAME_BACKUP_PREFIX("cannot change to name with prefix \"" + BACKUP_PREFIX + "\""),
		ROW_COUNT("dataSet has unequal row sizes"),
		// TODO allow that the dataSet has "too many" columns; if yes, check it for every column which exceeds the limit
		COLUMN_COUNT("dataSet does not have the correct amount of columns"),
		COLUMN_OVERWRITE("cannot find column to overwrite"),
		COLUMN_INDEX("no such column index exists in the dataSet source"),
		INPUT_DATA_TYPE("input data type from source and config do not fit together"),
		INPUT_ROW_SIZE("input row size from source and config do not fit together"),
		OUTPUT_DATA_TYPE("some values do not fit into data type"),
		MISSING_VALUES("there are some missing values");

		private String m_message;

		private InvalidCause(String message) {
			m_message = message;
		}
		
		public String getMessage() {
			return m_message;
		}
	}
	
	/**
	 * Contains all states in those this edit can be.
	 */
	static enum EditState {
		TODO, IN_PROGRESS, CREATE_SUCCESS_WRITE_POSTPONED, SUCCESS, FAIL, ROLLBACK_SUCCESS, ROLLBACK_FAIL, ROLLBACK_NOTHING_TODO;

		/**
		 * @return if this edit has been executed
		 */
		boolean isExecutedState() {
			return this == CREATE_SUCCESS_WRITE_POSTPONED || this == SUCCESS || this == FAIL || isRollbackState();
		}
		
		/**
		 * @return if this edit's rollback action has been used
		 */
		boolean isRollbackState() {
			return this == ROLLBACK_SUCCESS || this == ROLLBACK_FAIL || this == ROLLBACK_NOTHING_TODO;
		}
	}
	
	/**
	 * Stores the success of an edit action.
	 * Should also be set if an exception was thrown.
	 */
	static enum EditSuccess {
		UNDECIDED, TRUE, FALSE;
		
		/**
		 * This method should be called in a try-finally block such that the
		 * decision between TRUE and FALSE is not missed.
		 * 
		 * @param success
		 * @return
		 */
		static EditSuccess getSuccess(boolean success) {
			return success ? TRUE : FALSE;
		}

		boolean isSuccessDecided() {
			return this == TRUE || this == FALSE;
		}
		
		boolean didNotFail() {
			return this != FALSE;
		}
	}
	
	static final String BACKUP_PREFIX = "temp_";
	
	private String m_inputPathFromFileWithName;
	
	private String m_outputPathFromFile;
	
	private String m_name;
	
	private EditOverwritePolicy m_editOverwritePolicy;

	private TreeNodeMenu m_treeNodeMenu;
	
	protected DefaultMutableTreeNode m_treeNode;
	
	private TreeNodeEdit m_parent;

	private Object m_hdfObject;
	
	private Object m_hdfBackup;
	
	private EditAction m_editAction = EditAction.NO_ACTION;
	
	/**
	 * The edit which is used to copy from for {@linkplain EditAction#COPY} is copy source.
	 */
	private TreeNodeEdit m_copyEdit;
	
	/**
	 * The map from all invalid descendants of this edit to their invalid cause.
	 */
	private Map<TreeNodeEdit, InvalidCause> m_invalidEdits = new HashMap<>();
	
	/**
	 * The cause if the hdf object of this edit is not supported
	 * (or {@code null} if it is supported).
	 */
	private String m_unsupportedCause = null;
	
	private EditState m_editState = EditState.TODO;
	
	private EditSuccess m_editSuccess = EditSuccess.UNDECIDED;

	/**
	 * Initializes a new edit with all core settings.
	 * 
	 * @param inputPathFromFileWithName the input path from file or the name of
	 * 	the input knime spec
	 * @param outputPathFromFile the output path from file
	 * @param name the output name of this edit
	 * @param editOverwritePolicy the overwrite policy for this edit
	 * @param editAction the action of this edit
	 */
	TreeNodeEdit(String inputPathFromFileWithName, String outputPathFromFile, String name, EditOverwritePolicy editOverwritePolicy, EditAction editAction) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
		m_outputPathFromFile = outputPathFromFile;
		m_name = name;
		m_editOverwritePolicy = editOverwritePolicy;
		setEditAction(editAction);
	}
	
	/**
	 * Get a name of a new child hdf object of {@code parent} that does not have
	 * a name conflict with any other child of {@code parent} depending on the
	 * {@code editClass}.
	 * <br>
	 * See {@linkplain Hdf5TreeElement#getUniqueName(List, String)} for more
	 * details on the construction of the unique name.
	 * 
	 * @param parent the parent edit
	 * @param editClass the class of the new child edit
	 * @param name the name for the new child edit
	 * @return the unique name for the new child edit
	 * @see Hdf5TreeElement#getUniqueName(List, String)
	 * @see TreeNodeEdit#getNamesOfChildrenWithNameConflictPossible(Class)
	 */
	public static String getUniqueName(TreeNodeEdit parent, Class<? extends TreeNodeEdit> editClass, String name) {
		return Hdf5TreeElement.getUniqueName(parent.getNamesOfChildrenWithNameConflictPossible(editClass), name);
	}
	
	/**
	 * @return the input path where this edit's hdf object was located
	 * 	within the hdf file including its name
	 */
	public String getInputPathFromFileWithName() {
		return m_inputPathFromFileWithName;
	}
	
	/**
	 * @return the output path where this edit's hdf object should be located
	 * 	within the hdf file
	 */
	public String getOutputPathFromFile() {
		return m_outputPathFromFile;
	}

	/**
	 * Sets a new output path from file and updates those of all descendants.
	 * 
	 * @param outputPathFromFile the output path from file
	 */
	private void setOutputPathFromFile(String outputPathFromFile) {
		if (!m_outputPathFromFile.equals(outputPathFromFile)) {
			m_outputPathFromFile = outputPathFromFile;
			for (TreeNodeEdit edit : getAllChildren()) {
				edit.setOutputPathFromFile(getOutputPathFromFileWithName());
			}
		}
	}
	
	/**
	 * @return the name of this edit's hdf object after executing the action
	 */
	public String getName() {
		return m_name;
	}
	
	/**
	 * Sets the new name of this edit and updates the output paths from file
	 * of all descendants.
	 * 
	 * @param name the new name of this edit
	 */
	void setName(String name) {
		if (!m_name.equals(name)) {
			m_name = name;
			for (TreeNodeEdit edit : getAllChildren()) {
				edit.setOutputPathFromFile(getOutputPathFromFileWithName());
			}
		}
	}
	
	/**
	 * @return the overwrite policy of this edit if an object already exists
	 * 	on the output location
	 */
	EditOverwritePolicy getEditOverwritePolicy() {
		return m_editOverwritePolicy;
	}

	protected void setEditOverwritePolicy(EditOverwritePolicy editOverwritePolicy) {
		m_editOverwritePolicy = editOverwritePolicy;
	}
	
	/**
	 * @return the popup menu which can be accessed through a right mouse click
	 * 	on the edit
	 */
	public TreeNodeMenu getTreeNodeMenu() {
		return m_treeNodeMenu;
	}
	
	/**
	 * Sets and updates the tree node menu of this edit.
	 * 
	 * @param treeNodeMenu the tree node menu of this edit
	 */
	protected void setTreeNodeMenu(TreeNodeMenu treeNodeMenu) {
		m_treeNodeMenu = treeNodeMenu;
		if (m_treeNodeMenu != null) {
			m_treeNodeMenu.updateMenuItems();
		}
	}
	
	/**
	 * @return the container node for this edit used in the {@linkplain JTree}
	 * 	which represents all edits
	 */
	public DefaultMutableTreeNode getTreeNode() {
		return m_treeNode;
	}

	/**
	 * @return the parent of this edit based on the output path from file
	 */
	public TreeNodeEdit getParent() {
		return m_parent;
	}
	
	/**
	 * Sets the parent of this edit and updates the invalid map and the edit
	 * actions of the previous and current parent.
	 * 
	 * @param parent the new parent of this edit
	 */
	protected void setParent(TreeNodeEdit parent) {
		if (m_parent != parent) {
			if (m_parent != null && parent == null) {
				m_parent.updateInvalidMap(this, null);
				m_parent.removeModifyChildrenProperty();
			}
			m_parent = parent;
			if (m_parent != null) {
				updateParentEditAction();
			}
		}
	}

	/**
	 * @return the hdf object which is edited by this edit
	 */
	public Object getHdfObject() {
		return m_hdfObject;
	}

	protected void setHdfObject(Hdf5TreeElement hdfObject) {
		m_hdfObject = hdfObject;
		if (m_hdfObject != null && m_hdfObject == m_hdfBackup) {
			m_hdfBackup = null;
		}
	}
	
	protected void setHdfObject(Hdf5Attribute<?> hdfObject) {
		m_hdfObject = hdfObject;
		if (m_hdfObject != null && m_hdfObject == m_hdfBackup) {
			m_hdfBackup = null;
		}
	}
	
	/**
	 * @return the backup for the hdf object if the original version got deleted
	 */
	protected Object getHdfBackup() {
		return m_hdfBackup;
	}

	private void setHdfBackup(Hdf5TreeElement hdfBackup) {
		if (m_hdfObject == null || m_hdfObject != m_hdfBackup) {
			m_hdfBackup = hdfBackup;
		}
	}
	
	private void setHdfBackup(Hdf5Attribute<?> hdfBackup) {
		if (m_hdfObject == null || m_hdfObject != m_hdfBackup) {
			m_hdfBackup = hdfBackup;
		}
	}

	/**
	 * @return the type of action that is executed on this edit's hdf source
	 */
	public EditAction getEditAction() {
		return m_editAction;
	}
	
	/**
	 * Sets the edit action (if possible) and updates the edit actions of its ancestors.
	 * <br>
	 * It is not possible to set the edit action from COPY/CREATE/DELETE back
	 * to one of the others.
	 * <br>
	 * <br>
	 * <b>Note:</b>When deciding if the edit action should be DELETE or not, use
	 * {@linkplain TreeNodeEdit#setDeletion(boolean)}
	 * 
	 * @param editAction the new edit action
	 * @see TreeNodeEdit#setDeletion(boolean)
	 */
	protected void setEditAction(EditAction editAction) {
		if (!editAction.isModifyAction() || m_editAction == EditAction.NO_ACTION || m_editAction == EditAction.MODIFY_CHILDREN_ONLY) {
			m_editAction = editAction;
			
			if (m_parent != null && m_parent.getEditAction() == EditAction.NO_ACTION && m_editAction != EditAction.NO_ACTION) {
				m_parent.setEditAction(EditAction.MODIFY_CHILDREN_ONLY);
			}
		}
	}

	/**
	 * @return the edit that is used for {@linkplain EditAction#COPY} as copy source
	 */
	protected TreeNodeEdit getCopyEdit() {
		return m_copyEdit;
	}
	
	/**
	 * Sets the copy edit of this edit to {@code copyEdit} or to the copy edit
	 * of {@code copyEdit} if it exists such that the copy edit is the proper
	 * copy source for this edit.
	 * 
	 * @param copyEdit the new copy edit
	 * @throws IllegalArgumentException if this edit and {@code copyEdit} are
	 * 	the same instance or do not have the same {@linkplain FileNodeEdit}
	 * 	as root
	 */
	protected void setCopyEdit(TreeNodeEdit copyEdit) throws IllegalArgumentException {
		if (copyEdit != null && getRoot() != copyEdit.getRoot()) {
			throw new IllegalArgumentException("Copy source must have the same root as this edit");
			
		} else if (this == copyEdit) {
			throw new IllegalArgumentException("Copy source may not be this edit ifself");
		}
		
		m_copyEdit = copyEdit != null && copyEdit.getCopyEdit() != null ? copyEdit.getCopyEdit() : copyEdit;
	}

	public boolean isSupported() {
		return m_unsupportedCause == null;
	}
	
	/**
	 * Sets the cause why this edit is not supported or {@code null} if this
	 * edit is supported.
	 * 
	 * @param unsupportedCause the new unsupported cause
	 */
	void setUnsupportedCause(String unsupportedCause) {
		m_unsupportedCause = unsupportedCause;
	}

	public EditState getEditState() {
		return m_editState;
	}
	
	protected void setEditState(EditState editState) {
		m_editState = editState;
	}
	
	protected void setEditSuccess(boolean success) {
		m_editSuccess = EditSuccess.getSuccess(success);
	}
	
	/**
	 * Resets this edit's success to {@linkplain EditSuccess#UNDECIDED}.
	 */
	private void resetSuccess() {
		m_editSuccess = EditSuccess.UNDECIDED;
	}

	/**
	 * Integrates the attributeEdits of {@code copyEdit} into this edit.
	 * 
	 * @param copyEdit the edit to integrate
	 */
	protected void integrateAttributeEdits(TreeNodeEdit copyEdit) {
		if ((this instanceof GroupNodeEdit || this instanceof DataSetNodeEdit)
				&& (copyEdit instanceof GroupNodeEdit || copyEdit instanceof DataSetNodeEdit)) {
			AttributeNodeEdit[] copyAttributeEdits = copyEdit instanceof GroupNodeEdit
					? ((GroupNodeEdit) copyEdit).getAttributeNodeEdits() : ((DataSetNodeEdit) copyEdit).getAttributeNodeEdits();
					
			for (AttributeNodeEdit copyAttributeEdit : copyAttributeEdits) {
				if (copyAttributeEdit.getEditAction() != EditAction.NO_ACTION) {
					AttributeNodeEdit attributeEdit = this instanceof GroupNodeEdit
							? ((GroupNodeEdit) this).getAttributeNodeEdit(copyAttributeEdit.getInputPathFromFileWithName(), copyAttributeEdit.getEditAction())
							: ((DataSetNodeEdit) this).getAttributeNodeEdit(copyAttributeEdit.getInputPathFromFileWithName(), copyAttributeEdit.getEditAction());
					
					boolean isCreateOrCopyAction = copyAttributeEdit.getEditAction().isCreateOrCopyAction();
					if (attributeEdit != null && !isCreateOrCopyAction) {
						attributeEdit.copyPropertiesFrom(copyAttributeEdit);
					} else {
						copyAttributeEdit.copyAttributeEditTo(this, false);
					}
				}
			}
		}
	}
	
	/**
	 * Copies this edit to {@code parent} without copy source.
	 * 
	 * @param parent the parent of the new edit
	 * @return the new edit
	 */
	TreeNodeEdit copyWithoutCopySourceTo(TreeNodeEdit parent) {
		TreeNodeEdit copyEdit = null;
		
		if (this instanceof GroupNodeEdit) {
			copyEdit = ((GroupNodeEdit) this).copyGroupEditTo((GroupNodeEdit) parent, false);
			
		} else if (this instanceof DataSetNodeEdit) {
			copyEdit = ((DataSetNodeEdit) this).copyDataSetEditTo((GroupNodeEdit) parent, false);
			
		} else if (this instanceof ColumnNodeEdit) {
			copyEdit = ((ColumnNodeEdit) this).copyColumnEditTo((DataSetNodeEdit) parent, false);
			
		} else if (this instanceof AttributeNodeEdit) {
			copyEdit = ((AttributeNodeEdit) this).copyAttributeEditTo(parent, false);
		}
		
		return copyEdit;
	}
	
	/**
	 * Checks if the properties of this edit have changed in a way
	 * (based on {@code hdfSource}) that the hdf source has to be deleted and
	 * recreated in the edit's execution.
	 * 
	 * @param hdfSource the hdf source to compare with
	 * @return if the properties of this edit have changed
	 */
	protected abstract boolean havePropertiesChanged(Object hdfSource);
	
	/**
	 * Copies the properties from {@code copyEdit} to this edit.
	 * 
	 * @param copyEdit the edit with the properties to copy from
	 */
	protected void copyPropertiesFrom(TreeNodeEdit copyEdit) {
		copyCorePropertiesFrom(copyEdit);
		copyAdditionalPropertiesFrom(copyEdit);
	}
	
	/**
	 * Copies the core properties (properties which every TreeNodeEdit has)
	 * from {@code copyEdit} to this edit.
	 * 
	 * @param copyEdit the edit with the core properties to copy from
	 */
	private void copyCorePropertiesFrom(TreeNodeEdit copyEdit) {
		m_inputPathFromFileWithName = copyEdit.getInputPathFromFileWithName();
		m_outputPathFromFile = copyEdit.getOutputPathFromFile();
		m_name = copyEdit.getName();
		m_editOverwritePolicy = copyEdit.getEditOverwritePolicy();
		setEditAction(copyEdit.getEditAction());
		m_treeNodeMenu.updateMenuItems();
		if (m_editAction == EditAction.COPY) {
			setCopyEdit(copyEdit.getCopyEdit());
		}
	}

	/**
	 * Copies additional properties (properties those existence depends on the
	 * class of the edit) from {@code copyEdit} to this edit.
	 * 
	 * @param copyEdit the edit with the additional properties to copy from
	 */
	protected abstract void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit);

	/**
	 * Checks if there is still a name conflict between this edit and {@code edit}
	 * using the overwrite policies of both.
	 * 
	 * @param edit the edit with the possible name conflict to this edit
	 * @return if there is a name conflict considering the overwrite policies
	 */
	protected boolean avoidsOverwritePolicyNameConflict(TreeNodeEdit edit) {
		if (getClass() == edit.getClass()) {
			boolean wasHereBefore = !getEditAction().isCreateOrCopyAction() && getOutputPathFromFileWithName(true).equals(getInputPathFromFileWithName());
			if (wasHereBefore || !edit.getEditAction().isCreateOrCopyAction() && edit.getOutputPathFromFileWithName(true).equals(edit.getInputPathFromFileWithName())) {
				EditOverwritePolicy policy = (wasHereBefore ? edit : this).getEditOverwritePolicy();
				if (policy == EditOverwritePolicy.INTEGRATE) {
					// TODO does not include every case
					for (TreeNodeEdit childEdit : getAllChildren()) {
						for (TreeNodeEdit childEdit2 : edit.getAllChildren()) {
							if (childEdit.isInConflict(childEdit2)) {
								return false;
							}
						}
					}
					return true;
					
				} else {
					return policy != EditOverwritePolicy.NONE;
				}
			}
		}
		return false;
	}
	
	/**
	 * Checks if {@code edit} with MODIFY action with overwrite policy IGNORE will
	 * be ignored and this causes a name conflict between this edit and {@code edit}.
	 * 
	 * @param edit the edit that might be ignored
	 * @return if there is a name conflict because {@code edit} will be ignored
	 */
	protected boolean willBeNameConflictWithIgnoredEdit(TreeNodeEdit edit) {
		if (mayBeNameConflictWithIgnoredEdit(edit)) {
			List<TreeNodeEdit> conflictEdits = new ArrayList<>();
			TreeNodeEdit conflictEdit = edit;
			
			// continue the search after ignored edits to find a cycle of all ignored edits
			do {
				conflictEdits.add(conflictEdit);
				conflictEdit = conflictEdit.getEditWhyActionIsIgnored();
			} while (conflictEdit != null && !conflictEdits.contains(conflictEdit)
					&& conflictEdit.getEditAction() == EditAction.MODIFY && conflictEdit.getEditOverwritePolicy() == EditOverwritePolicy.IGNORE);
			
			return conflictEdit != null;
		}
		
		return false;
	}

	/**
	 * Checks there will be a name conflict between this edit and {@code edit} if
	 * {@code edit} is ignored.
	 * 
	 * @param edit the edit that might be ignored
	 * @return if there might be a name conflict because {@code edit} will be ignored
	 */
	private boolean mayBeNameConflictWithIgnoredEdit(TreeNodeEdit edit) {
		return edit.getEditAction() == EditAction.MODIFY && edit.getEditOverwritePolicy() == EditOverwritePolicy.IGNORE
				&& m_parent == edit.getParent() && getName().equals(Hdf5TreeElement.getPathAndName(edit.getInputPathFromFileWithName())[1]);
	}
	
	/**
	 * @return the edit why this edit with MODIFY action and overwrite policy
	 * 	IGNORE will be ignored
	 */
	private TreeNodeEdit getEditWhyActionIsIgnored() {
		for (TreeNodeEdit edit : m_parent.getAllChildren()) {
			if (mayBeNameConflictWithIgnoredEdit(edit)
					|| getName().equals(edit.getName()) && this != edit && edit.getEditAction() != EditAction.DELETE) {
				return edit;
			}
		}
		
		return null;
	}
	
	protected Hdf5TreeElement getOpenedHdfObjectOfParent() throws IOException, NullPointerException {
		Hdf5TreeElement parentObject = (Hdf5TreeElement) m_parent.getHdfObject();
		
		if (parentObject != null && !parentObject.isFile()) {
			parentObject.open();
		}
		
		return parentObject;
	}
	
	/**
	 * @return the hdf backup if available, the hdf object otherwise (this
	 * edit is invalid if this method returns {@code null} and the action
	 * is neither COPY nor CREATE)
	 */
	Object getHdfSource() {
		return m_hdfBackup != null ? m_hdfBackup : m_hdfObject;
	}
	
	/**
	 * Finds the copy source for the edits with COPY action.
	 * 
	 * @return the copy source for the edits with COPY action
	 * @throws IOException if no copy source exists
	 */
	Object findCopySource() throws IOException {
		Object copySource = null;
		
		FileNodeEdit fileEdit = getRoot();
		if (this instanceof GroupNodeEdit) {
			GroupNodeEdit copyGroupEdit = fileEdit.getGroupEditByPath(m_inputPathFromFileWithName);
			if (copyGroupEdit != null) {
				copySource = copyGroupEdit.getHdfSource();
			}
			if (copySource == null) {
				copySource = ((Hdf5File) fileEdit.getHdfObject()).getGroupByPath(m_inputPathFromFileWithName);
			}
		} else if (this instanceof DataSetNodeEdit || this instanceof ColumnNodeEdit) {
			DataSetNodeEdit copyDataSetEdit = fileEdit.getDataSetEditByPath(m_inputPathFromFileWithName);
			if (copyDataSetEdit != null) {
				copySource = copyDataSetEdit.getHdfSource();
			}
			if (copySource == null) {
				copySource = ((Hdf5File) fileEdit.getHdfObject()).getDataSetByPath(m_inputPathFromFileWithName);
			}
		} else if (this instanceof AttributeNodeEdit) {
			AttributeNodeEdit copyAttributeEdit = fileEdit.getAttributeEditByPath(m_inputPathFromFileWithName);
			if (copyAttributeEdit != null) {
				copySource = copyAttributeEdit.getHdfSource();
			}
			if (copySource == null) {
				copySource = ((Hdf5File) fileEdit.getHdfObject()).getAttributeByPath(m_inputPathFromFileWithName);
			}
		}

		return copySource;
	}
	
	/**
	 * Creates a backup from this edit's hdf object.
	 * 
	 * @return if the backup was created successfully
	 */
	boolean createBackup() {
		try {
			if (this instanceof FileNodeEdit) {
				setHdfBackup(((Hdf5File) m_hdfObject).createBackup(BACKUP_PREFIX));
			} else {
				Object parentBackup = (this instanceof ColumnNodeEdit ? m_parent.getParent() : m_parent).getHdfBackup();
				if (parentBackup != null) {
					if (this instanceof GroupNodeEdit) {
						setHdfBackup(((Hdf5Group) parentBackup).getGroup(Hdf5TreeElement.getPathAndName(m_inputPathFromFileWithName)[1]));
					} else if (this instanceof DataSetNodeEdit || this instanceof ColumnNodeEdit) {
						setHdfBackup(((Hdf5Group) parentBackup).getDataSet(Hdf5TreeElement.getPathAndName(m_inputPathFromFileWithName)[1]));
					} else if (this instanceof AttributeNodeEdit) {
						setHdfBackup(((Hdf5TreeElement) parentBackup).getAttribute(Hdf5TreeElement.getPathAndName(m_inputPathFromFileWithName)[1]));
					}
				} else {
					if (this instanceof AttributeNodeEdit) {
						setHdfBackup(((Hdf5Attribute<?>) m_hdfObject).createBackup(BACKUP_PREFIX));
						
					} else if (this instanceof ColumnNodeEdit) {
						if (m_parent.getHdfBackup() == null) {
							m_parent.createBackup();
						}
						setHdfBackup((Hdf5DataSet<?>) m_parent.getHdfBackup());
						
					} else {
						setHdfBackup(((Hdf5TreeElement) m_hdfObject).createBackup(BACKUP_PREFIX));
					}
				}
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).warn("Backup could not be created: " + ioe.getMessage(), ioe);
		}
		
		return m_hdfBackup != null;
	}
	
	/**
	 * Deletes the hdf backup if available.
	 * <br>
	 * <br>
	 * <b>Note:</b> Be careful that the instance of the deleted backup is
	 * not usable anymore.
	 * 
	 * @return if the backup was deleted successfully
	 */
	boolean deleteBackup() {
		boolean success = true;
		
		try {
			if (m_hdfBackup != null) {
				if (m_hdfBackup instanceof Hdf5File) {
					Hdf5File file = (Hdf5File) m_hdfBackup;
					success = file.exists() ? file.deleteFile() : true;
				} else if (m_hdfBackup instanceof Hdf5TreeElement) {
					Hdf5TreeElement treeElement = (Hdf5TreeElement) m_hdfBackup;
					success = treeElement.exists() ? treeElement.getParent().deleteObject(treeElement.getName()) : true;
				} else if (m_hdfBackup instanceof Hdf5Attribute<?>) {
					Hdf5Attribute<?> attribute = (Hdf5Attribute<?>) m_hdfBackup;
					success = attribute.exists() ? attribute.getParent().deleteAttribute(attribute.getName()) : true;
				}
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).warn("Backup could not be deleted: " + ioe.getMessage(), ioe);
			success = false;
		}
		
		if (success) {
			if (m_hdfBackup instanceof Hdf5TreeElement) {
				setHdfBackup((Hdf5TreeElement) null);
			} else if (m_hdfBackup instanceof Hdf5Attribute<?>) {
				setHdfBackup((Hdf5Attribute<?>) null);
			}
		}
		
		return success;
	}
	
	/**
	 * Finds and updates the copy sources for all descendant edits with COPY actions.
	 */
	protected void updateCopySources() {
    	if (m_editAction == EditAction.COPY) {
			setCopyEdit(findCopyEdit());
		}
    	
		for (TreeNodeEdit edit : getAllChildren()) {
			edit.updateCopySources();
		}
	}
	
	public boolean isValid() {
		return m_invalidEdits.isEmpty();
	}
	
	/**
	 * Updates the invalid cause of this edit.
	 * 
	 * @param cause the invalid cause
	 */
	void updateInvalidMap(InvalidCause cause) {
		updateInvalidMap(this, cause);
	}
	
	/**
	 * Updates the invalid cause of the input edit.
	 * 
	 * @param edit the input edit
	 * @param cause the invalid cause
	 */
	private void updateInvalidMap(TreeNodeEdit edit, InvalidCause cause) {
		if (cause != null) {
			m_invalidEdits.put(edit, cause);
		} else if (m_invalidEdits.containsKey(edit)) {
			m_invalidEdits.remove(edit);
		}
		if (m_parent != null) {
			m_parent.updateInvalidMap(edit, cause);
		}
	}
	
	/**
	 * @return all descendant edits that are resettable because they are invalid
	 */
	public TreeMap<TreeNodeEdit, InvalidCause> getResettableEdits() {
		TreeMap<TreeNodeEdit, InvalidCause> resettableEdits = new TreeMap<>(new TreeNodeEditComparator());
		for (TreeNodeEdit edit : m_invalidEdits.keySet()) {
			InvalidCause cause = m_invalidEdits.get(edit);
			if (edit instanceof ColumnNodeEdit) {
				edit = edit.getParent();
				if (m_invalidEdits.containsKey(edit)) {
					continue;
				}
			}
			if (!resettableEdits.containsKey(edit) && (cause != InvalidCause.NAME_DUPLICATE || edit.isOverwriteApplicable())) {
				resettableEdits.put(edit, cause);
			}
		}
		return resettableEdits;
	}
	
	/**
	 * Get a listing of messages of all invalid descendants of this edit and {@code otherEdit}.
	 * If this edit and {@code otherEdit}) have a same invalid descendant, the message for this edit will be used.
	 * 
	 * @param otherEdit the other edit to see those messages
	 * @return the listing of messages of all invalid descendants
	 */
	public String getInvalidCauseMessages(TreeNodeEdit otherEdit) {
		Map<TreeNodeEdit, InvalidCause> invalidEditCauseMap = new HashMap<>();
		invalidEditCauseMap.putAll(otherEdit.m_invalidEdits);
		invalidEditCauseMap.putAll(m_invalidEdits);
		
		StringBuilder messageBuilder = new StringBuilder();
		
		TreeNodeEdit[] invalidEdits = invalidEditCauseMap.keySet().toArray(new TreeNodeEdit[0]);
		Arrays.sort(invalidEdits, new TreeNodeEditComparator());
		int maxMessageCountPerCause = 10;
		for (InvalidCause invalidCause : InvalidCause.values()) {
			int found = 0;
			int count = 0;
			String curInvalidPath = null;
			Class<? extends TreeNodeEdit> curClass = null;
			for (TreeNodeEdit invalidEdit : invalidEdits) {
				if (invalidCause == invalidEditCauseMap.get(invalidEdit)) {
					invalidEditCauseMap.remove(invalidEdit);
					String path = invalidEdit.getOutputPathFromFileWithName(true);
					if (!path.equals(curInvalidPath) || invalidEdit.getClass() != curClass) {
						found++;
						curInvalidPath = path;
						curClass = invalidEdit.getClass();
						if (found <= maxMessageCountPerCause) {
							messageBuilder.append(found == 1 ? invalidCause.getMessage() + ":" : "");
							messageBuilder.append(count > 1 ? " (count: " + count + ")" : "");
							messageBuilder.append("\n- " + invalidEdit.getHdfObjectType() + " " + curInvalidPath);
							count = 1;
						}
					} else if (found <= maxMessageCountPerCause) {
						count++;
					}
				}
			}
			messageBuilder.append(count > 1 ? " (count: " + count + ")" : "");
			messageBuilder.append(found > maxMessageCountPerCause ? "\n- and "
					+ (found - maxMessageCountPerCause) + " more\n" : found > 0 ? "\n" : "");
		}
		
		return messageBuilder.toString();
	}
	
	/**
	 * @return the information of this edit that is show on the tooltip
	 */
	public String getToolTipText() {
		return (isSupported() ? (this instanceof FileNodeEdit ? ((FileNodeEdit) this).getFilePath() : m_inputPathFromFileWithName != null ? m_inputPathFromFileWithName : "NEW")
						+ (this instanceof ColumnNodeEdit && m_editAction != EditAction.CREATE ? "/" + m_name : "") : "NOT SUPPORTED [" + m_unsupportedCause + "]")
				+ (isValid() ? "" : " (invalid" + (m_invalidEdits.containsKey(this) ? " - " + m_invalidEdits.get(this).getMessage() : " children") + ")");
	}
	
	/**
	 * Updates the edit state and adds the total progress in this edit if the edit
	 * state was IN_PROGRESS before.
	 * 
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @throws IOException if an error occurred while calculating the total
	 * 	progress to do in this edit
	 */
	private void updateStateAndProgress(ExecutionContext exec, long totalProgressToDo) throws IOException {
		if (m_editState == EditState.IN_PROGRESS && m_editSuccess.isSuccessDecided()) {
			if (m_editSuccess == EditSuccess.TRUE) {
				addProgress(getProgressToDoInEdit(), exec, totalProgressToDo, true);
				setEditState(EditState.SUCCESS);
				
			} else {
				setEditState(EditState.FAIL);
			}
		}
	}
	
	/**
	 * Adds the done progress to the {@link ExecutionContext} {@code exec}.
	 * 
	 * @param progressToAdd the progress to add
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @param updateMessage if the message in the execution context should be
	 * 	updated (the message shows the last edit which has added some progress)
	 */
	protected void addProgress(long progressToAdd, ExecutionContext exec, long totalProgressToDo, boolean updateMessage) {
		// TODO change after testing
		long progressDone = Math.round(exec.getProgressMonitor().getProgress() * totalProgressToDo) + progressToAdd;
		exec.setProgress((double) progressDone / totalProgressToDo);
		if (updateMessage) {
			exec.setMessage(getSummary());
		}
		System.out.println("Progress done (Edit " + getOutputPathFromFileWithName() + "): " + progressDone);
	}
	
	/**
	 * Get a summary of the edit states of all descendant edits.
	 * 
	 * @param rollback {@code true} if the rollback edit states should be shown,
	 *  {@code false} if the normal edit states should be shown
	 * @return the summary of the edit states
	 */
	public String getSummaryOfEditStates(boolean rollback) {
		StringBuilder summaryBuilder = new StringBuilder();
		
		List<TreeNodeEdit> descendants = getAllDecendants();
		for (EditState state : EditState.values()) {
			if (rollback == state.isRollbackState()) {
				summaryBuilder.append("State " + state + ":\n");
				for (TreeNodeEdit descendant : descendants.toArray(new TreeNodeEdit[descendants.size()])) {
					if (state == descendant.getEditState()) {
						descendants.remove(descendant);
						if (!(descendant instanceof ColumnNodeEdit)) {
							summaryBuilder.append("- " + descendant.getSummary() + "\n");
						}
					}
				}
			}
		}
		
		return summaryBuilder.toString();
	}
	
	/**
	 * @return information about the hdf object type, output file path with name and edit action
	 */
	public String getSummary() {
		return getHdfObjectType() + " " + getOutputPathFromFileWithName(true) + " (" + m_editAction + ")";
	}
	
	private String getHdfObjectType() {
		if (this instanceof FileNodeEdit) {
			return "file";
		} else if (this instanceof GroupNodeEdit) {
			return "group";
		} else if (this instanceof DataSetNodeEdit) {
			return "dataSet";
		} else if (this instanceof ColumnNodeEdit) {
			return "column";
		} else if (this instanceof AttributeNodeEdit) {
			return "attribute";
		}
		return "unsupported object";
	}
	
	/**
	 * @return the total progress which will be done during the execution for
	 * 	this edit
	 * @throws IOException if the hdf source is needed and cannot be found
	 */
	protected abstract long getProgressToDoInEdit() throws IOException;
	
	private TreeNodeEdit findCopyEdit() {
		if (this instanceof GroupNodeEdit) {
			return getRoot().getGroupEditByPath(m_inputPathFromFileWithName);
		} else if (this instanceof DataSetNodeEdit) {
			return getRoot().getDataSetEditByPath(m_inputPathFromFileWithName);
		} else if (this instanceof ColumnNodeEdit) {
			return getRoot().getColumnEditByPath(m_inputPathFromFileWithName, ((ColumnNodeEdit) this).getInputColumnIndex());
		} else if (this instanceof AttributeNodeEdit) {
			return getRoot().getAttributeEditByPath(m_inputPathFromFileWithName);
		}
		return null;
	}

	/**
	 * @return the output path from file with name (without check that there
	 * 	slashes in name)
	 */
	public String getOutputPathFromFileWithName() {
		return getOutputPathFromFileWithName(false);
	}
	
	/**
	 * @param checkSlashesInName if it should be checked for slashes in the name
	 * 	and they should be replaced by '\/'
	 * @return the output path from file with name
	 */
	String getOutputPathFromFileWithName(boolean checkSlashesInName) {
		return (!m_outputPathFromFile.isEmpty() ? m_outputPathFromFile + "/" : "") + (checkSlashesInName ? m_name.replaceAll("/", "\\\\/") : m_name);
	}

	/**
	 * Reloads the {@link JTree} and ensures that this edit is visible.
	 */
	public void reloadTreeWithEditVisible() {
		reloadTreeWithEditVisible(false);
	}

	/**
	 * Reloads the {@link JTree} and ensures that this edit is visible and also
	 * its children if {@code childrenVisible} is set to {@code true}.
	 * 
	 * @param childrenVisible if this edit's children should also be visible
	 */
	public void reloadTreeWithEditVisible(boolean childrenVisible) {
		FileNodeEdit root = getRoot();
		root.reloadTree();
		if (childrenVisible) {
			TreeNodeEdit[] childEdits = getAllChildren();
			root.makeTreeNodeVisible(childEdits.length > 0 ? childEdits[0] : this);
		} else {
			root.makeTreeNodeVisible(this);
		}
	}
	
	/**
	 * @return the root of this edit
	 */
	protected FileNodeEdit getRoot() {
		return m_parent != null ? m_parent.getRoot() : (FileNodeEdit) this;
	}
	
	/**
	 * @return the path from the root
	 */
	private TreeNodeEdit[] getPath() {
		List<TreeNodeEdit> editsInPath = new ArrayList<>();
		editsInPath.add(this);
		while (editsInPath.get(0).getParent() != null) {
			editsInPath.add(0, editsInPath.get(0).getParent());
		}
		return editsInPath.toArray(new TreeNodeEdit[editsInPath.size()]);
	}
	
	/**
	 * Checks if this edit is a descendant of {@code other}. It is also a
	 * descendant if {@code this == other}.
	 * 
	 * @param other the possible ancestor
	 * @return if this edit is a descendant of {@code other}
	 */
	public boolean isEditDescendantOf(TreeNodeEdit other) {
		TreeNodeEdit[] thisPath = getPath();
		TreeNodeEdit[] otherPath = other.getPath();
		
		for (int i = 0; i < thisPath.length; i++) {
			if (i >= otherPath.length || thisPath[i] != otherPath[i]) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Updates the edit action of the parent.
	 */
	private void updateParentEditAction() {
		setEditAction(getEditAction());
	}
	
	/**
	 * Sets the edit action MODIFY_CHILDREN_ONLY back to NO_ACTION if possible.
	 */
	private void removeModifyChildrenProperty() {
		boolean removeModifyChildrenProperty = m_editAction == EditAction.MODIFY_CHILDREN_ONLY;
		if (removeModifyChildrenProperty) {
			for (TreeNodeEdit child : getAllChildren()) {
				if (child.getEditAction() != EditAction.NO_ACTION) {
					removeModifyChildrenProperty = false;
					break;
				}
			}
		}
		if (removeModifyChildrenProperty) {
			setEditAction(EditAction.NO_ACTION);
			if (m_parent != null) {
				m_parent.removeModifyChildrenProperty();
			}
		}
	}
	
	/**
	 * Sets the edit action to DELETE if {@code toDelete == true} and to
	 * MODIFY/NO_ACTION otherwise. Also does the same for all
	 * descendants.
	 * <br>
	 * <br>
	 * If this edit has no hdf source, it will be removed from its parent
	 * instead of letting it be invalid.
	 * 
	 * @param toDelete if the edit action should be set to DELETE or not
	 */
	void setDeletion(boolean toDelete) {
		if (toDelete != (getEditAction() == EditAction.DELETE)) {
	    	if (toDelete && getEditAction().isCreateOrCopyAction() || !toDelete && m_invalidEdits.get(this) == InvalidCause.NO_HDF_SOURCE && getEditAction() == EditAction.DELETE) {
	    		removeFromParentCascade();
	    		
	    	} else {
	    		m_editAction = toDelete ? EditAction.DELETE : (this instanceof ColumnNodeEdit ? EditAction.NO_ACTION : EditAction.MODIFY);
	    		updateParentEditAction();
	    		m_treeNodeMenu.updateMenuItems();
	    	}
	    	
	    	for (TreeNodeEdit edit : getAllChildren()) {
	        	edit.setDeletion(toDelete);
	    	}
		}
	}

	/**
	 * Get the child with the specified class and name.
	 * 
	 * @param editClass the class of the child
	 * @param name the name of the child
	 * @return the child
	 */
	private TreeNodeEdit getChildOfClass(Class<? extends TreeNodeEdit> editClass, String name) {
		for (TreeNodeEdit edit : getAllChildren()) {
			if (editClass == edit.getClass() && name.equals(edit.getName())) {
				return edit;
			}
		}
		return null;
	}
	
	/**
	 * Get the child with the specified class and name.
	 * 
	 * @param editClass the class of the child
	 * @param inputPathFromFileWithName the input path from file of the child
	 * @return the child
	 */
	private TreeNodeEdit getChildOfClassByInputPath(Class<? extends TreeNodeEdit> editClass, String inputPathFromFileWithName) {
		if (this instanceof GroupNodeEdit) {
			if (editClass == GroupNodeEdit.class) {
				return ((GroupNodeEdit) this).getGroupNodeEdit(inputPathFromFileWithName);
			} else if (editClass == DataSetNodeEdit.class) {
				return ((GroupNodeEdit) this).getDataSetNodeEdit(inputPathFromFileWithName);
			} else if (editClass == AttributeNodeEdit.class) {
				return ((GroupNodeEdit) this).getAttributeNodeEdit(inputPathFromFileWithName, EditAction.NO_ACTION);
			}
		} else if (this instanceof DataSetNodeEdit) {
			if (editClass == AttributeNodeEdit.class) {
				return ((DataSetNodeEdit) this).getAttributeNodeEdit(inputPathFromFileWithName, EditAction.NO_ACTION);
			}
		}
		
		return null;
	}
	
	/**
	 * Get the names of the children that might have a name conflict to an edit
	 * of the class {@code editClass}.
	 * 
	 * @param editClass the class to be checked
	 * @return the names of the children that might have a name conflict
	 * @see TreeNodeEdit#isNameConflictPossible(Class)
	 */
	private List<String> getNamesOfChildrenWithNameConflictPossible(Class<? extends TreeNodeEdit> editClass) {
		List<String> usedNames = new ArrayList<>();
		
		for (TreeNodeEdit child : getAllChildren()) {
			if (child.isNameConflictPossible(editClass)) {
				usedNames.add(child.getName());
			}
		}
		
		return usedNames;
	}
	
	/**
	 * Checks if a name conflict between this edit and any edit with the class
	 * {@code otherClass} is possible.
	 * 
	 * @param otherClass an other class of edits
	 * @return if a name conflict between this edit and {@code otherClass}
	 * 	is possible
	 */
	private boolean isNameConflictPossible(Class<? extends TreeNodeEdit> otherClass) {
		Class<? extends TreeNodeEdit> thisClass = getClass();
		return thisClass == otherClass
				|| (thisClass == DataSetNodeEdit.class || thisClass == GroupNodeEdit.class || thisClass == UnsupportedObjectNodeEdit.class)
						&& (otherClass == DataSetNodeEdit.class || otherClass == GroupNodeEdit.class || otherClass == UnsupportedObjectNodeEdit.class);
	}
	
	/**
	 * @return all descendants of this edit (including this edit)
	 */
	protected List<TreeNodeEdit> getAllDecendants() {
		List<TreeNodeEdit> descendants = new ArrayList<>();
		
		descendants.add(this);
		for (TreeNodeEdit child : getAllChildren()) {
			descendants.addAll(child.getAllDecendants());
		}
		
		return descendants;
	}
	
	/**
	 * @return all children of this edit
	 */
	protected abstract TreeNodeEdit[] getAllChildren();
	
	/**
	 * Removes all descendants (including this edit) from their parents.
	 */
	protected void removeFromParentCascade() {
		for (TreeNodeEdit edit : getAllChildren()) {
			edit.removeFromParentCascade();
		}
		
		removeFromParent();
	}
	
	/**
	 * Removes this edit from its parent.
	 */
	protected abstract void removeFromParent();
	
	/**
	 * Writes the properties of this edit into {@code settings}.
	 * 
	 * @param settings the settings to be written to
	 */
	protected void saveSettingsTo(NodeSettingsWO settings) {
		settings.addString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey(), m_inputPathFromFileWithName);
		settings.addString(SettingsKey.NAME.getKey(), m_name);
		settings.addString(SettingsKey.EDIT_OVERWRITE_POLICY.getKey(), m_editOverwritePolicy.getName());
		settings.addString(SettingsKey.EDIT_ACTION.getKey(), m_editAction.getActionName());
	}

	/**
	 * Loads the properties of this edit from {@code settings}.
	 * 
	 * @param settings the settings to be load from
	 * @throws InvalidSettingsException if a required property does not exist
	 */
	protected abstract void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException;

	/**
	 * Creates (if none exists) and adds the tree node of this edit to the
	 * parent tree node in the {@linkplain JTree} if possible. Otherwise,
	 * this method does nothing.
	 * 
	 * @return if the tree node of this edit was added successfully
	 */
	@SuppressWarnings("unchecked")
	public boolean addEditToParentNodeIfPossible() {
		DefaultMutableTreeNode parentNode = getParent().getTreeNode();
		if (parentNode != null && !parentNode.isNodeChild(m_treeNode)) {
			if (m_treeNode == null) {
				m_treeNode = new DefaultMutableTreeNode(this);
			}
			
			Enumeration<DefaultMutableTreeNode> enumeration = parentNode.children();
			DefaultMutableTreeNode[] siblings = Collections.list(enumeration).toArray(new DefaultMutableTreeNode[0]);
			int insert;
			for (insert = 0; insert < siblings.length; insert++) {
				TreeNodeEdit edit = (TreeNodeEdit) siblings[insert].getUserObject();
				if (TreeNodeEditComparator.compareProperties(this, edit) < 0) {
					break;
				}
			}
			parentNode.insert(m_treeNode, insert);
			
			for (TreeNodeEdit edit : getAllChildren()) {
		        edit.addEditToParentNodeIfPossible();
			}
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * Uses the overwrite policy of {@code newEdit} to add it in this edit,
	 * but does not actually add it. Also uses the overwrite policy for columns
	 * if this is a dataSet edit.
	 * <br>
	 * Only does the changes on children of this edit and {@code newEdit}
	 * which are needed to do a successful {@code integrate(newEdit.getParent())}
	 * such that this edit will still be valid afterwards.
	 * 
	 * @param newEdit the new edit which should be added to this edit
	 */
	protected void useOverwritePolicy(TreeNodeEdit newEdit) {
		if (newEdit instanceof DataSetNodeEdit) {
			((DataSetNodeEdit) newEdit).useOverwritePolicyOfColumns();
		}
		
		TreeNodeEdit parentOfNewEdit = newEdit.getParent();
		TreeNodeEdit editToOverwrite = parentOfNewEdit.getChildOfClassByInputPath(newEdit.getClass(), newEdit.getOutputPathFromFileWithName(true));
		TreeNodeEdit oldEdit = getChildOfClass(newEdit.getClass(), newEdit.getName());
		if (editToOverwrite != null || oldEdit != null) {
			if (newEdit.isOverwriteApplicable()) {
				switch(newEdit.getEditOverwritePolicy()) {
				case IGNORE:
					newEdit.removeFromParent();
					break;
				case OVERWRITE:
					if (editToOverwrite != null) {
						editToOverwrite.setDeletion(true);
					} else {
						TreeNodeEdit deleteEdit = null;
						if (oldEdit instanceof GroupNodeEdit) {
							deleteEdit = ((GroupNodeEdit) oldEdit).copyGroupEditTo((GroupNodeEdit) parentOfNewEdit, false);
						} else if (oldEdit instanceof DataSetNodeEdit) {
							deleteEdit = ((DataSetNodeEdit) oldEdit).copyDataSetEditTo((GroupNodeEdit) parentOfNewEdit, false);
						} else if (oldEdit instanceof AttributeNodeEdit) {
							deleteEdit = ((AttributeNodeEdit) oldEdit).copyAttributeEditTo(parentOfNewEdit, false);
						}
						deleteEdit.setDeletion(true);
					}
					break;
				case RENAME:
					List<String> usedNames = getNamesOfChildrenWithNameConflictPossible(newEdit.getClass());
					usedNames.addAll(parentOfNewEdit.getNamesOfChildrenWithNameConflictPossible(newEdit.getClass()));
					newEdit.setName(Hdf5TreeElement.getUniqueName(usedNames, newEdit.getName()));
					break;
				case INTEGRATE:
					if (editToOverwrite == null || editToOverwrite.getEditAction() != EditAction.DELETE) {
						newEdit.removeFromParent();
						if (editToOverwrite != null) {
							editToOverwrite.removeFromParent();
						}
						
						TreeNodeEdit integrateEdit = null;
						if (oldEdit instanceof GroupNodeEdit) {
							integrateEdit = ((GroupNodeEdit) oldEdit).copyGroupEditTo((GroupNodeEdit) parentOfNewEdit, false);
							if (editToOverwrite != null) {
								((GroupNodeEdit) integrateEdit).integrate((GroupNodeEdit) editToOverwrite);
								integrateEdit.copyPropertiesFrom(editToOverwrite);
							}
							for (TreeNodeEdit edit : newEdit.getAllChildren()) {
								integrateEdit.useOverwritePolicy(edit);
							}
							((GroupNodeEdit) integrateEdit).integrate((GroupNodeEdit) newEdit);
							
						} else if (oldEdit instanceof DataSetNodeEdit) {
							integrateEdit = ((DataSetNodeEdit) oldEdit).copyDataSetEditTo((GroupNodeEdit) parentOfNewEdit, false);
							if (editToOverwrite != null) {
								((DataSetNodeEdit) integrateEdit).integrate((DataSetNodeEdit) editToOverwrite, true);
								integrateEdit.copyPropertiesFrom(editToOverwrite);
							}
							for (TreeNodeEdit edit : newEdit.getAllChildren()) {
								integrateEdit.useOverwritePolicy(edit);
							}
							((DataSetNodeEdit) integrateEdit).integrate((DataSetNodeEdit) newEdit, false);
						}
					}
					break;
				default:
					break;
				}
			} else if (oldEdit != null) {
				for (TreeNodeEdit edit : newEdit.getAllChildren()) {
					oldEdit.useOverwritePolicy(edit);
				}
			}
		}
	}
	
	/**
	 * Checks if the location of the hdfObject will be changed (but not deleted)
	 * by execution of this edit.
	 * 
	 * @return if the overwrite policy is applicable
	 */
	private boolean isOverwriteApplicable() {
		return m_editAction.isCreateOrCopyAction()
				|| m_editAction == EditAction.MODIFY && !getOutputPathFromFileWithName(true).equals(m_inputPathFromFileWithName);
	}
	
	/**
	 * Validates this edit internally and/or externally (based on if the validation
	 * result depends on the respective hdf file).
	 * 
	 * @param internalCheck if an internal check should be done
	 * @param externalCheck if an external check should be done
	 */
	protected void validate(boolean internalCheck, boolean externalCheck) {
		InvalidCause cause = null;
		
		if (externalCheck && cause == null) {
			if (m_editAction == EditAction.COPY) {
				cause = m_copyEdit == null ? InvalidCause.NO_COPY_SOURCE : null;
			} else if (m_editAction == EditAction.CREATE) {
				cause = this instanceof FileNodeEdit ? ((FileNodeEdit) this).validateFileCreation() :
						this instanceof ColumnNodeEdit ? ((ColumnNodeEdit) this).getInputInvalidCause() :
						this instanceof AttributeNodeEdit ? ((AttributeNodeEdit) this).getInputInvalidCause() : null;
			} else if (isSupported()) {
				cause = m_hdfObject == null ? InvalidCause.NO_HDF_SOURCE : this instanceof DataSetNodeEdit
						&& ((Hdf5DataSet<?>) m_hdfObject).numberOfColumns() != ((DataSetNodeEdit) this).getRequiredColumnCountForExecution()
								? InvalidCause.COLUMN_COUNT : null;
			}
		}
		
		if (externalCheck && m_parent != null) {
			cause = cause == null && m_parent.getEditAction() == EditAction.DELETE && m_editAction != EditAction.DELETE ? InvalidCause.PARENT_DELETE : cause;
			
			if (cause == null) {
				for (TreeNodeEdit edit : m_parent.getAllChildren()) {
					if (isInConflict(edit)) {
						cause = InvalidCause.NAME_DUPLICATE;
						break;
					}
				}
			}
		}
		
		cause = cause == null && internalCheck ? validateEditInternal() : cause;
		
		updateInvalidMap(cause);

		for (TreeNodeEdit edit : getAllChildren()) {
	        edit.validate(internalCheck, externalCheck);
		}
	}
	
	/**
	 * Checks if this edit is valid in all properties those validation is not
	 * dependent on the respective hdf file where this edit is executed.
	 * 
	 * @return the cause why this edit is invalid internally (or {@code null}
	 * 	if it is valid)
	 */
	protected abstract InvalidCause validateEditInternal();
	
	/**
	 * Checks if the properties of this edit and the other edit {@code edit}
	 * contradict.
	 * 
	 * @param edit the other edit
	 * @return if this edit is in conflict to the other edit
	 */
	protected abstract boolean isInConflict(TreeNodeEdit edit);
	
	/**
	 * Executes the edit defined by {@linkplain EditAction} and the properties
	 * of this edit. The edit actions of all descendants are also executed in
	 * this method. It also independently from each other sets the success of
	 * all executed edits.
	 * 
	 * @param inputTable the knime input table
	 * @param flowVariables the knime flow variables
	 * @param saveColumnProperties if the column properties of the input table
	 * 	should be stored as attributes in the respective dataSets in hdf
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @return if the execution of this edit and all descendants was successful
	 * @throws CanceledExecutionException if the user cancelled the node while
	 * 	executing this edit
	 * @throws IOException if an error occurred
	 */
	protected boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws CanceledExecutionException, IOException {
		exec.checkCanceled();
		
		setEditState(EditState.IN_PROGRESS);
		
		boolean successOfOther = true;

		try {
			switch (m_editAction) {
			case CREATE:
				createAction(flowVariables, exec, totalProgressToDo);
				break;
			case COPY:
				copyAction(exec, totalProgressToDo);
				break;
			case NO_ACTION:
			case MODIFY_CHILDREN_ONLY:
				m_editSuccess = EditSuccess.TRUE;
				break;
			default:
				break;
			}
			
			updateStateAndProgress(exec, totalProgressToDo);
			
			successOfOther &= doChildActionsInOrder(inputTable, flowVariables, saveColumnProperties, exec, totalProgressToDo);
			
			switch (m_editAction) {
			case DELETE:
				deleteAction();
				break;
			case MODIFY:
				modifyAction(exec, totalProgressToDo);
				break;
			default:
				break;
			}
		} finally {
			updateStateAndProgress(exec, totalProgressToDo);
		}
		
		return m_editSuccess.didNotFail() && successOfOther;
	}
	
	/**
	 * Executes the edit actions of all descendants. It also independently from
	 * each other sets the success of all executed edits.
	 * 
	 * @param inputTable the knime input table
	 * @param flowVariables the knime flow variables
	 * @param saveColumnProperties if the column properties of the input table
	 * 	should be stored as attributes in the respective dataSets in hdf
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @return if the execution of this edit and all descendants was successful
	 * @throws CanceledExecutionException if the user cancelled the node while
	 * 	executing this edit
	 * @throws IOException if an error occurred
	 */
	private boolean doChildActionsInOrder(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws CanceledExecutionException, IOException {
		List<TreeNodeEdit> deleteEdits = new ArrayList<>();
		List<TreeNodeEdit> otherEdits = new ArrayList<>();
		List<String> objectNames = new ArrayList<>();
		List<String> attributeNames = new ArrayList<>();
		
		TreeNodeEdit[] children = getAllChildren();
		for (TreeNodeEdit edit : children) {
			if (!(edit instanceof ColumnNodeEdit)) {
				if (edit.getEditAction() == EditAction.DELETE) {
					deleteEdits.add(edit);
					(edit instanceof AttributeNodeEdit ? attributeNames : objectNames).add(Hdf5TreeElement.getPathAndName(m_inputPathFromFileWithName)[1]);
				} else {
					otherEdits.add(edit);
					(edit instanceof AttributeNodeEdit ? attributeNames : objectNames).add(edit.getName());
				}
			}
		}
		
		for (TreeNodeEdit edit : children) {
			if (edit.isBackupNeeded(objectNames, attributeNames)) {
				edit.createBackup();
				if (edit.getEditAction() != EditAction.DELETE) {
					try {
						edit.deleteActionAndResetEditSuccess();
					} catch (Exception e) {
						edit.setEditSuccess(false);
					}
				}
			}
		}

		boolean success = true;
		for (TreeNodeEdit edit : deleteEdits) {
			success &= edit.doAction(null, null, false, exec, totalProgressToDo);
		}
		
		for (TreeNodeEdit edit : otherEdits) {
			success &= edit.doAction(inputTable, flowVariables, saveColumnProperties, exec, totalProgressToDo);
		}
		
		return success;
	}
	
	/**
	 * Checks if a backup of the hdf object is needed because it gets deleted or
	 * overwritten (based on the {@code objectNames} and {@code attributeNames}).
	 * 
	 * @param objectNames the object names that will be in the parent hdf
	 * 	object after executing this edit
	 * @param attributeNames the attribute names that will be in the parent hdf
	 * 	object after executing this edit
	 * @return if a backup of the hdf object is needed
	 */
	private boolean isBackupNeeded(List<String> objectNames, List<String> attributeNames) {
		boolean backupNeeded = false;
		
		if (m_editAction == EditAction.DELETE) {
			backupNeeded = true;
			
		} else if (m_editAction == EditAction.MODIFY) {
			backupNeeded = havePropertiesChanged(m_hdfObject);
			if (!backupNeeded) {
				String[] pathAndName = Hdf5TreeElement.getPathAndName(m_inputPathFromFileWithName);
				backupNeeded = getOutputPathFromFile().equals(pathAndName[0])
						&& (this instanceof AttributeNodeEdit ? attributeNames : objectNames).contains(pathAndName[1]);
			}
		}
		
		return backupNeeded;
	}
	
	/**
	 * Deletes the hdf object and resets the success of this edit.
	 * 
	 * @return if the hdf object was deleted successfully
	 * @throws IOException if the hdf object does not exist or an error in the
	 * 	hdf library occurred
	 */
	protected boolean deleteActionAndResetEditSuccess() throws IOException {
		deleteAction();
		boolean success = m_editSuccess == EditSuccess.TRUE;
		resetSuccess();
		
		return success;
	}

	/**
	 * Executes the CREATE action of this edit. It also sets the success of this edit.
	 * <br>
	 * <br>
	 * <b>Note:</b> When using this method outside of
	 * {@linkplain TreeNodeEdit#doAction(BufferedDataTable, Map, boolean, ExecutionContext, long)},
	 * reset the success of this edit afterwards!
	 * 
	 * @param flowVariables the knime flow variables
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @throws IOException if an error occurred
	 */
	protected abstract void createAction(Map<String, FlowVariable> flowVariables, ExecutionContext exec, long totalProgressToDo) throws IOException;
	
	/**
	 * Executes the COPY action of this edit. It also sets the success of this edit.
	 * <br>
	 * <br>
	 * <b>Note:</b> When using this method outside of
	 * {@linkplain TreeNodeEdit#doAction(BufferedDataTable, Map, boolean, ExecutionContext, long)},
	 * reset the success of this edit afterwards!
	 * 
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @throws IOException if an error occurred
	 */
	protected abstract void copyAction(ExecutionContext exec, long totalProgressToDo) throws IOException;

	/**
	 * Executes the DELETE action of this edit. It also sets the success of this edit.
	 * <br>
	 * <br>
	 * <b>Note:</b> When using this method outside of
	 * {@linkplain TreeNodeEdit#doAction(BufferedDataTable, Map, boolean, ExecutionContext, long)},
	 * reset the success of this edit afterwards!
	 * 
	 * @throws IOException if an error occurred
	 */
	protected abstract void deleteAction() throws IOException;

	/**
	 * Executes the MODIFY action of this edit. It also sets the success of this edit.
	 * <br>
	 * <br>
	 * <b>Note:</b> When using this method outside of
	 * {@linkplain TreeNodeEdit#doAction(BufferedDataTable, Map, boolean, ExecutionContext, long)},
	 * reset the success of this edit afterwards!
	 * 
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @throws IOException if an error occurred
	 */
	protected abstract void modifyAction(ExecutionContext exec, long totalProgressToDo) throws IOException;
	
	/**
	 * Executes the rollback action of this edit. This method is called if there
	 * was an error while executing the hdf writer and the edit has already made
	 * changes on the hdf object.
	 * 
	 * @return if the edit is reset successfully before execution
	 * @throws IOException if an error occurred
	 */
	protected boolean rollbackAction() throws IOException {
		resetSuccess();
		
		try {
			if (m_editState == EditState.ROLLBACK_NOTHING_TODO
					|| m_parent.getEditState() == EditState.ROLLBACK_SUCCESS
					&& (m_parent.getEditAction() == EditAction.DELETE || m_parent.getEditAction() == EditAction.MODIFY && m_parent.getHdfBackup() != null)) {
					// this edit's rollback has already been done successfully in the parent's rollback
				m_editSuccess = EditSuccess.TRUE;
				
			} else {
				switch (m_editAction) {
				case CREATE:
				case COPY:
					deleteAction();
					break;
				case MODIFY:
				case DELETE:
					try {
						String newName = Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1];
						if (this instanceof AttributeNodeEdit) {
							Hdf5Attribute<?> attribute = (Hdf5Attribute<?>) getHdfSource();
							setHdfObject(attribute.getParent().renameAttribute(attribute.getName(), newName));
						} else {
							Hdf5TreeElement treeElement = (Hdf5TreeElement) getHdfSource();
							setHdfObject(treeElement.getParent().moveObject(treeElement, newName));
						}
						/* 
						 * do not set m_hdfBackup to null here because '!=null' check is needed for children
						 * (at the beginning of this try-finally-block)
						 */
					} finally {
						m_editSuccess = EditSuccess.getSuccess(m_hdfObject != null);
					}
					break;
				default:
					m_editSuccess = EditSuccess.TRUE;
					break;
				}
			}
		} finally {
			if (m_editState != EditState.ROLLBACK_NOTHING_TODO) {
				setEditState(m_editSuccess == EditSuccess.TRUE ? EditState.ROLLBACK_SUCCESS : EditState.ROLLBACK_FAIL);
			}
		}
		
		EditSuccess success = m_editSuccess;
		resetSuccess();
		
		return success == EditSuccess.TRUE;
	}
	
	/**
	 * The Comparator class for {@linkplain TreeNodeEdit}s.
	 */
	private static class TreeNodeEditComparator implements Comparator<TreeNodeEdit> {
		
		@Override
		public int compare(TreeNodeEdit o1, TreeNodeEdit o2) {
			if (o1.getParent() == o2.getParent()) {
				return compareProperties(o1, o2);
				
			} else {
				TreeNodeEdit[] path1 = o1.getPath();
				TreeNodeEdit[] path2 = o2.getPath();
				int minLength = Math.min(path1.length, path2.length);
				int compare = 0;
				for (int i = 0; compare == 0 && i < minLength; i++) {
					compare = compareProperties(path1[i], path2[i]);
				}
				return compare == 0 ? Integer.compare(path1.length, path2.length) : compare;
			}
		}
		
		/**
		 * Compares the properties between two edits. It firstly compares the
		 * edit classes. If they are {@linkplain ColumnNodeEdit}s, they are
		 * checked for DELETE action. Edits, that are not deleted, get the
		 * smaller value to compare. Otherwise, it compares their names in
		 * a not case sensitive way. 
		 * 
		 * @param o1 the first edit
		 * @param o2 the second edit
		 * @return -1, 0 or 1 depending if the values v1,v2 of o1,o2 fulfill
		 * 	{@code v1 < v2}, {@code v1 == v2} or {@code v1 > v2}
		 */
		private static int compareProperties(TreeNodeEdit o1, TreeNodeEdit o2) {
			int compare = Integer.compare(getClassValue(o1), getClassValue(o2));
			if (o1 instanceof ColumnNodeEdit && o2 instanceof ColumnNodeEdit) {
				return (o1.getEditAction() == EditAction.DELETE) == (o2.getEditAction() == EditAction.DELETE) ? 0
						: (o1.getEditAction() == EditAction.DELETE ? 1 : -1);
			} else {
				return compare == 0 ? o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase()) : compare;
			}
		}
		
		/**
		 * Get the class value for the edit which is 0 for {@code GroupNodeEdit},
		 * 1 for {@code DataSetNodeEdit}, 2 for {@code ColumnNodeEdit},
		 * 4 for {@code AttributeNodeEdit} or 3 for other classes.
		 * 
		 * @param o the edit
		 * @return the class value
		 */
		private static int getClassValue(TreeNodeEdit o) {
			return o.getClass() == GroupNodeEdit.class ? 0 : 
				(o.getClass() == DataSetNodeEdit.class ? 1 :
				(o.getClass() == ColumnNodeEdit.class ? 2 :
				(o.getClass() == AttributeNodeEdit.class ? 4 : 3)));
		}
	}
	
	/**
	 * The class for the {@linkplain JPopupMenu} which can be accessed through
	 * a right mouse click on the edit.
	 */
	public abstract class TreeNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = 4973286624577483071L;

    	private PropertiesDialog m_propertiesDialog;

    	private JMenuItem m_itemEdit;
    	
    	private JMenuItem m_itemCreateGroup;
    	
    	private JMenuItem m_itemDelete;
		
    	/**
    	 * Creates the {@linkplain TreeNodeMenu} with the menu items those
    	 * options are set to {@code true}.
    	 * 
    	 * @param editable if this edit's properties may be changed
    	 * @param groupCreatable if this edit may contain groups as children
    	 * @param deletable if this edit may be deleted
    	 */
		protected TreeNodeMenu(boolean editable, boolean groupCreatable, boolean deletable) {
			if (editable) {
				m_itemEdit = new JMenuItem("Edit properties");
				m_itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (m_editAction != EditAction.DELETE) {
							if (m_propertiesDialog == null) {
								m_propertiesDialog = getPropertiesDialog();
							}
							
							m_propertiesDialog.loadFromEdit();
							m_propertiesDialog.setLocationRelativeTo((Frame) SwingUtilities.getAncestorOfClass(Frame.class, getInvoker()));
							m_propertiesDialog.setVisible(true);
						}
					}
				});
	    		add(m_itemEdit);
			}
			
    		if (groupCreatable) {
    			m_itemCreateGroup = new JMenuItem("Create new group");
    			m_itemCreateGroup.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						onCreateGroup();
					}
				});
	    		add(m_itemCreateGroup);
    		}

    		if (deletable) {
    			m_itemDelete = new JMenuItem();
    			updateMenuItems();
    			m_itemDelete.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						int dialogResult = JOptionPane.showConfirmDialog(TreeNodeMenu.this,
								getDeleteConfirmMessage(), "Warning", JOptionPane.YES_NO_OPTION);
						if (dialogResult == JOptionPane.YES_OPTION){
							onDelete();
						}
					}
				});
				add(m_itemDelete);
    		}
    	}
		
		/**
		 * Updates the menu items which are dependent on if the edit action
		 * is DELETE.
		 */
		private void updateMenuItems() {
			if (m_itemEdit != null) {
				m_itemEdit.setEnabled(m_editAction != EditAction.DELETE);
			}
			if (m_itemCreateGroup != null) {
				m_itemCreateGroup.setEnabled(m_editAction != EditAction.DELETE);
			}
			if (m_itemDelete != null) {
				m_itemDelete.setText(m_editAction != EditAction.DELETE ? "Delete" : "Remove deletion");
			}
		}
		
		/**
		 * Get the confirmation message when changing if this edit should be
		 * deleted or not.
		 * 
		 * @return the confirmation message for deletion
		 */
		private String getDeleteConfirmMessage() {
			return "Are you sure to "
					+ (m_editAction != EditAction.DELETE ? "delete" : "remove the deletion of")
					+ " this object and all its descendants?";
		}

		/**
		 * To implement in subclasses if needed.
		 * 
		 * @return {@code null}
		 */
		protected PropertiesDialog getPropertiesDialog() {
			return null;
		}
		
		/**
		 * To implement in subclasses if needed.
		 */
		protected void onCreateGroup() {
		}
		
		/**
		 * Defines what happens if the user clicks on the delete option of
		 * the menu.
		 */
		protected abstract void onDelete();
	}
	
	/**
	 * Abstract class for a {@linkplain JDialog} to set all properties of the
	 * edit.
	 */
	protected static abstract class PropertiesDialog extends JDialog {

		private static final long serialVersionUID = -2868431511358917946L;
		
		private final JPanel m_contentPanel = new JPanel();
		
		private final GridBagConstraints m_constraints = new GridBagConstraints();

		/**
		 * Creates a new {@code PropertiesDialog} with the input title where
		 * its owner is the invoker of {@code menu}.
		 * 
		 * @param menu the menu which accesses this dialog
		 * @param title the title of this dialog
		 */
		protected PropertiesDialog(TreeNodeMenu menu, String title) {
			super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, menu.getInvoker()), title, true);
			setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
			// setLocation(400, 400);
			
			JPanel panel = new JPanel(new BorderLayout());
			add(panel, BorderLayout.CENTER);
			panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

			panel.add(m_contentPanel, BorderLayout.CENTER);
			m_contentPanel.setLayout(new GridBagLayout());
			m_constraints.fill = GridBagConstraints.BOTH;
			m_constraints.insets = new Insets(2, 2, 2, 2);
			m_constraints.gridy = 0;
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
			panel.add(buttonPanel, BorderLayout.PAGE_END);
			JButton okButton = new JButton("OK");
			okButton.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					saveToEdit();
					setVisible(false);
				}
			});
	        getRootPane().setDefaultButton(okButton);
	        okButton.setMnemonic(KeyEvent.VK_ENTER);
			buttonPanel.add(okButton);
			
			JButton cancelButton = new JButton("Cancel");
			cancelButton.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					setVisible(false);
				}
			});
			buttonPanel.add(cancelButton);
		}

		/**
		 * Adds a new (maybe optional) property as a new row in the GridBagLayout.
		 * 
		 * @param description the description of the new property
		 * @param component the component of the new property
		 * @param checkBoxListener used if the property is optional
		 * 	(or {@code null} if the property is not optional)
		 * @param weighty specifies how much extra y-space the {@code component} gets
		 * @return the {@linkplain JCheckBox} created by the {@code checkBoxListener}
		 * 	(or {@code null} if the property is not optional)
		 */
		protected JCheckBox addProperty(String description, JComponent component, ChangeListener checkBoxListener, double weighty) {
			PropertyDescriptionPanel propertyPanel = new PropertyDescriptionPanel(description,
					checkBoxListener, Double.compare(weighty, 0.0) != 0);
			m_constraints.gridx = 0;
            m_constraints.weightx = 0.0;
            m_constraints.weighty = weighty;
			m_contentPanel.add(propertyPanel, m_constraints);
            m_constraints.gridx++;
            m_constraints.weightx = 1.0;
            m_contentPanel.add(component, m_constraints);
			m_constraints.gridy++;
			
			return propertyPanel.getCheckBox();
		}

		/**
		 * Adds a new (maybe optional) property as a new row in the GridBagLayout
		 * where its component gets no extra space in y-direction.
		 * 
		 * @param description the description of the new property
		 * @param component the component of the new property
		 * @param checkBoxListener used if the property is optional
		 * 	(or {@code null} if the property is not optional)
		 * @return the {@linkplain JCheckBox} created by the {@code checkBoxListener}
		 * 	(or {@code null} if the property is not optional)
		 */
		protected JCheckBox addProperty(String description, JComponent component, ChangeListener checkBoxListener) {
	        return addProperty(description, component, checkBoxListener, 0.0);
		}

		/**
		 * Adds a new property as a new row in the GridBagLayout.
		 * 
		 * @param description the description of the new property
		 * @param component the component of the new property
		 * @param weighty specifies how much extra y-space the {@code component} gets
		 */
		protected void addProperty(String description, JComponent component, double weighty) {
			addProperty(description, component, null, weighty);
		}

		/**
		 * Adds a new property as a new row in the GridBagLayout
		 * where its component gets no extra space in y-direction.
		 * 
		 * @param description the description of the new property
		 * @param component the component of the new property
		 */
		protected void addProperty(String description, JComponent component) {
			addProperty(description, component, null, 0.0);
		}
		
		/**
		 * The class for a {@linkplain JPanel} which contains a description and
		 * optionally a {@linkplain JCheckBox} which enables/disables the property.
		 */
		protected class PropertyDescriptionPanel extends JPanel {

			private static final long serialVersionUID = 3019076429508416644L;
			
			private JCheckBox m_checkBox;
			
			private PropertyDescriptionPanel(String description, ChangeListener checkBoxListener, boolean northwest) {
				setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
				
				JComponent descriptionLabel = null;
				if (checkBoxListener != null) {
					descriptionLabel = new JCheckBox(description);
					m_checkBox = (JCheckBox) descriptionLabel;
					m_checkBox.addChangeListener(checkBoxListener);

				} else {
					descriptionLabel = new JLabel(description);
				}
				
				add(descriptionLabel);
				if (northwest) {
					descriptionLabel.setAlignmentY(0.0f);
				}
			}
			
			private JCheckBox getCheckBox() {
				return m_checkBox;
			}
		}
		
		/**
		 * Loads the properties from this edit into the dialog.
		 */
		protected abstract void loadFromEdit();
		
		/**
		 * Saves the models in the dialog to the properties in this edit.
		 */
		protected abstract void saveToEdit();
	}
}

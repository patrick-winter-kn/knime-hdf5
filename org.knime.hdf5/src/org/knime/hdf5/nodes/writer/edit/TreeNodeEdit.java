package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5TreeElement;

public abstract class TreeNodeEdit {

	protected static enum SettingsKey {
		NAME("name"),
		INPUT_PATH_FROM_FILE_WITH_NAME("inputPathFromFileWithName"),
		EDIT_OVERWRITE_POLICY("editOverwritePolicy"),
		EDIT_ACTION("editAction"),
		FILE_PATH("filePath"),
		INPUT_TYPE("inputType"),
		POSSIBLE_OUTPUT_TYPES("possibleOutputTypes"),
		OUTPUT_TYPE("outputType"),
		LITTLE_ENDIAN("littleEndian"),
		ROUNDING("rounding"),
		FIXED("fixed"),
		STRING_LENGTH("stringLength"),
		STANDARD_VALUE("standardValue"),
		TOTAL_STRING_LENGTH("totalStringLength"),
		ITEM_STRING_LENGTH("itemStringLength"),
		FLOW_VARIABLE_ARRAY_POSSIBLE("flowVariableArrayPossible"),
		FLOW_VARIABLE_ARRAY_USED("flowVariableArrayUsed"),
		NUMBER_OF_DIMENSIONS("numberOfDimensions"),
		COMPRESSION("compression"),
		CHUNK_ROW_SIZE("chunkRowSize"),
		INPUT_ROW_COUNT("inputRowCount"),
		INPUT_COLUMN_INDEX("inputColumnIndex"),
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
	    
		public boolean isModifyAction() {
			return this == MODIFY || this == MODIFY_CHILDREN_ONLY;
		}
	}

	public static enum InvalidCause {
		FILE_EXTENSION("file extension is not .h5 or .hdf5"),
		FILE_ALREADY_EXISTS("file already exists"),
		NO_DIR_FOR_FILE("no directory for file exists"),
		NAME_DUPLICATE("name duplicate"),
		NO_HDF_SOURCE("no hdf source available"),
		NO_COPY_SOURCE("no source available to copy from"),
		PARENT_DELETE("parent is getting deleted so this also has to be deleted"),
		NAME_CHARS("name contains invalid characters"),
		NAME_BACKUP_PREFIX("cannot change to name with prefix \"" + BACKUP_PREFIX + "\""),
		ROW_COUNT("dataSet has unequal row sizes"),
		COLUMN_INDEX("no such column index exists in the dataSet source"),
		DATA_TYPE("some values do not fit into data type"),
		MISSING_VALUES("there are some missing values");

		private String m_message;

		private InvalidCause(String message) {
			m_message = message;
		}
		
		protected String getMessage() {
			return m_message;
		}
	}
	
	public static enum EditState {
		TODO, IN_PROGRESS, SUCCESS, FAIL;
	}
	
	static final String BACKUP_PREFIX = "temp_";
	
	private String m_inputPathFromFileWithName;
	
	private String m_outputPathFromFile;
	
	private String m_name;
	
	private EditOverwritePolicy m_editOverwritePolicy;
	
	private boolean m_overwrite;

	private TreeNodeMenu m_treeNodeMenu;
	
	protected DefaultMutableTreeNode m_treeNode;
	
	private TreeNodeEdit m_parent;

	private Object m_hdfObject;
	
	private Object m_hdfBackup;
	
	private EditAction m_editAction = EditAction.NO_ACTION;
	
	private List<TreeNodeEdit> m_incompleteCopies = new ArrayList<>();
	
	private TreeNodeEdit m_copyEdit;
	
	private Map<TreeNodeEdit, InvalidCause> m_invalidEdits = new HashMap<>();
	
	private String m_unsupportedCause = null;
	
	private EditState m_editState = EditState.TODO;

	TreeNodeEdit(String inputPathFromFileWithName, String outputPathFromFile, String name, EditOverwritePolicy editOverwritePolicy, EditAction editAction) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
		m_outputPathFromFile = outputPathFromFile;
		m_name = name;
		m_editOverwritePolicy = editOverwritePolicy;
		setEditAction(editAction);
	}
	
	public static String getUniqueName(TreeNodeEdit parent, Class<? extends TreeNodeEdit> editClass, String name) {
		return Hdf5TreeElement.getUniqueName(parent.getNamesOfChildrenWithNameConflictPossible(editClass), name);
	}
	
	public String getInputPathFromFileWithName() {
		return m_inputPathFromFileWithName;
	}
	
	void setInputPathFromFileWithName(String inputPathFromFileWithName) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
		for (TreeNodeEdit copyEdit : m_incompleteCopies) {
			copyEdit.setInputPathFromFileWithName(inputPathFromFileWithName);
		}
	}
	
	public String getOutputPathFromFile() {
		return m_outputPathFromFile;
	}

	public String getName() {
		return m_name;
	}
	
	void setName(String name) {
		m_name = name;
	}
	
	EditOverwritePolicy getEditOverwritePolicy() {
		return m_editOverwritePolicy;
	}

	protected void setEditOverwritePolicy(EditOverwritePolicy editOverwritePolicy) {
		m_editOverwritePolicy = editOverwritePolicy;
	}

	public boolean isOverwrite() {
		return m_overwrite;
	}
	/*
	private void setOverwrite(boolean overwrite) {
		m_overwrite = overwrite;
	}
	*/
	public TreeNodeMenu getTreeNodeMenu() {
		return m_treeNodeMenu;
	}
	
	protected void setTreeNodeMenu(TreeNodeMenu treeNodeMenu) {
		m_treeNodeMenu = treeNodeMenu;
		if (m_treeNodeMenu != null) {
			m_treeNodeMenu.updateDeleteItemText();
		}
	}
	
	public DefaultMutableTreeNode getTreeNode() {
		return m_treeNode;
	}

	public TreeNodeEdit getParent() {
		return m_parent;
	}
	
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

	public Object getHdfObject() {
		return m_hdfObject;
	}

	public void setHdfObject(Hdf5TreeElement hdfObject) {
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

	public EditAction getEditAction() {
		return m_editAction;
	}
	
	public void setEditAction(EditAction editAction) {
		if (!editAction.isModifyAction() || m_editAction == EditAction.NO_ACTION || m_editAction == EditAction.MODIFY_CHILDREN_ONLY) {
			m_editAction = editAction;
			
			if (m_parent != null && m_parent.getEditAction() == EditAction.NO_ACTION && m_editAction != EditAction.NO_ACTION) {
				m_parent.setEditAction(EditAction.MODIFY_CHILDREN_ONLY);
			}
		}
	}

	protected TreeNodeEdit getCopyEdit() {
		return m_copyEdit;
	}
	
	private void setCopyEdit(TreeNodeEdit copyEdit) {
		m_copyEdit = copyEdit;
	}

	public boolean isSupported() {
		return m_unsupportedCause == null;
	}
	
	void setUnsupportedCause(String unsupportedCause) {
		m_unsupportedCause = unsupportedCause;
	}

	public EditState getEditState() {
		return m_editState;
	}
	
	private void setEditState(EditState editState) {
		m_editState = editState;
	}
	
	protected abstract boolean havePropertiesChanged();
	
	protected void copyPropertiesFrom(TreeNodeEdit copyEdit) {
		copyCorePropertiesFrom(copyEdit);
		copyAdditionalPropertiesFrom(copyEdit);
	}
	
	private void copyCorePropertiesFrom(TreeNodeEdit copyEdit) {
		m_inputPathFromFileWithName = copyEdit.getInputPathFromFileWithName();
		m_outputPathFromFile = copyEdit.getOutputPathFromFile();
		m_name = copyEdit.getName();
		m_editOverwritePolicy = copyEdit.getEditOverwritePolicy();
		setEditAction(copyEdit.getEditAction());
		m_treeNodeMenu.updateDeleteItemText();
		if (m_editAction == EditAction.COPY) {
			m_parent.addIncompleteCopy(this);
		}
	}
	
	protected abstract void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit);

	protected boolean avoidsOverwritePolicyNameConflict(TreeNodeEdit edit) {
		if (getClass() == edit.getClass()) {
			boolean wasHereBefore = getOutputPathFromFileWithName(true).equals(getInputPathFromFileWithName());
			if (wasHereBefore || edit.getOutputPathFromFileWithName(true).equals(edit.getInputPathFromFileWithName())) {
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
				} else {
					return policy != EditOverwritePolicy.NONE;
				}
			}
		}
		return false;
	}
	
	protected boolean willModifyActionBeAbortedOnEdit(TreeNodeEdit edit) {
		if (edit.getEditAction() == EditAction.MODIFY && edit.getEditOverwritePolicy() == EditOverwritePolicy.ABORT
				&& getName().equals(Hdf5TreeElement.getPathAndName(edit.getInputPathFromFileWithName())[1])) {
			List<TreeNodeEdit> conflictEdits = new ArrayList<>();
			TreeNodeEdit conflictEdit = edit;
			do {
				conflictEdits.add(conflictEdit);
				conflictEdit = conflictEdit.willBeAbortedOnEdit();
			} while (conflictEdit != null && !conflictEdits.contains(conflictEdit)
					&& conflictEdit.getEditAction() == EditAction.MODIFY && conflictEdit.getEditOverwritePolicy() == EditOverwritePolicy.ABORT);
			
			return conflictEdit != null;
		}
		
		return false;
	}
	
	private TreeNodeEdit willBeAbortedOnEdit() {
		for (TreeNodeEdit edit : m_parent.getAllChildren()) {
			if (getName().equals(Hdf5TreeElement.getPathAndName(edit.getInputPathFromFileWithName())[1])
					&& edit.getEditAction() == EditAction.MODIFY && edit.getEditOverwritePolicy() == EditOverwritePolicy.ABORT
					|| getName().equals(edit.getName()) && this != edit && edit.getEditAction() != EditAction.DELETE) {
				return edit;
			}
		}
		
		return null;
	}
	
	protected Hdf5TreeElement getOpenedHdfObjectOfParent() {
		Hdf5TreeElement parent = (Hdf5TreeElement) getParent().getHdfObject();
		
		if (parent != null && !parent.isFile()) {
			parent.open();
		}
		
		return parent;
	}
	
	Object getHdfSource() {
		return m_hdfObject != null ? m_hdfObject : m_hdfBackup;
	}
	
	Object findCopySource() {
		FileNodeEdit fileEdit = getRoot();
		if (this instanceof GroupNodeEdit) {
			try {
				return ((Hdf5File) fileEdit.getHdfObject()).getGroupByPath(m_inputPathFromFileWithName);
			} catch (IOException ioe) {
				return fileEdit.getGroupEditByPath(m_inputPathFromFileWithName).getHdfBackup();
			}
		} else if (this instanceof DataSetNodeEdit || this instanceof ColumnNodeEdit) {
			try {
				return ((Hdf5File) fileEdit.getHdfObject()).getDataSetByPath(m_inputPathFromFileWithName);
			} catch (IOException ioe) {
				return fileEdit.getDataSetEditByPath(m_inputPathFromFileWithName).getHdfBackup();
			}
		} else if (this instanceof AttributeNodeEdit) {
			try {
				return ((Hdf5File) fileEdit.getHdfObject()).getAttributeByPath(m_inputPathFromFileWithName);
			} catch (IOException ioe) {
				return fileEdit.getAttributeEditByPath(m_inputPathFromFileWithName).getHdfBackup();
			}
		}
		
		return null;
	}
	
	boolean createBackup() {
		try {
			if (this instanceof AttributeNodeEdit) {
				setHdfBackup(((Hdf5Attribute<?>) m_hdfObject).createBackup(BACKUP_PREFIX));
			} else {
				setHdfBackup(((Hdf5TreeElement) m_hdfObject).createBackup(BACKUP_PREFIX));
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).warn("Backup could not be created: " + ioe.getMessage(), ioe);
		}
		
		return m_hdfBackup != null;
	}
	
	boolean deleteBackup() {
		boolean success = true;
		
		try {
			if (m_hdfBackup != null) {
				if (m_hdfBackup instanceof Hdf5TreeElement) {
					Hdf5TreeElement treeElement = (Hdf5TreeElement) m_hdfBackup;
					success = treeElement.isValid() ? treeElement.getParent().deleteObject(treeElement.getName()) >= 0 : true;
				} else if (m_hdfBackup instanceof Hdf5Attribute<?>) {
					Hdf5Attribute<?> attribute = (Hdf5Attribute<?>) m_hdfBackup;
					success = attribute.isValid() ? attribute.getParent().deleteAttribute(attribute.getName()) >= 0 : true;
				}
			}
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).warn("Backup could not be deleted: " + ioe.getMessage(), ioe);
			success = false;
		}
		
		return success;
	}
	
	protected void addIncompleteCopy(TreeNodeEdit edit) {
		if (getRoot() == edit.getRoot()) {
			if (m_copyEdit == null) {
				if (edit.getCopyEdit() != null) {
					edit.getCopyEdit().removeIncompleteCopy(edit);
				}
				m_incompleteCopies.add(edit);
				edit.setCopyEdit(this);
				
			} else {
				m_copyEdit.addIncompleteCopy(edit);
			}
		}
	}
	
	protected void removeIncompleteCopy(TreeNodeEdit edit) {
		m_incompleteCopies.remove(edit);
		edit.setCopyEdit(null);
		if (edit instanceof DataSetNodeEdit) {
			for (ColumnNodeEdit columnEdit : ((DataSetNodeEdit) edit).getColumnNodeEdits()) {
				removeIncompleteCopy(columnEdit);
			}
		}
	}
	
	protected boolean areIncompleteCopiesLeft() {
		return !m_incompleteCopies.isEmpty();
	}
	
	protected void updateIncompleteCopies() {
    	if (m_editAction == EditAction.COPY && m_copyEdit == null) {
    		TreeNodeEdit copyEdit = findCopyEdit();
			if (copyEdit != null) {
				copyEdit.addIncompleteCopy(this);
			}
		}
    	
		for (TreeNodeEdit edit : getAllChildren()) {
			edit.updateIncompleteCopies();
		}
	}
	
	public boolean isValid() {
		return m_invalidEdits.isEmpty();
	}
	
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
	
	protected TreeNodeEdit[] getInvalidsWithCause(InvalidCause cause) {
		List<TreeNodeEdit> edits = new ArrayList<>();
		for (TreeNodeEdit edit : m_invalidEdits.keySet()) {
			if (cause == m_invalidEdits.get(edit)) {
				edits.add(edit);
			}
		}
		return edits.toArray(new TreeNodeEdit[edits.size()]);
	}
	
	/**
	 * Get the list of messages of all invalid descendants of this edit and {@code otherEdit}.
	 * If this edit and {@code otherEdit}) have the same invalid descendant, the message of this edit will be used.
	 * 
	 * @param otherEdit
	 * @return
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
			for (TreeNodeEdit invalidEdit : invalidEdits) {
				if (invalidCause == invalidEditCauseMap.get(invalidEdit)) {
					if (!invalidEdit.getOutputPathFromFileWithName().equals(curInvalidPath)) {
						found++;
						curInvalidPath = invalidEdit.getOutputPathFromFileWithName();
						if (found <= maxMessageCountPerCause) {
							messageBuilder.append(found == 1 ? invalidCause.getMessage() + ":" : "");
							messageBuilder.append(count > 1 ? " (" + count + ")" : "");
							messageBuilder.append("\n- " + curInvalidPath);
							count = 1;
						}
					} else if (found <= maxMessageCountPerCause) {
						count++;
					}
				}
			}
			messageBuilder.append(count > 1 ? " (" + count + ")" : "");
			messageBuilder.append(found > maxMessageCountPerCause ? "\n- and "
					+ (found - maxMessageCountPerCause) + " more\n" : found > 0 ? "\n" : "");
		}
		
		return messageBuilder.toString();
	}
	
	public String getToolTipText() {
		return getOutputPathFromFileWithName()
				+ (isValid() ? "" : " (invalid" + (m_invalidEdits.containsKey(this) ? " - " + m_invalidEdits.get(this).getMessage() : " children") + ")")
				+ (isSupported() ? "" : " - NOT SUPPORTED (" + m_unsupportedCause + ")");
	}
	
	private void setSuccess(boolean success, ExecutionContext exec, long totalProgressToDo) {
		if (m_editState == EditState.IN_PROGRESS) {
			if (success) {
				if (!(this instanceof DataSetNodeEdit && m_editAction == EditAction.CREATE)) {
					addProgress(getProgressToDoInEdit(), exec, totalProgressToDo);
				}
				setEditState(EditState.SUCCESS);
				
			} else {
				setEditState(EditState.FAIL);
			}
		}
	}
	
	protected void addProgress(long progressToAdd, ExecutionContext exec, long totalProgressToDo) {
		// TODO change after testing
		long progressDone = Math.round(exec.getProgressMonitor().getProgress() * totalProgressToDo) + progressToAdd;
		exec.setProgress((double) progressDone / totalProgressToDo);
		System.out.println("Progress done (Edit " + getOutputPathFromFileWithName() + "): " + progressDone);
	}
	
	public String getSummaryOfEditStates() {
		StringBuilder summaryBuilder = new StringBuilder();
		
		List<TreeNodeEdit> descendants = getAllDecendants();
		for (EditState state : EditState.values()) {
			summaryBuilder.append("State " + state + ":\n");
			for (TreeNodeEdit descendant : descendants.toArray(new TreeNodeEdit[descendants.size()])) {
				if (state == descendant.getEditState()) {
					if (!(descendant instanceof ColumnNodeEdit)) {
						summaryBuilder.append("- " + descendant.getOutputPathFromFileWithName() + " (" + descendant.getEditAction() + ")\n");
					}
					descendants.remove(descendant);
				}
			}
		}
		
		return summaryBuilder.toString();
	}
	
	protected abstract long getProgressToDoInEdit();
	
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

	public String getOutputPathFromFileWithName() {
		return getOutputPathFromFileWithName(false);
	}
	
	String getOutputPathFromFileWithName(boolean checkSlashesInName) {
		return (!m_outputPathFromFile.isEmpty() ? m_outputPathFromFile + "/" : "") + (checkSlashesInName ? m_name.replaceAll("/", "\\\\/") : m_name);
	}

	public void reloadTreeWithEditVisible() {
		reloadTreeWithEditVisible(false);
	}
	
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
	
	protected FileNodeEdit getRoot() {
		return m_parent != null ? m_parent.getRoot() : (FileNodeEdit) this;
	}
	
	private TreeNodeEdit[] getPath() {
		List<TreeNodeEdit> editsInPath = new ArrayList<>();
		editsInPath.add(this);
		while (editsInPath.get(0).getParent() != null) {
			editsInPath.add(0, editsInPath.get(0).getParent());
		}
		return editsInPath.toArray(new TreeNodeEdit[editsInPath.size()]);
	}
	
	public boolean isEditDescendant(TreeNodeEdit other) {
		TreeNodeEdit[] thisPath = getPath();
		TreeNodeEdit[] otherPath = other.getPath();
		
		for (int i = 0; i < thisPath.length; i++) {
			if (i >= otherPath.length || thisPath[i] != otherPath[i]) {
				return false;
			}
		}
		
		return true;
	}
	
	private void updateParentEditAction() {
		setEditAction(getEditAction());
	}
	
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
	
	void setDeletion(boolean isDelete) {
    	if (isDelete && getEditAction().isCreateOrCopyAction() || !isDelete && m_invalidEdits.get(this) == InvalidCause.NO_HDF_SOURCE && getEditAction() == EditAction.DELETE) {
    		removeFromParent();
    		
    	} else {
    		m_editAction = isDelete ? EditAction.DELETE : EditAction.MODIFY;
    		updateParentEditAction();
    		m_treeNodeMenu.updateDeleteItemText();
    	}
    	
    	for (TreeNodeEdit edit : getAllChildren()) {
        	edit.setDeletion(isDelete);
    	}
	}

	private TreeNodeEdit getChildOfClass(Class<?> editClass, String name) {
		for (TreeNodeEdit edit : getAllChildren()) {
			if (editClass == edit.getClass() && name.equals(edit.getName())) {
				return edit;
			}
		}
		return null;
	}
	
	private TreeNodeEdit getChildOfClassByInputPath(Class<?> editClass, String inputPathFromFileWithName) {
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
	
	private List<String> getNamesOfChildrenWithNameConflictPossible(Class<? extends TreeNodeEdit> editClass) {
		List<String> usedNames = new ArrayList<>();
		
		for (TreeNodeEdit child : getAllChildren()) {
			if (child.isNameConflictPossible(editClass)) {
				usedNames.add(child.getName());
			}
		}
		
		return usedNames;
	}
	
	private boolean isNameConflictPossible(Class<? extends TreeNodeEdit> otherClass) {
		Class<? extends TreeNodeEdit> thisClass = getClass();
		return thisClass == otherClass
				|| (thisClass == DataSetNodeEdit.class || thisClass == GroupNodeEdit.class || thisClass == UnsupportedObjectNodeEdit.class)
						&& (otherClass == DataSetNodeEdit.class || otherClass == GroupNodeEdit.class || otherClass == UnsupportedObjectNodeEdit.class);
	}
	
	protected List<TreeNodeEdit> getAllDecendants() {
		List<TreeNodeEdit> descendants = new ArrayList<>();
		
		descendants.add(this);
		for (TreeNodeEdit child : getAllChildren()) {
			descendants.addAll(child.getAllDecendants());
		}
		
		return descendants;
	}
	
	protected abstract TreeNodeEdit[] getAllChildren();
	
	protected abstract void removeFromParent();
	
	protected void saveSettingsTo(NodeSettingsWO settings) {
		settings.addString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey(), m_inputPathFromFileWithName);
		settings.addString(SettingsKey.NAME.getKey(), m_name);
		settings.addInt(SettingsKey.EDIT_OVERWRITE_POLICY.getKey(), m_editOverwritePolicy.ordinal());
		settings.addString(SettingsKey.EDIT_ACTION.getKey(), m_editAction.getActionName());
	}

	protected abstract void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException;

	@SuppressWarnings("unchecked")
	public void addEditToParentNode() {
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
		        edit.addEditToParentNode();
			}
		}
	}
	
	/**
	 * @param newEdit	the new edit which should be added to this edit
	 */
	protected void useOverwritePolicy(TreeNodeEdit newEdit) {
		TreeNodeEdit parentOfNewEdit = newEdit.getParent();
		TreeNodeEdit editToOverwrite = parentOfNewEdit.getChildOfClassByInputPath(newEdit.getClass(), newEdit.getOutputPathFromFileWithName(true));
		TreeNodeEdit oldEdit = getChildOfClass(newEdit.getClass(), newEdit.getName());
		if (editToOverwrite != null || oldEdit != null) {
			if (newEdit.isOverwriteApplicable()) {
				switch(newEdit.getEditOverwritePolicy()) {
				case ABORT:
					newEdit.removeFromParent();
					break;
				case OVERWRITE:
					if (editToOverwrite != null) {
						editToOverwrite.setEditAction(EditAction.DELETE);
					} else {
						TreeNodeEdit deleteEdit = null;
						if (oldEdit instanceof GroupNodeEdit) {
							deleteEdit = ((GroupNodeEdit) oldEdit).copyGroupEditTo((GroupNodeEdit) parentOfNewEdit, false);
						} else if (oldEdit instanceof DataSetNodeEdit) {
							deleteEdit = ((DataSetNodeEdit) oldEdit).copyDataSetEditTo((GroupNodeEdit) parentOfNewEdit, false);
						} else if (oldEdit instanceof AttributeNodeEdit) {
							if (parentOfNewEdit instanceof GroupNodeEdit) {
								deleteEdit = ((AttributeNodeEdit) oldEdit).copyAttributeEditTo((GroupNodeEdit) parentOfNewEdit, false);
							} else if (parentOfNewEdit instanceof DataSetNodeEdit) {
								deleteEdit = ((AttributeNodeEdit) oldEdit).copyAttributeEditTo((DataSetNodeEdit) parentOfNewEdit, false);
							}
						}
						deleteEdit.setEditAction(EditAction.DELETE);
						// TODO do it without this line (because of copy...EditTo())
						deleteEdit.setCopyEdit(null);
					}
					
					//newEdit.setOverwrite(true);
					break;
				case RENAME:
					List<String> usedNames = getNamesOfChildrenWithNameConflictPossible(newEdit.getClass());
					usedNames.addAll(parentOfNewEdit.getNamesOfChildrenWithNameConflictPossible(newEdit.getClass()));
					newEdit.setName(Hdf5TreeElement.getUniqueName(usedNames, newEdit.getName()));
					break;
				case INTEGRATE:
					// TODO test it
					if (editToOverwrite == null || editToOverwrite.getEditAction() != EditAction.DELETE) {
						newEdit.removeFromParent();
						TreeNodeEdit integrateEdit = null;
						if (oldEdit instanceof GroupNodeEdit) {
							integrateEdit = editToOverwrite != null ? editToOverwrite
									: ((GroupNodeEdit) oldEdit).copyGroupEditTo((GroupNodeEdit) parentOfNewEdit, false);
							for (TreeNodeEdit edit : newEdit.getAllChildren()) {
								integrateEdit.useOverwritePolicy(edit);
							}
							((GroupNodeEdit) integrateEdit).integrate((GroupNodeEdit) newEdit, ColumnNodeEdit.UNKNOWN_ROW_COUNT, false);
						} else if (oldEdit instanceof DataSetNodeEdit) {
							integrateEdit = editToOverwrite != null ? editToOverwrite
									: ((DataSetNodeEdit) oldEdit).copyDataSetEditTo((GroupNodeEdit) parentOfNewEdit, false);
							for (TreeNodeEdit edit : newEdit.getAllChildren()) {
								integrateEdit.useOverwritePolicy(edit);
							}
							((DataSetNodeEdit) integrateEdit).integrate((DataSetNodeEdit) newEdit, ColumnNodeEdit.UNKNOWN_ROW_COUNT, false);
						}
						// TODO do it without this line (because of copy...EditTo())
						integrateEdit.setCopyEdit(null);
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
	 * 
	 * @return true if an overwrite is applicable respective
	 * if the location of the hdfObject will be changed (but not deleted) by execution of this edit
	 */
	private boolean isOverwriteApplicable() {
		return m_editAction.isCreateOrCopyAction()
				|| m_editAction == EditAction.MODIFY && !getOutputPathFromFileWithName(true).equals(m_inputPathFromFileWithName);
	}
	
	protected void validate(BufferedDataTable inputTable, boolean internalCheck, boolean externalCheck) {
		InvalidCause cause = null;
		
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
		
		if (externalCheck && cause == null) {
			if (m_editAction == EditAction.COPY) {
				cause = m_copyEdit == null ? InvalidCause.NO_COPY_SOURCE : cause;
			} else if (m_editAction == EditAction.CREATE) {
				// TODO get inputSpec names and flowVariable names here
				// cause = false ? InvalidCause.NO_COPY_SOURCE : cause;
			} else {
				cause = isSupported() && m_hdfObject == null ? InvalidCause.NO_HDF_SOURCE : cause;
			}
		}
		
		cause = cause == null && internalCheck ? validateEditInternal(inputTable) : cause;
		
		updateInvalidMap(this, cause);

		for (TreeNodeEdit edit : getAllChildren()) {
	        edit.validate(inputTable, internalCheck, externalCheck);
		}
	}
	
	protected abstract InvalidCause validateEditInternal(BufferedDataTable inputTable);
	
	protected abstract boolean isInConflict(TreeNodeEdit edit);
	
	protected boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws CanceledExecutionException, IOException {
		exec.checkCanceled();
		
		setEditState(EditState.IN_PROGRESS);
		
		boolean success = false;
		boolean successOfOther = true;

		try {
			switch (m_editAction) {
			case CREATE:
				success = createAction(inputTable, flowVariables, saveColumnProperties, exec, totalProgressToDo);
				break;
			case COPY:
				success = copyAction(exec, totalProgressToDo);
				break;
			case NO_ACTION:
			case MODIFY_CHILDREN_ONLY:
				success = true;
				break;
			default:
				break;
			}
			
			if (success) {
				setSuccess(true, exec, totalProgressToDo);
			}
			
			successOfOther &= doChildActionsInOrder(inputTable, flowVariables, saveColumnProperties, exec, totalProgressToDo);
			
			switch (m_editAction) {
			case DELETE:
				success = deleteAction();
				break;
			case MODIFY:
				success = modifyAction(exec, totalProgressToDo);
				break;
			default:
				break;
			}
			
			if (success) {
				setSuccess(true, exec, totalProgressToDo);
			}
		} finally {
			if (!success) {
				setSuccess(false, exec, totalProgressToDo);
			}
		}
		
		return success && successOfOther;
	}

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
		
		// TODO maybe find a better solution for the problem if MODIFY and DELETE exist for the same object (cause by OVERWRITE or INTEGRATE+OVERWRITE)
		/*for (TreeNodeEdit otherEdit : otherEdits.toArray(new TreeNodeEdit[otherEdits.size()])) {
			if (otherEdit.getEditAction() == EditAction.MODIFY) {
				for (TreeNodeEdit deleteEdit : deleteEdits) {
					if (otherEdit.isNameConflictPossible(deleteEdit.getClass())
							&& otherEdit.getInputPathFromFileWithName().equals(deleteEdit.getInputPathFromFileWithName())) {
						// TODO edit otherEdit so that it isn't stated as EditState.TODO
						otherEdits.remove(otherEdit);
						break;
					}
				}
			}
		}
		
		List<TreeNodeEdit> executeEdits = new ArrayList<>(deleteEdits);
		executeEdits.addAll(otherEdits);*/
		for (TreeNodeEdit edit : children) {
			if (edit.isBackupNeeded(objectNames, attributeNames)) {
				edit.createBackup();
				if (edit.getEditAction() != EditAction.DELETE) {
					edit.deleteAction();
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
	
	private boolean isBackupNeeded(List<String> objectNames, List<String> attributeNames) {
		boolean backupNeeded = false;
		
		if (m_editAction == EditAction.DELETE) {
			backupNeeded = m_parent.getEditAction() != EditAction.DELETE;
			
		} else if (m_editAction == EditAction.MODIFY) {
			backupNeeded = havePropertiesChanged();
			if (!backupNeeded) {
				String[] pathAndName = Hdf5TreeElement.getPathAndName(m_inputPathFromFileWithName);
				backupNeeded = getOutputPathFromFile().equals(pathAndName[0])
						&& (this instanceof AttributeNodeEdit ? attributeNames : objectNames).contains(pathAndName[1]);
			}
		}
		
		return backupNeeded;
	}

	protected abstract boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables,
			boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws IOException, CanceledExecutionException;
	protected abstract boolean copyAction(ExecutionContext exec, long totalProgressToDo) throws IOException, CanceledExecutionException;
	protected abstract boolean deleteAction() throws IOException;
	protected abstract boolean modifyAction(ExecutionContext exec, long totalProgressToDo) throws IOException, CanceledExecutionException;
	
	protected boolean rollbackAction() throws IOException {
		boolean success = false;
		
		switch (m_editAction) {
		case CREATE:
		case COPY:
			success = deleteAction();
			break;
		case MODIFY:
		case DELETE:
			String newName = Hdf5TreeElement.getPathAndName(getInputPathFromFileWithName())[1];
			try {
				if (this instanceof AttributeNodeEdit) {
					Hdf5Attribute<?> attribute = (Hdf5Attribute<?>) getHdfSource();
					setHdfObject(attribute.getParent().renameAttribute(attribute.getName(), newName));
				} else {
					Hdf5TreeElement treeElement = (Hdf5TreeElement) getHdfSource();
					setHdfObject(treeElement.getParent().moveObject(treeElement.getName(), treeElement.getParent(), newName));
				}
				success = m_hdfObject != null;
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).warn(ioe.getMessage(), ioe);
				success = false;
			}
			break;
		default:
			success = true;
			break;
		}
		
		if (!success) {
			NodeLogger.getLogger(getClass()).warn("Rollback of "
					+ getOutputPathFromFileWithName() + " could not be executed. Backup is still available.");
		}
		
		return success;
	}
	
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
		
		private static int compareProperties(TreeNodeEdit o1, TreeNodeEdit o2) {
			int compare = Integer.compare(getClassValue(o1), getClassValue(o2));
			if (o1 instanceof ColumnNodeEdit && o2 instanceof ColumnNodeEdit) {
				return (o1.getEditAction() == EditAction.DELETE) == (o2.getEditAction() == EditAction.DELETE) ? 0
						: (o1.getEditAction() == EditAction.DELETE ? 1 : -1);
			} else {
				return compare == 0 ? o1.getName().compareTo(o2.getName()) : compare;
			}
		}
		
		private static int getClassValue(TreeNodeEdit o) {
			return o.getClass() == GroupNodeEdit.class ? 0 : 
				(o.getClass() == DataSetNodeEdit.class ? 1 :
				(o.getClass() == ColumnNodeEdit.class ? 2 :
				(o.getClass() == AttributeNodeEdit.class ? 4 : 3)));
		}
	}
	
	public abstract class TreeNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = 4973286624577483071L;

    	private PropertiesDialog m_propertiesDialog;
		
    	private JMenuItem m_itemDelete;
		
		protected TreeNodeMenu(boolean editable, boolean groupCreatable, boolean deletable) {
			if (editable) {
				JMenuItem itemEdit = new JMenuItem("Edit properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (m_propertiesDialog == null) {
							m_propertiesDialog = getPropertiesDialog();
						}
						
						m_propertiesDialog.loadFromEdit();
						m_propertiesDialog.setVisible(true);
					}
				});
	    		add(itemEdit);
			}
			
    		if (groupCreatable) {
	    		JMenuItem itemCreateGroup = new JMenuItem("Create new group");
	    		itemCreateGroup.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						onCreateGroup();
					}
				});
	    		add(itemCreateGroup);
    		}

    		if (deletable) {
    			m_itemDelete = new JMenuItem();
    			updateDeleteItemText();
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
		
		private void updateDeleteItemText() {
			if (m_itemDelete != null) {
				m_itemDelete.setText(m_editAction != EditAction.DELETE ? "Delete" : "Remove deletion");
			}
		}
		
		private String getDeleteConfirmMessage() {
			return "Are you sure to "
					+ (m_editAction != EditAction.DELETE ? "delete" : "remove the deletion of")
					+ " this object and all its descendants?";
		}

		protected PropertiesDialog getPropertiesDialog() {
			// to implement in subclasses
			return null;
		}
		
		protected void onCreateGroup() {
			// to implement in subclasses
		}
		
		protected abstract void onDelete();
	}
	
	protected static abstract class PropertiesDialog extends JDialog {

		private static final long serialVersionUID = -2868431511358917946L;
		
		private final JPanel m_contentPanel = new JPanel();
		
		private final GridBagConstraints m_constraints = new GridBagConstraints();
		
		protected PropertiesDialog(Component comp, String title) {
			super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, comp), title, true);
			setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
			setLocation(400, 400);
			
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

		protected JCheckBox addProperty(String description, JComponent component, ChangeListener checkBoxListener) {
	        return addProperty(description, component, checkBoxListener, 0.0);
		}
		
		protected void addProperty(String description, JComponent component, double weighty) {
			addProperty(description, component, null, weighty);
		}
		
		protected void addProperty(String description, JComponent component) {
			addProperty(description, component, null, 0.0);
		}
		
		protected class PropertyDescriptionPanel extends JPanel {

			private static final long serialVersionUID = 3019076429508416644L;
			
			private JCheckBox m_checkBox;
			
			private PropertyDescriptionPanel(String description, ChangeListener checkBoxListener, boolean northwest) {
				setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
				if (checkBoxListener != null) {
					m_checkBox = new JCheckBox();
					add(m_checkBox);
					m_checkBox.addChangeListener(checkBoxListener);
					
					if (northwest) {
						m_checkBox.setAlignmentY(0.0f);
					}
				}
				JLabel nameLabel = new JLabel(description);
				add(nameLabel);
				if (northwest) {
					nameLabel.setAlignmentY(0.0f);
				}
			}
			
			private JCheckBox getCheckBox() {
				return m_checkBox;
			}
		}
		
		protected abstract void loadFromEdit();
		
		protected abstract void saveToEdit();
	}
}

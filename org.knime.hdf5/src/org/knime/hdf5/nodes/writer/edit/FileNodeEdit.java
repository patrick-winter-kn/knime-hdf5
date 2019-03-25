package org.knime.hdf5.nodes.writer.edit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
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
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.EditAction;

import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;

/**
 * Class for edits on files in an hdf file. The respective hdf
 * source is an {@linkplain Hdf5File}.
 */
public class FileNodeEdit extends GroupNodeEdit {

	private static final String COLUMN_PROPERTY_NAMES = "knime.columnnames";
	
	private static final String COLUMN_PROPERTY_TYPES = "knime.columntypes";
	
	private final String m_filePath;
	
	private JTree m_tree;
	
	/**
	 * Initializes a new file edit for the input hdf {@code file}.
	 * The edit action is set to NO_ACTION.
	 * 
	 * @param file the hdf file for this edit
	 */
	public FileNodeEdit(Hdf5File file) {
		this(file.getFilePath(), false, EditAction.NO_ACTION);
		setHdfObject(file);
	}

	/**
	 * Initializes a new file edit to CREATE a new file at the {@code filePath}.
	 * 
	 * @param filePath the file path for the new file
	 * @param overwriteFile if an existing file should be overwritten or if
	 * 	there should be an error then
	 */
	public FileNodeEdit(String filePath, boolean overwriteFile) {
		this(filePath, overwriteFile, EditAction.CREATE);
	}
	
	/**
	 * Initializes a new file edit with all core settings.
	 * 
	 * @param filePath the file path for the new file
	 * @param overwriteFile if an existing file should be overwritten or if
	 * 	there should be an error then
	 * @param editAction the action of this edit
	 */
	private FileNodeEdit(String filePath, boolean overwriteFile, EditAction editAction) {
		super(null, null, filePath.substring(filePath.lastIndexOf(File.separator) + 1),
				overwriteFile ? EditOverwritePolicy.OVERWRITE : EditOverwritePolicy.NONE, editAction);
		m_filePath = new File(filePath).getAbsolutePath();
	}
	
	/**
	 * Copies this file edit with only those descendants that have an action
	 * (i.e. the edit action is not NO_ACTION).
	 * 
	 * @return the new file edit
	 * @throws IOException if the hdf file of this edit could not be opened
	 */
	public FileNodeEdit copyWithoutNoActionEdits() throws IOException {
		FileNodeEdit copyFileEdit = null;
		if (getEditAction().isCreateOrCopyAction()) {
			copyFileEdit = new FileNodeEdit(m_filePath, isOverwriteHdfFile());
		} else {
			Hdf5File file = null;
			try {
				file = Hdf5File.openFile(m_filePath, Hdf5File.READ_ONLY_ACCESS);
				copyFileEdit = new FileNodeEdit(file);
				
			} finally {
				if (file != null) {
					file.close();
				}
			}
		}
		
		for (TreeNodeEdit edit : getAllChildren()) {
			if (edit.getEditAction() != EditAction.NO_ACTION) {
				edit.copyWithoutCopySourceTo(copyFileEdit);
			}
		}
		
		for (TreeNodeEdit edit : copyFileEdit.getAllDecendants()) {
			if (edit != copyFileEdit && !(edit instanceof ColumnNodeEdit) && edit.getEditAction() == EditAction.NO_ACTION) {
				edit.removeFromParent();
			}
		}
		
		return copyFileEdit;
	}

	/**
	 * @return the absolute file path of this edit's hdf file
	 */
	public String getFilePath() {
		return m_filePath;
	}
	
	/**
	 * @return if an already existing file on the file path should be overwritten
	 */
	public boolean isOverwriteHdfFile() {
		return getEditOverwritePolicy() == EditOverwritePolicy.OVERWRITE;
	}
	
	/**
	 * Does nothing because no parent of a file edit can exist.
	 * To set the tree node of this edit as a root of a {@linkplain JTree}, use
	 * {@linkplain FileNodeEdit#setEditAsRootOfTree(JTree)}.
	 * 
	 * @return {@code false}
	 * @see FileNodeEdit#setEditAsRootOfTree(JTree)
	 */
	@Override
	public boolean addEditToParentNodeIfPossible() {
		return false;
		// not needed here; instead use setEditAsRootOfTree[JTree]
	}
	
	/**
	 * Creates (if none exists) and sets the tree node of this file edit as the
	 * root of the {@linkplain JTree}. 
	 * 
	 * @param tree the tree to set the root
	 */
	public void setEditAsRootOfTree(JTree tree) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		((DefaultTreeModel) tree.getModel()).setRoot(m_treeNode);
		m_tree = tree;
		
		reloadTree();
	}

	/**
	 * Updates the {@linkplain JTree} where this file edit is the root of.
	 */
	public void reloadTree() {
		validate();
		((DefaultTreeModel) (m_tree.getModel())).reload();
	}
	
	/**
	 * Makes the tree node of {@code edit} visible or its tries to make its
	 * parent (including the children) visible if the tree node of {@code edit}
	 * is no descendant (anymore) of this edit's root node.
	 * 
	 * @param edit the edit to make visible
	 * @throws NullPointerException if no tree node for {@code edit} exists
	 */
	public void makeTreeNodeVisible(TreeNodeEdit edit) throws NullPointerException {
		if (edit.getTreeNode().isNodeAncestor(getTreeNode())) {
			m_tree.makeVisible(new TreePath(edit.getTreeNode().getPath()));
		} else if (edit.getParent().getTreeNode().getChildCount() != 0) {
			m_tree.makeVisible(new TreePath(edit.getParent().getTreeNode().getPath())
					.pathByAddingChild(edit.getParent().getTreeNode().getFirstChild()));
		} else {
			makeTreeNodeVisible(edit.getParent());
		}
	}
	
	/**
	 * Resets the {@code resetEdits} back to their initialization states with
	 * their hdf objects.
	 * 
	 * @param resetEdits the edits to be reset
	 */
	public void resetEdits(List<TreeNodeEdit> resetEdits) {
		Hdf5File file = null;
		try {
			file = Hdf5File.openFile(m_filePath, Hdf5File.READ_WRITE_ACCESS);
			
		} catch (IOException ioe) {
			// file is not needed then
		}
		
		for (TreeNodeEdit edit : resetEdits) {
			resetEdit(edit);
			if (edit.getParent() != null && edit.getParent().getEditAction() == EditAction.DELETE) {
				edit.setDeletion(true);
			}
		}
		reloadTree();

		if (file != null) {
			try {
				file.close();
			} catch (IOException ioe) {
				NodeLogger.getLogger(getClass()).error("Error while resetting TreeNodeEdits: " + ioe);
			}
		}
	}
	
	/**
	 * Resets the {@code edit} to the initialization state with its hdf object.
	 * 
	 * @param edit the edit to be reset
	 */
	private void resetEdit(TreeNodeEdit edit) {
		Object hdfObject = null;
		if (!edit.getEditAction().isCreateOrCopyAction()) {
			try {
				loadHdfObjectOf(edit);
				hdfObject = edit.getHdfObject();
			} catch (IOException ioe) {
				// hdfObject stays null
			}
		}
		
		if (hdfObject != null) {
			if (edit instanceof ColumnNodeEdit) {
				edit = edit.getParent();
			}
			
			TreeNodeEdit newEdit = null;
			if (edit instanceof GroupNodeEdit) {
				newEdit = new GroupNodeEdit((GroupNodeEdit) edit.getParent(), (Hdf5Group) hdfObject);
			} else if (edit instanceof DataSetNodeEdit) {
				newEdit = new DataSetNodeEdit((GroupNodeEdit) edit.getParent(), (Hdf5DataSet<?>) hdfObject);
			} else if (edit instanceof AttributeNodeEdit) {
				try {
					newEdit = new AttributeNodeEdit(edit.getParent(), (Hdf5Attribute<?>) hdfObject);
				} catch (IOException ioe) {
					NodeLogger.getLogger(getClass()).error("Could not reset AttributeNodeEdit: " + ioe);
					newEdit = null;
				}
			}
			if (newEdit != null) {
				newEdit.addEditToParentNodeIfPossible();
				edit.copyPropertiesFrom(newEdit);
				newEdit.removeFromParentCascade();
			}
		} else {
			edit.setDeletion(true);
		}
	}
	
	/**
	 * Get the descendant group edit with the specified input path within the
	 * hdf file of this edit or {@code null} if none exists. Every group edit
	 * in between the path also has to exist in the same way (recursively).
	 * 
	 * @param inputPathFromFileWithName the input path from file
	 * @return the searched descendant group edit
	 */
	GroupNodeEdit getGroupEditByPath(String inputPathFromFileWithName) {
		if (!inputPathFromFileWithName.isEmpty()) {
			int pathLength = inputPathFromFileWithName.lastIndexOf("/");
			GroupNodeEdit parent = pathLength >= 0 ? getGroupEditByPath(inputPathFromFileWithName.substring(0, pathLength)) : this;
			
			return parent != null ? parent.getGroupNodeEdit(inputPathFromFileWithName) : null;
			
		} else {
			return this;
		}
	}

	/**
	 * Get the descendant dataSet edit with the specified input path within the
	 * hdf file of this edit or {@code null} if none exists. Every group edit
	 * in between the path also has to exist in the same way (recursively).
	 * 
	 * @param inputPathFromFileWithName the input path from file
	 * @return the searched descendant dataSet edit
	 */
	DataSetNodeEdit getDataSetEditByPath(String inputPathFromFileWithName) {
		int pathLength = inputPathFromFileWithName.lastIndexOf("/");
		GroupNodeEdit parent = pathLength >= 0 ? getGroupEditByPath(inputPathFromFileWithName.substring(0, pathLength)) : this;
		
		return parent != null ? parent.getDataSetNodeEdit(inputPathFromFileWithName) : null;
	}

	/**
	 * Get the descendant column edit which is a child of a dataSet with the
	 * specified input path within a hdf file of this edit or {@code null}
	 * if none exists. Every group edit in between the path also has to exist
	 * in the same way (recursively).
	 * <br>
	 * Finds the column edit within the
	 * dataSet edit by using the {@code inputColumnIndex}.
	 * 
	 * @param inputPathFromFileWithName the input path from file
	 * @return the searched descendant column edit
	 */
	ColumnNodeEdit getColumnEditByPath(String inputPathFromFileWithName, int inputColumnIndex) {
		DataSetNodeEdit parent = getDataSetEditByPath(inputPathFromFileWithName);
		
		return parent != null ? parent.getColumnNodeEdit(inputPathFromFileWithName, inputColumnIndex) : null;
	}

	/**
	 * Get the descendant attribute edit with the specified input path within the
	 * hdf file of this edit or {@code null} if none exists. Every group edit
	 * and dataSet edit in between the path also has to exist in the same way
	 * (recursively).
	 * 
	 * @param inputPathFromFileWithName the input path from file
	 * @return the searched descendant attribute edit
	 */
	AttributeNodeEdit getAttributeEditByPath(String inputPathFromFileWithName) {
		AttributeNodeEdit attributeEdit = null;
		
		String[] pathAndName = Hdf5TreeElement.getPathAndName(inputPathFromFileWithName);
		GroupNodeEdit parentGroup = getGroupEditByPath(pathAndName[0]);
		
		if (parentGroup != null) {
			attributeEdit = parentGroup.getAttributeNodeEdit(inputPathFromFileWithName, EditAction.NO_ACTION);
			
		} else {
			DataSetNodeEdit parentDataSet = getDataSetEditByPath(pathAndName[0]);
			attributeEdit = parentDataSet != null ? parentDataSet.getAttributeNodeEdit(inputPathFromFileWithName, EditAction.NO_ACTION) : null;
		}
		
		return attributeEdit;
	}
	
	@Override
	protected long getProgressToDoInEdit() {
		return 1L + (getEditAction().isCreateOrCopyAction() ? 195L : 0L);
	}

	/**
	 * @return the total progress which will be done during the execution for
	 * 	this edit and its descendants
	 * @throws IOException if a hdf source is needed and cannot be found
	 */
	private long getTotalProgressToDo() throws IOException {
		long progressToDo = 0;
		
		for (TreeNodeEdit edit : getAllDecendants()) {
			progressToDo += edit.getProgressToDoInEdit();
		}
		
		return progressToDo;
	}

	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);
		
        settings.addString(SettingsKey.FILE_PATH.getKey(), m_filePath);
	}

	/**
	 * Loads the stored file edit from {@code settings}. Uses the input
	 * {@code filePath} if not {@code null} or empty. Otherwise, uses the
	 * file path stored in the {@code settings}. Same for {@code policy}.
	 * 
	 * @param settings the settings to be load from
	 * @param filePath the file path to use if a different one from the
	 * 	{@code settings} should be used
	 * @param policy the overwrite policy to use if a different one from the
	 * 	{@code settings} should be used
	 * @return the loaded file edit
	 * @throws InvalidSettingsException if a required property does not exist
	 */
	public static FileNodeEdit loadSettingsFrom(final NodeSettingsRO settings, String filePath, EditOverwritePolicy policy) throws InvalidSettingsException {
		FileNodeEdit edit = null;
		
		if (filePath == null || filePath.trim().isEmpty()) {
			filePath = settings.getString(SettingsKey.FILE_PATH.getKey());
		}
		if (policy == null) {
			policy = EditOverwritePolicy.get(settings.getString(SettingsKey.EDIT_OVERWRITE_POLICY.getKey()));
		}
		try {
			String renamedFilePath = policy == EditOverwritePolicy.RENAME ? Hdf5File.getUniqueFilePath(filePath) : filePath;
			EditAction editAction = policy != EditOverwritePolicy.INTEGRATE || !Hdf5File.existsHdf5File(filePath)
					? EditAction.CREATE : EditAction.NO_ACTION;
			
			edit = new FileNodeEdit(renamedFilePath, policy == EditOverwritePolicy.OVERWRITE, editAction);
			
		} catch (IOException ioe) {
			throw new InvalidSettingsException(ioe.getMessage(), ioe);
		}
	    
		edit.loadSettingsFrom(settings);
		return edit;
	}
	
	@Override
	public void loadChildrenOfHdfObject() throws IOException {
		super.loadChildrenOfHdfObject();
		validate();
	}
	
	/**
	 * Loads the hdf objects of all descendants of the hdf file.
	 * The hdf must already be loaded.
	 * 
	 * @throws IOException if a hdf object could not be loaded
	 */
	private void loadAllHdfObjectsOfFile() throws IOException {
		List<TreeNodeEdit> descendants = getAllDecendants();
		descendants.remove(this);
		for (TreeNodeEdit edit : descendants) {
			loadHdfObjectOf(edit);
		}
	}

	/**
	 * Loads the hdf object of {@code edit}.
	 * 
	 * @param edit the edit for which the hdf object should be loaded
	 * @throws IOException if the hdf object of {@code edit} could not be loaded
	 * @throws NullPointerException if the parent hdf object has not been loaded
	 */
	private void loadHdfObjectOf(TreeNodeEdit edit) throws IOException, NullPointerException {
		if (edit.getEditAction() != EditAction.CREATE && edit.getInputPathFromFileWithName() != null) {
			if (edit instanceof GroupNodeEdit) {
				edit.setHdfObject(((Hdf5File) getHdfObject()).getGroupByPath(edit.getInputPathFromFileWithName()));
			} else if (edit instanceof DataSetNodeEdit || edit instanceof ColumnNodeEdit) {
				edit.setHdfObject(((Hdf5File) getHdfObject()).getDataSetByPath(edit.getInputPathFromFileWithName()));
			} else if (edit instanceof AttributeNodeEdit) {
				edit.setHdfObject(((Hdf5File) getHdfObject()).getAttributeByPath(edit.getInputPathFromFileWithName()));
			}
		}
	}

	/**
	 * Integrates {@code copyEdit} to this file edit specified by
	 * {@linkplain FileNodeEdit#integrate(FileNodeEdit)}. Also updates
	 * its copy sources and validates this edit.
	 * <br>
	 * <br>
	 * For more information
	 * why the validation failed, see
	 * {@linkplain FileNodeEdit#getInvalidCauseMessages(TreeNodeEdit)}.
	 * 
	 * @param copyEdit the file edit to be integrated
	 * @return if this file edit is valid
	 * @see FileNodeEdit#getInvalidCauseMessages(TreeNodeEdit)
	 */
	public boolean integrateAndValidate(FileNodeEdit copyEdit) {
		integrate(copyEdit);
		updateCopySources();
		validate();
		return isValid();
	}

	/**
	 * Integrates the children (and descendants) of {@code copyEdit}
	 * (not those with {@code editAction == NO_ACTION}) in this file edit. The
	 * properties of this file edit will stay the same. The properties
	 * of this edit's children will be overwritten for those with an
	 * action (not for those with {@code editAction == MODIFY_CHILDREN_ONLY}).
	 * 
	 * @param copyEdit the file edit to be integrated
	 */
	public void integrate(FileNodeEdit copyEdit) {
		super.integrate(copyEdit);
	}

	/**
	 * First applies the overwrites policies of this file and its descendants.
	 * <br>
	 * <br>
	 * Then integrates {@code copyEdit} to this file edit specified by
	 * {@linkplain FileNodeEdit#integrate(FileNodeEdit)}. Also updates
	 * its copy sources and validates this edit, also considering
	 * the data and row size of the {@code inputTable}.
	 * <br>
	 * <br>
	 * For more information
	 * why the validation failed, see
	 * {@linkplain FileNodeEdit#getInvalidCauseMessages(TreeNodeEdit)}.
	 * 
	 * @param copyEdit the file edit to be integrated
	 * @param inputTable the knime data table that will be used to create
	 * 	dataSets with help of the dataSet edits
	 * @return if this file edit is valid
	 * @see FileNodeEdit#getInvalidCauseMessages(TreeNodeEdit)
	 */
	public boolean finalIntegrateAndValidate(FileNodeEdit copyEdit, BufferedDataTable inputTable) {
		useOverwritePolicyForFile(copyEdit);
		integrate(copyEdit);
		updateCopySources();
		doLastValidation(copyEdit, inputTable);
		return isValid() && copyEdit.isValid();
	}
	
	/**
	 * Uses the overwrite policies of all children of this file edit to add
	 * them in this edit, but does not actually add them.
	 * 
	 * @param fileEdit the new edit which should be integrated to this edit
	 * @see TreeNodeEdit#useOverwritePolicy(TreeNodeEdit)
	 */
	private void useOverwritePolicyForFile(FileNodeEdit fileEdit) {
		for (TreeNodeEdit edit : fileEdit.getAllChildren()) {
			useOverwritePolicy(edit);
		}
		
		// postprocessing of EditOverwritePolicy.INTEGRATE
		for (TreeNodeEdit edit : fileEdit.getAllDecendants()) {
			if (edit.getEditAction() == EditAction.NO_ACTION && !(edit instanceof ColumnNodeEdit || edit instanceof FileNodeEdit)) {
				edit.removeFromParent();
			}
		}
	}
	
	/**
	 * Validates this file edit externally and {@code copyEdit} internally with
	 * help of the {@code inputTable}.
	 * 
	 * @param copyEdit edit for internal validation
	 * @param inputTable
	 */
	private void doLastValidation(FileNodeEdit copyEdit, BufferedDataTable inputTable) {
		// external validation
		validate(null, false, true);
		// internal validation
		copyEdit.validate(inputTable, true, false);
	}

	/**
	 * Validates this file edit internally and externally.
	 */
	private void validate() {
		validate(null, true, true);
	}
	
	/**
	 * Validates this edit internally and/or externally (based on if the validation
	 * result depends on the respective hdf file). Also uses the {@code inputTable}
	 * in an internal validation.
	 * 
	 * @param inputTable
	 * @param internalCheck if an internal check should be done
	 * @param externalCheck if an external check should be done
	 */
	protected void validate(BufferedDataTable inputTable, boolean internalCheck, boolean externalCheck) {
		Hdf5File file = null;
		try {
			file = (Hdf5File) getHdfObject();
			if (file != null) {
				file.open(Hdf5File.READ_ONLY_ACCESS);
			}
			
			validate(internalCheck, externalCheck);
			if (internalCheck && inputTable != null) {
				lastValidationOfColumnEdits(inputTable);
			}
		} catch (Exception e) {
			NodeLogger.getLogger(getClass()).error(e.getMessage(), e);
			
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException ioe) {
					NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
				}
			}
		}
	}
	
	@Override
	protected InvalidCause validateEditInternal() {
		InvalidCause cause = m_filePath.endsWith(".h5") || m_filePath.endsWith(".hdf5") ? null : InvalidCause.FILE_EXTENSION;
		
		cause = cause == null && getName().contains("/") ? InvalidCause.NAME_CHARS : cause;
		
		return cause;
	}
	
	/**
	 * @return the cause why the creation of new hdf file for this edit is
	 * 	invalid (or {@code null} if it is valid)
	 */
	InvalidCause validateFileCreation() {
		InvalidCause cause = null;
		
		if (getEditAction() == EditAction.CREATE) {
			cause = !isOverwriteHdfFile() && Hdf5File.existsHdf5File(m_filePath) ? InvalidCause.FILE_ALREADY_EXISTS : cause;
			
			cause = cause == null && !Hdf5File.isHdf5FileCreatable(m_filePath, isOverwriteHdfFile()) ? InvalidCause.NO_DIR_FOR_FILE : cause;
		}
		
		return cause;
	}
	
	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof FileNodeEdit && !edit.equals(this) && ((FileNodeEdit) edit).getFilePath().equals(getFilePath())
				&& edit.getName().equals(getName());
	}
	
	/**
	 * Validates all descendants with a CREATE action based on the knime input
	 * specs (not based on the data) such that it will be noticed if some knime
	 * input is different from the one when the settings were defined.
	 * 
	 * @param colSpecs the knime column specs of the input table
	 * @param flowVariables the knime flow variables
	 */
	public void validateCreateActions(DataColumnSpec[] colSpecs, Map<String, FlowVariable> flowVariables) {
		for (TreeNodeEdit edit : getAllDecendants()) {
			if (edit.getEditAction() == EditAction.CREATE) {
				if (edit instanceof ColumnNodeEdit) {
					((ColumnNodeEdit) edit).validateCreateAction(colSpecs);
				} else if (edit instanceof AttributeNodeEdit) {
					((AttributeNodeEdit) edit).validateCreateAction(flowVariables);
				}
			}
		}
	}
	
	/**
	 * Validate the column edits on the data and row size of the knime input
	 * table.
	 * 
	 * @param inputTable the knime input table
	 */
	private void lastValidationOfColumnEdits(BufferedDataTable inputTable) {
		long inputRowCount = inputTable.size();
		List<ColumnNodeEdit> validateEdits = new ArrayList<>();
		for (TreeNodeEdit edit : getAllDecendants()) {
			if (edit instanceof ColumnNodeEdit && edit.getEditAction() == EditAction.CREATE) {
				ColumnNodeEdit columnEdit = (ColumnNodeEdit) edit;
				validateEdits.add(columnEdit);
				columnEdit.setInputRowSize(inputRowCount);
			}
		}

		DataTableSpec tableSpec = inputTable.getDataTableSpec();
		CloseableRowIterator iter = inputTable.iterator();
		while (iter.hasNext()) {
			DataRow row = iter.next();
			InvalidCause[] causes = new InvalidCause[validateEdits.size()];
			for (int i = 0; i < validateEdits.size(); i++) {
				ColumnNodeEdit edit = validateEdits.get(i);
				EditDataType parentDataType = ((DataSetNodeEdit) edit.getParent()).getEditDataType();
				Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(parentDataType.getOutputType(), true);
				Object standardValue = parentDataType.getStandardValue();
				int columnIndex = tableSpec.findColumnIndex(edit.getInputPathFromFileWithName());
				
				try {
					Object value = knimeType.getValueFromDataCell(row.getCell(columnIndex));
					if (value == null) {
						if (standardValue != null) {
							value = standardValue;
						} else {
							causes[i] = InvalidCause.MISSING_VALUES;
						}
					}
					if (value != null) {
						causes[i] = !parentDataType.getOutputType().areValuesConvertible(new Object[]{ value }, edit.getInputType(), parentDataType) ? InvalidCause.OUTPUT_DATA_TYPE : null;
					}
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger(getClass()).error("Validation of dataType of new column \""
							+ edit.getOutputPathFromFileWithName() +  "\" could not be checked: " + udte.getMessage(), udte);
				}
			}
			for (int i = causes.length-1; i >= 0; i--) {
				ColumnNodeEdit edit = validateEdits.get(i);
				boolean valid = edit.isValid();
				if (valid) {
					InvalidCause cause = causes[i] != null ? causes[i] : edit.validateEditInternal();
					if (cause != null) {
						edit.updateInvalidMap(cause);
						valid = false;
					}
				}
				if (!valid) {
					validateEdits.remove(i);
				}
			}
		}
	}
	
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
	 * @return if the execution of this edit and all descendants was successful
	 * @throws CanceledExecutionException if the user cancelled the node while
	 * 	executing this edit
	 * @throws IOException if an error occurred
	 * @throws NullPointerException if {@code inputTable} is {@code null}
	 * 	although dataSets need to be written with its data
	 */
	public boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec) throws CanceledExecutionException, IOException, NullPointerException {
		boolean preparationSuccess = false;
		setEditState(EditState.IN_PROGRESS);
		try {
			// TODO there might still be race conditions from outside the program
			Hdf5File.checkHdf5FileWritable(m_filePath);
			
			if (!getEditAction().isCreateOrCopyAction()) {
				setHdfObject(Hdf5File.openFile(m_filePath, Hdf5File.READ_WRITE_ACCESS));
				preparationSuccess = getHdfObject() != null;
				if (preparationSuccess) {
					loadAllHdfObjectsOfFile();
				}
			} else if (isOverwriteHdfFile() && Hdf5File.existsHdf5File(m_filePath)) {
				setHdfObject(Hdf5File.openFile(m_filePath, Hdf5File.READ_WRITE_ACCESS));
				createBackup();
				preparationSuccess = deleteActionAndResetEditSuccess();
			} else {
				preparationSuccess = true;
			}
		} finally {
			if (!preparationSuccess) {
				setEditState(EditState.FAIL);
			}
		}
		
		exec.setProgress(0.0);
		// TODO change after testing
		long pTD = getTotalProgressToDo();
		boolean success = preparationSuccess && doAction(inputTable, flowVariables, saveColumnProperties, exec, pTD)
				&& doPostponedDataSetActions(inputTable, saveColumnProperties, exec, pTD);
		System.out.println("TotalProgressToDo: " + pTD);
		return success;
	}
	
	@Override
	protected void createAction(Map<String, FlowVariable> flowVariables, ExecutionContext exec, long totalProgressToDo) throws IOException {
		try {
			setHdfObject(Hdf5File.createFile(m_filePath));
		} finally {
			setEditSuccess(getHdfObject() != null);
		}
	}

	/**
	 * Not supported here.
	 */
	@Override
	protected void copyAction(ExecutionContext exec, long totalProgressToDo) {
	}

	@Override
	protected void deleteAction() throws IOException {
		try {
			Hdf5File file = (Hdf5File) getHdfObject();
			if (file.deleteFile()) {
				setHdfObject((Hdf5File) null);
			}
		} finally {
			setEditSuccess(getHdfObject() == null);
		}
	}

	/**
	 * Not supported here.
	 */
	@Override
	protected void modifyAction(ExecutionContext exec, long totalProgressToDo) {
	}
	
	/**
	 * Executes the actions of the dataSets which have already been created by
	 * the regular action, but could not be written with the input data. So
	 * in this method, the dataSets will be written with the data of
	 * {@code inputTable}.
	 * 
	 * @param inputTable the knime input table
	 * @param saveColumnProperties if the column properties of the input table
	 * 	should be stored as attributes in the respective dataSets in hdf
	 * @param exec the knime execution context
	 * @param totalProgressToDo the total progress to do while executing the
	 * 	hdf writer
	 * @return if the execution of this edit and all descendants was successful
	 * @throws CanceledExecutionException if the user cancelled the node while
	 * 	executing this edit
	 * @throws IOException if an error occurred
	 * @throws NullPointerException if {@code inputTable} is {@code null}
	 * 	although dataSets need to be written with its data
	 */
	@SuppressWarnings("unchecked")
	private boolean doPostponedDataSetActions(BufferedDataTable inputTable, boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws CanceledExecutionException, IOException, NullPointerException {
		List<DataSetNodeEdit> dataSetEditList = new ArrayList<>();
		for (TreeNodeEdit edit : getAllDecendants()) {
			if (edit.getEditState() == EditState.CREATE_SUCCESS_WRITE_POSTPONED) {
				if (edit instanceof DataSetNodeEdit) {
					dataSetEditList.add((DataSetNodeEdit) edit);
				}
			}
		}
		DataSetNodeEdit[] dataSetEdits = dataSetEditList.toArray(new DataSetNodeEdit[dataSetEditList.size()]);
		
		if (inputTable == null) {
			if (dataSetEdits.length == 0) {
				return true;
			}
			throw new NullPointerException("inputTable cannot be null when creating dataSets");
		}
		
		DataTableSpec tableSpec = inputTable.getDataTableSpec();
		long inputRowCount = inputTable.size();
		
		int[][] specIndices = new int[dataSetEdits.length][];
		List<ColumnNodeEdit>[] copyColumnEditLists = (List<ColumnNodeEdit>[]) new ArrayList<?>[dataSetEdits.length];
		ColumnNodeEdit[][] copyColumnEdits = new ColumnNodeEdit[dataSetEdits.length][];
		Hdf5DataSet<?>[][] copyDataSets = new Hdf5DataSet<?>[dataSetEdits.length][];
		long[][] dataSetColumnIndices = new long[dataSetEdits.length][];
		Hdf5DataSet<Object>[] outputDataSets = (Hdf5DataSet<Object>[]) new Hdf5DataSet<?>[dataSetEdits.length];
		
		for (int i = 0; i < dataSetEdits.length; i++) {
			try {
				ColumnNodeEdit[] columnEdits = dataSetEdits[i].getNotDeletedColumnNodeEdits();
				specIndices[i] = new int[columnEdits.length];
				copyColumnEditLists[i] = new ArrayList<>();
				for (int j = 0; j < columnEdits.length; j++) {
					ColumnNodeEdit edit = columnEdits[j];
					if (edit.getEditAction() == EditAction.CREATE) {
						specIndices[i][j] = tableSpec.findColumnIndex(edit.getName());
						edit.setInputRowSize(inputRowCount);
						
					} else {
						specIndices[i][j] = -1;
						copyColumnEditLists[i].add(edit);
					}
				}
				
				copyColumnEdits[i] = copyColumnEditLists[i].toArray(new ColumnNodeEdit[copyColumnEditLists[i].size()]);
				copyDataSets[i] = new Hdf5DataSet<?>[copyColumnEdits[i].length];
				dataSetColumnIndices[i] = new long[copyColumnEdits[i].length];
				for (int j = 0; j < copyColumnEdits[i].length; j++) {
					copyDataSets[i][j] = (Hdf5DataSet<?>) copyColumnEdits[i][j].findCopySource();
					dataSetColumnIndices[i][j] = copyColumnEdits[i][j].getInputColumnIndex();
					copyDataSets[i][j].open();
				}

				outputDataSets[i] = (Hdf5DataSet<Object>) dataSetEdits[i].getHdfObject();
				dataSetEdits[i].setHdfObject((Hdf5DataSet<Object>) null);
				
			} catch (Exception e) {
				dataSetEdits[i].setEditState(EditState.FAIL);
				throw e;
			}
		}
		
		boolean success = false;
		CloseableRowIterator iter = inputTable.iterator();
		try {
			boolean withoutFail = true;
			long rowIndex = 0;
			while (iter.hasNext()) {
				exec.checkCanceled();
			
				DataRow row = iter.next();
				
				for (int i = 0; i < dataSetEdits.length; i++) {
					try {
						withoutFail &= outputDataSets[i].copyValuesToRow(rowIndex, row, specIndices[i], copyDataSets[i], dataSetColumnIndices[i],
								dataSetEdits[i].getEditDataType().getStandardValue(), dataSetEdits[i].getEditDataType().getRounding());
						if (withoutFail) {
							addProgress(dataSetEdits[i].getProgressToDoPerRow(), exec, totalProgressToDo, false);
						}
					} catch (HDF5DataspaceInterfaceException hdie) {
						dataSetEdits[i].setEditState(EditState.FAIL);
						throw new IOException("Fail for writing dataSet \"" + outputDataSets[i].getPathFromFileWithName() + "\": " + hdie.getMessage(), hdie);
					
					} catch (Exception e) {
						dataSetEdits[i].setEditState(EditState.FAIL);
						throw e;
					}
				}
				
				rowIndex++;
			}

			if (withoutFail && saveColumnProperties) {
				for (int i = 0; i < dataSetEdits.length; i++) {
					ColumnNodeEdit[] columnEdits = dataSetEdits[i].getNotDeletedColumnNodeEdits();
					String[] columnNames = new String[columnEdits.length];
					String[] columnTypes = new String[columnEdits.length];
					for (int j = 0; j < columnEdits.length; j++) {
						exec.checkCanceled();
						
						ColumnNodeEdit edit = columnEdits[j];
						if (edit.getEditAction() == EditAction.CREATE) {
							DataColumnSpec spec = tableSpec.getColumnSpec(specIndices[i][j]);
							columnNames[j] = spec.getName();
							columnTypes[j] = spec.getType().getName();
							
						} else {
							// TODO use here property attributes which already exist
							columnNames[j] = edit.getName();
							columnTypes[j] = edit.getInputType().toString();
						}
					}
	
					try {
						withoutFail &= outputDataSets[i].createAndWriteAttribute(COLUMN_PROPERTY_NAMES, columnNames, false) != null;
						withoutFail &= outputDataSets[i].createAndWriteAttribute(COLUMN_PROPERTY_TYPES, columnTypes, false) != null;
					} catch (Exception e) {
						dataSetEdits[i].setEditState(EditState.FAIL);
						throw new IOException("Property attributes of dataSet \"" + outputDataSets[i].getPathFromFileWithName()
								+ "\" could not be written completely: " + e.getMessage(), e);
					}
				}
			}
			
			success = withoutFail;
			
		} finally {
			iter.close();
			
			for (int i = 0; i < dataSetEdits.length; i++) {
				if (success) {
					if (dataSetEdits[i].getEditAction() == EditAction.MODIFY) {
						Hdf5DataSet<?> oldDataSet = (Hdf5DataSet<?>) dataSetEdits[i].getHdfSource();
						for (String attrName : oldDataSet.loadAttributeNames()) {
							outputDataSets[i].copyAttribute(oldDataSet.getAttribute(attrName), attrName);
						}
					}
					
					dataSetEdits[i].setEditState(EditState.SUCCESS);
					dataSetEdits[i].setHdfObject(outputDataSets[i]);
					
				} else {
					((Hdf5Group) dataSetEdits[i].getOpenedHdfObjectOfParent()).deleteObject(dataSetEdits[i].getName());
				}
			}
		}
		
		return success;
	}
	
	/**
	 * Deletes all backups in this file edit.
	 * 
	 * @return if all backups are deleted
	 */
	public boolean deleteAllBackups() {
		boolean success = true;
		
		for (TreeNodeEdit edit : getAllDecendants()) {
			success &= edit.deleteBackup();
		}
		
		return success;
	}
	
	/**
	 * Executes the all rollback actions of edits which have already been
	 * executed successfully before an error occurred.
	 * 
	 * @return if the hdf file is the same as before its execution
	 */
	public boolean doRollbackAction() {
		boolean success = true;
		
		if (getEditAction().isCreateOrCopyAction()) {
			if (getEditState() == EditState.SUCCESS) {
				try {
					success &= deleteActionAndResetEditSuccess();
					if (getHdfBackup() != null) {
						setHdfObject(((Hdf5File) getHdfBackup()).copyFile(m_filePath));
						success &= getHdfObject() != null;
					}
				} catch (Exception e) {
					success = false;
					NodeLogger.getLogger(getClass()).error(e.getMessage(), e);
					
				} finally {
					setEditState(success ? EditState.ROLLBACK_SUCCESS : EditState.ROLLBACK_FAIL);
				}
			} else {
				setEditState(EditState.ROLLBACK_NOTHING_TODO);
			}
		} else {
			List<TreeNodeEdit> rollbackEdits = getAllDecendants();
			rollbackEdits.remove(this);
			
			List<String> attributePaths = new ArrayList<>();
			List<String> objectPaths = new ArrayList<>();
			for (TreeNodeEdit edit : rollbackEdits.toArray(new TreeNodeEdit[rollbackEdits.size()])) {
				if (!edit.getEditState().isExecutedState()) {
					rollbackEdits.remove(edit);
				} else if (edit.getEditState() == EditState.SUCCESS && edit.getEditAction() == EditAction.MODIFY_CHILDREN_ONLY
						|| edit.getEditState() == EditState.FAIL && edit.getEditAction().isCreateOrCopyAction()) {
					edit.setEditState(EditState.ROLLBACK_NOTHING_TODO);
				} else if (edit.getEditAction() == EditAction.MODIFY || edit.getEditAction() == EditAction.DELETE) {
					(edit instanceof AttributeNodeEdit ? attributePaths : objectPaths).add(edit.getInputPathFromFileWithName());
				}
			}

			for (TreeNodeEdit edit : rollbackEdits.toArray(new TreeNodeEdit[rollbackEdits.size()])) {
				// in case hdfObject was created/edited before the fail, delete it so that it can be substituted by hdfBackup
				if (edit.getHdfObject() != null && edit.getEditState() != EditState.ROLLBACK_NOTHING_TODO
						&& (edit instanceof AttributeNodeEdit ? attributePaths : objectPaths).contains(edit.getOutputPathFromFileWithName(true))) {
					if (!edit.getEditAction().isCreateOrCopyAction()) {
						if (edit.getHdfBackup() == null) {
							success &= edit.createBackup();
						}
						
						try {
							success &= edit.deleteActionAndResetEditSuccess();
							
						} catch (IOException ioe) {
							success = false;
							rollbackEdits.remove(edit);
							edit.setEditState(EditState.ROLLBACK_FAIL);
							NodeLogger.getLogger(getClass()).error("Fail in rollback of \"" + edit.getOutputPathFromFileWithName() + "\": " + ioe.getMessage(), ioe);
						}
					} else {
						rollbackEdits.remove(edit);
						rollbackEdits.add(0, edit);
					}
				}
			}
			
			for (TreeNodeEdit edit : rollbackEdits) {
				try {
					success &= edit.rollbackAction();
				} catch (Exception e) {
					success = false;
					NodeLogger.getLogger(getClass()).error("Fail in rollback of \"" + edit.getOutputPathFromFileWithName() + "\": " + e.getMessage(), e);
				}
			}
		}
		
		return success;
	}
	
	@Override
	public String toString() {
		return "{ filePath=" + getFilePath() + ",action=" + getEditAction() + ",state=" + getEditState()
				+ ",overwriteFile=" + isOverwriteHdfFile() + ",valid=" + isValid()
				+ ",file=" + getHdfObject() + ",backup=" + getHdfBackup() + " }";
	}
}

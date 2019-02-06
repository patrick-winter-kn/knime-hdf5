package org.knime.hdf5.nodes.writer.edit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5TreeElement;

public class FileNodeEdit extends GroupNodeEdit {
	
	private final String m_filePath;
	
	private final boolean m_overwriteHdfFile;
	
	private JTree m_tree;
	
	public FileNodeEdit(Hdf5File file) {
		this(file.getFilePath(), false, EditAction.NO_ACTION);
		setHdfObject(file);
	}
	
	public FileNodeEdit(String filePath, boolean overwriteFile) {
		this(filePath, overwriteFile, EditAction.CREATE);
	}
	
	private FileNodeEdit(String filePath, boolean overwriteFile, EditAction editAction) {
		super(null, null, filePath.substring(filePath.lastIndexOf(File.separator) + 1), EditOverwritePolicy.NONE, editAction);
		m_filePath = filePath;
		m_overwriteHdfFile = overwriteFile;
	}
	
	public String getFilePath() {
		return m_filePath;
	}
	
	public boolean isOverwriteHdfFile() {
		return m_overwriteHdfFile;
	}
	
	GroupNodeEdit getGroupEditByPath(String inputPathFromFileWithName) {
		if (!inputPathFromFileWithName.isEmpty()) {
			int pathLength = inputPathFromFileWithName.lastIndexOf("/");
			GroupNodeEdit parent = pathLength >= 0 ? getGroupEditByPath(inputPathFromFileWithName.substring(0, pathLength)) : this;
			
			return parent != null ? parent.getGroupNodeEdit(inputPathFromFileWithName) : null;
			
		} else {
			return this;
		}
	}

	DataSetNodeEdit getDataSetEditByPath(String inputPathFromFileWithName) {
		int pathLength = inputPathFromFileWithName.lastIndexOf("/");
		GroupNodeEdit parent = pathLength >= 0 ? getGroupEditByPath(inputPathFromFileWithName.substring(0, pathLength)) : this;
		
		return parent != null ? parent.getDataSetNodeEdit(inputPathFromFileWithName) : null;
	}

	ColumnNodeEdit getColumnEditByPath(String inputPathFromFileWithName, int inputColumnIndex) {
		DataSetNodeEdit parent = getDataSetEditByPath(inputPathFromFileWithName);
		
		return parent != null ? parent.getColumnNodeEdit(inputPathFromFileWithName, inputColumnIndex) : null;
	}

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
	
	public static FileNodeEdit useFileSettings(final NodeSettingsRO settings, String filePath, final EditOverwritePolicy policy) throws InvalidSettingsException {
		FileNodeEdit edit = null;
		
		if (filePath == null || filePath.trim().isEmpty()) {
			filePath = settings.getString(SettingsKey.FILE_PATH.getKey());
		}
		try {
			String renamedFilePath = policy == EditOverwritePolicy.RENAME ? Hdf5File.getUniqueFilePath(filePath) : filePath;
			EditAction editAction = policy == EditOverwritePolicy.OVERWRITE || !filePath.equals(renamedFilePath)
					? EditAction.CREATE : EditAction.get(settings.getString(SettingsKey.EDIT_ACTION.getKey()));
			
			edit = new FileNodeEdit(renamedFilePath, policy == EditOverwritePolicy.OVERWRITE, editAction);
			
		} catch (IOException ioe) {
			throw new InvalidSettingsException(ioe.getMessage(), ioe);
		}
	    
		edit.loadSettingsFrom(settings);
		return edit;
	}

	private long getTotalProgressToDo() throws IOException {
		long progressToDo = 0;
		
		for (TreeNodeEdit edit : getAllDecendants()) {
			progressToDo += edit.getProgressToDoInEdit();
		}
		
		return progressToDo;
	}
	
	@Override
	protected long getProgressToDoInEdit() {
		return 1L + (getEditAction().isCreateOrCopyAction() ? 195L : 0L);
	}

	public boolean doLastValidationBeforeExecution(FileNodeEdit copyEdit, BufferedDataTable inputTable) {
		useOverwritePolicyForFile(copyEdit);
		super.integrate(copyEdit, inputTable != null ? inputTable.size() : ColumnNodeEdit.UNKNOWN_ROW_COUNT);
		updateCopySources();
		doLastValidation(copyEdit, inputTable);
		return isValid() && copyEdit.isValid();
	}
	
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

	public boolean integrateAndValidate(FileNodeEdit copyEdit) {
		integrate(copyEdit);
		updateCopySources();
		validate();
		return isValid();
	}
	
	public void integrate(FileNodeEdit copyEdit) {
		super.integrate(copyEdit, ColumnNodeEdit.UNKNOWN_ROW_COUNT);
	}

	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);
		
        settings.addString(SettingsKey.FILE_PATH.getKey(), m_filePath);
        settings.addBoolean(SettingsKey.OVERWRITE_HDF_FILE.getKey(), m_overwriteHdfFile);
	}
	
	@Override
	public void loadChildrenOfHdfObject() throws IOException {
		super.loadChildrenOfHdfObject();
		validate();
	}
	
	private void loadAllHdfObjectsOfFile() throws IOException {
		List<TreeNodeEdit> descendants = getAllDecendants();
		descendants.remove(this);
		for (TreeNodeEdit edit : descendants) {
			loadHdfObjectOf(edit);
		}
	}
	
	private void loadHdfObjectOf(TreeNodeEdit edit) throws IOException {
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
	
	@Override
	public boolean addEditToParentNodeIfPossible() {
		return false;
		// not needed here; instead use setEditAsRootOfTree[JTree]
	}
	
	public void setEditAsRootOfTree(JTree tree) {
		if (m_treeNode == null) {
			m_treeNode = new DefaultMutableTreeNode(this);
		}
		((DefaultTreeModel) tree.getModel()).setRoot(m_treeNode);
		m_tree = tree;
		
		reloadTree();
	}

	public void reloadTree() {
		validate();
		((DefaultTreeModel) (m_tree.getModel())).reload();
	}
	
	public void makeTreeNodeVisible(TreeNodeEdit edit) {
		if (edit.getTreeNode().isNodeAncestor(getTreeNode())) {
			m_tree.makeVisible(new TreePath(edit.getTreeNode().getPath()));
		} else if (edit.getParent().getTreeNode().getChildCount() != 0) {
			m_tree.makeVisible(new TreePath(edit.getParent().getTreeNode().getPath())
					.pathByAddingChild(edit.getParent().getTreeNode().getFirstChild()));
		} else {
			makeTreeNodeVisible(edit.getParent());
		}
	}
	
	public void removeInvalidsWithCause(InvalidCause cause) {
		for (TreeNodeEdit edit : getInvalidsWithCause(cause)) {
			edit.removeFromParent();
		}
		reloadTree();
	}

	private void validate() {
		validate(null, true, true);
	}
	
	private void doLastValidation(FileNodeEdit copyEdit, BufferedDataTable inputTable) {
		// external validation
		validate(null, false, true);
		// internal validation
		copyEdit.validate(inputTable, true, false);
	}
	
	@Override
	protected void validate(BufferedDataTable inputTable, boolean internalCheck, boolean externalCheck) {
		Hdf5File file = null;
		try {
			file = (Hdf5File) getHdfObject();
			if (file != null) {
				file.open(Hdf5File.READ_ONLY_ACCESS);
			}
			super.validate(inputTable, internalCheck, externalCheck);
			
		} catch (Exception e) {
			NodeLogger.getLogger(getClass()).error(e.getMessage(), e);
			
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException ioe) {
					NodeLogger.getLogger("HDF5 Files").error(ioe.getMessage(), ioe);
				}
			}
		}
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		InvalidCause cause = m_filePath.endsWith(".h5") || m_filePath.endsWith(".hdf5") ? null : InvalidCause.FILE_EXTENSION;
		
		cause = cause == null && getName().contains("/") ? InvalidCause.NAME_CHARS : cause;
		
		cause = cause == null && getEditAction() == EditAction.CREATE && Hdf5File.existsHdfFile(m_filePath) && !m_overwriteHdfFile ? InvalidCause.FILE_ALREADY_EXISTS : cause;
		
		cause = cause == null && getEditAction() == EditAction.CREATE && !Hdf5File.isHdfFileCreatable(m_filePath, m_overwriteHdfFile) ? InvalidCause.NO_DIR_FOR_FILE : cause;
		
		return cause;
	}
	
	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return edit instanceof FileNodeEdit && !edit.equals(this) && ((FileNodeEdit) edit).getFilePath().equals(getFilePath())
				&& edit.getName().equals(getName());
	}
	
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
	
	// continue here: manage EditStates of FileNodeEdit
	// see error in Hdf5File.isOpenAnywhere(): Bad value in H5.H5Fget_obj_count()
	public boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties, ExecutionContext exec) throws IOException, CanceledExecutionException {
		boolean preparationSuccess = false;
		setEditState(EditState.IN_PROGRESS);
		try {
			if (!getEditAction().isCreateOrCopyAction()) {
				setHdfObject(Hdf5File.openFile(m_filePath, Hdf5File.READ_WRITE_ACCESS));
				preparationSuccess = getHdfObject() != null;
				if (preparationSuccess) {
					loadAllHdfObjectsOfFile();
				}
			} else if (m_overwriteHdfFile && Hdf5File.existsHdfFile(m_filePath)) {
				setHdfObject(Hdf5File.openFile(m_filePath, Hdf5File.READ_WRITE_ACCESS));
				createBackup();
				preparationSuccess = deleteAction();
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
		boolean success = preparationSuccess && doAction(inputTable, flowVariables, saveColumnProperties, exec, pTD);
		System.out.println("TotalProgressToDo: " + pTD);
		return success;
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables,
			boolean saveColumnProperties, ExecutionContext exec, long totalProgressToDo) throws IOException {
		setHdfObject(Hdf5File.createFile(m_filePath));
		return getHdfObject() != null;
	}

	@Override
	protected boolean copyAction(ExecutionContext exec, long totalProgressToDo) {
		return false;
	}

	@Override
	protected boolean deleteAction() throws IOException {
		Hdf5File file = (Hdf5File) getHdfObject();
		if (file.deleteFile()) {
			setHdfObject((Hdf5File) null);
		}
		
		return getHdfObject() == null;
	}

	@Override
	protected boolean modifyAction(BufferedDataTable inputTable, boolean saveColumnProperties,
			ExecutionContext exec, long totalProgressToDo) {
		return false;
	}
	
	public boolean deleteAllBackups() {
		boolean success = true;
		
		for (TreeNodeEdit edit : getAllDecendants()) {
			success &= edit.deleteBackup();
		}
		
		return success;
	}
	
	public boolean doRollbackAction() {
		boolean success = true;
		
		if (getEditAction().isCreateOrCopyAction()) {
			if (Hdf5File.existsHdfFile(m_filePath)) {
				try {
					success &= deleteAction();
					if (getHdfBackup() != null) {
						setHdfObject(((Hdf5File) getHdfBackup()).copyFile(m_filePath));
						success &= getHdfObject() != null;
					}
				} catch (IOException ioe) {
					success = false;
					NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
					
				} finally {
					setEditState(success ? EditState.ROLLBACK_SUCCESS : EditState.ROLLBACK_FAIL);
				}
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
						|| edit.getEditState() == EditState.FAIL && edit.getEditAction().isCreateOrCopyAction() /*&& edit.getHdfObject() == null*/) {
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
							success &= edit.deleteAction();
							
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
}

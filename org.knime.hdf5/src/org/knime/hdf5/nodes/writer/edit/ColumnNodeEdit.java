package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;

public class ColumnNodeEdit extends TreeNodeEdit {

	public static final long UNKNOWN_ROW_COUNT = -1;
	
	private static final int NO_COLUMN_INDEX = -1;

	private final int m_inputColumnIndex;
	
	private final HdfDataType m_inputType;
	
	private long m_inputRowCount;
	
	private ColumnNodeEdit(DataSetNodeEdit parent, ColumnNodeEdit copyColumn, boolean noAction) {
		this(parent, copyColumn.getInputPathFromFileWithName(), copyColumn.getInputColumnIndex(),
				copyColumn.getName(), copyColumn.getInputType(), copyColumn.getInputRowCount(), noAction ? copyColumn.getEditAction()
				: (copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY));
		copyAdditionalPropertiesFrom(copyColumn);
		if (getEditAction() == EditAction.COPY) {
			copyColumn.getParent().addIncompleteCopy(parent);
		}
	}

	public ColumnNodeEdit(DataSetNodeEdit parent, DataColumnSpec columnSpec) {
		this(parent, columnSpec.getName(), NO_COLUMN_INDEX, columnSpec.getName(), HdfDataType.getHdfDataType(columnSpec.getType()),
				UNKNOWN_ROW_COUNT, EditAction.CREATE);
		if (parent.getEditAction() == EditAction.COPY) {
			parent.setEditAction(EditAction.CREATE);
		}
	}
	
	ColumnNodeEdit(DataSetNodeEdit parent, int inputColumnIndex, String name, HdfDataType inputType, long inputRowCount) {
		this(parent, parent.getInputPathFromFileWithName(), inputColumnIndex, name, inputType, inputRowCount, EditAction.NO_ACTION);
	}
	
	ColumnNodeEdit(DataSetNodeEdit parent, String inputPathFromFileWithName, int inputColumnIndex, String name, HdfDataType inputType,
			long inputRowCount, EditAction editAction) {
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, EditOverwritePolicy.NONE, editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_inputColumnIndex = inputColumnIndex;
		m_inputType = inputType;
		m_inputRowCount = inputRowCount;
		parent.addColumnNodeEdit(this);
	}
	
	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent, boolean copyWithoutChildren) {
		ColumnNodeEdit newColumnEdit = new ColumnNodeEdit(parent, this, copyWithoutChildren);
		newColumnEdit.addEditToParentNode();
		
		return newColumnEdit;
	}
	
	int getInputColumnIndex() {
		return m_inputColumnIndex;
	}

	public HdfDataType getInputType() {
		return m_inputType;
	}

	long getInputRowCount() {
		return m_inputRowCount;
	}
	
	void setInputRowCount(long inputRowCount) {
		if (m_inputRowCount == UNKNOWN_ROW_COUNT) {
			m_inputRowCount = inputRowCount;
			if (((DataSetNodeEdit) getParent()).getInputRowCount() == UNKNOWN_ROW_COUNT) {
				((DataSetNodeEdit) getParent()).setInputRowCount(inputRowCount);
			}
		}
	}
	
	@Override
	protected void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit) {
		// nothing to do here
	}
	
	@Override
	public String getToolTipText() {
		return "(" + m_inputType.toString() + ") " + super.getToolTipText();
	}
	
	@Override
	protected int getProgressToDoInEdit() {
		return getEditAction() != EditAction.NO_ACTION && getEditAction() != EditAction.MODIFY_CHILDREN_ONLY && getEditState() != EditState.SUCCESS ? 1 : 0;
	}
	
	@Override
	void setDeletion(boolean isDelete) {
		if (isDelete) {
			((DataSetNodeEdit) getParent()).disconsiderColumnNodeEdit(this);
		}
		super.setDeletion(isDelete);
		if (!isDelete) {
			((DataSetNodeEdit) getParent()).considerColumnNodeEdit(this);
		}
	}

	@Override
	protected TreeNodeEdit[] getAllChildren() {
		return new TreeNodeEdit[0];
	}
	
	@Override
	protected void removeFromParent() {
		((DataSetNodeEdit) getParent()).removeColumnNodeEdit(this);
		setParent(null);
	}
	
	@Override
	public void saveSettingsTo(NodeSettingsWO settings) {
		super.saveSettingsTo(settings);

		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		settings.addLong(SettingsKey.INPUT_ROW_COUNT.getKey(), m_inputRowCount);
		settings.addInt(SettingsKey.INPUT_COLUMN_INDEX.getKey(), m_inputColumnIndex);
	}
	
	@Override
	protected void loadSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		// nothing to do here
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		InvalidCause cause = getEditAction() == EditAction.DELETE || m_inputRowCount == UNKNOWN_ROW_COUNT
				|| m_inputRowCount == ((DataSetNodeEdit) getParent()).getInputRowCount() ? null : InvalidCause.ROW_COUNT;
		
		if (cause == null) {
			if (getEditAction() != EditAction.CREATE || inputTable != null) {
				Object[] values = null;

				EditDataType parentDataType = ((DataSetNodeEdit) getParent()).getEditDataType();
				if (getEditAction() == EditAction.CREATE) {
					try {
						DataTableSpec tableSpec = inputTable.getDataTableSpec();
						int columnIndex = tableSpec.findColumnIndex(getInputPathFromFileWithName());
						Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(parentDataType.getOutputType(), true);
						values = knimeType.createArray((int) inputTable.size());
						Object standardValue = parentDataType.getStandardValue();
						CloseableRowIterator iter = inputTable.iterator();
						int rowIndex = 0;
						while (iter.hasNext()) {
							DataRow row = iter.next();
							Object value = knimeType.getValueFromDataCell(row.getCell(columnIndex));
							if (value == null) {
								if (standardValue != null) {
									value = standardValue;
								} else {
									cause = cause == null ? InvalidCause.MISSING_VALUES : cause;
									break;
								}
							}
							values[rowIndex] = value;
							rowIndex++;
						}
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger(getClass()).error("Validation of dataType of new column \""
								+ getOutputPathFromFileWithName() +  "\" could not be checked: " + udte.getMessage(), udte);
						values = null;
					}
				} else {
					try {
						Hdf5DataSet<?> dataSet = ((Hdf5File) getRoot().getHdfObject()).getDataSetByPath(getInputPathFromFileWithName());
						// only supported for dataSets with max. 2 dimensions
						values = dataSet.readColumn(dataSet.getDimensions().length > 1 ? new long[] { getInputColumnIndex() } : new long[0]);
						
					} catch (IOException | HDF5DataspaceInterfaceException ioe) {
						cause = cause == null ? InvalidCause.NO_HDF_OBJECT : cause;
					}
				}
				
				if (values != null && cause == null) {
					cause = !parentDataType.getOutputType().areValuesConvertible(values, m_inputType, parentDataType) ? InvalidCause.DATA_TYPE : cause;
				}
			}
		}
		
		return cause;
	}
	
	public boolean isMaybeInvalid() {
		EditDataType parentDataType = ((DataSetNodeEdit) getParent()).getEditDataType();
		return !m_inputType.getAlwaysConvertibleHdfTypes().contains(parentDataType.getOutputType())
				|| !parentDataType.getOutputType().isNumber() && parentDataType.isFixed();
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return false;
	}

	@Override
	protected boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		return false;
	}
	
	@Override
	protected boolean copyAction() {
		return false;
	}

	@Override
	protected boolean deleteAction() {
		return false;
	}

	@Override
	protected boolean modifyAction() {
		return false;
	}
	
	public class ColumnNodeMenu extends TreeNodeMenu {

		private static final long serialVersionUID = 7696321716083384515L;
    	
		private ColumnNodeMenu() {
			super(false, false, true);
    	}

		@Override
		protected void onDelete() {
			ColumnNodeEdit edit = ColumnNodeEdit.this;
			TreeNodeEdit parentOfVisible = edit.getParent();
			edit.setDeletion(edit.getEditAction() != EditAction.DELETE);

        	DataSetNodeEdit parentEdit = (DataSetNodeEdit) parentOfVisible;
        	if (parentEdit.getTreeNode().getChildCount() == 0) {
        		parentOfVisible = parentEdit.getParent();
				parentEdit.setDeletion(true);
        	}

            parentOfVisible.reloadTreeWithEditVisible(true);
		}
	}
}

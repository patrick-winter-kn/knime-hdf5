package org.knime.hdf5.nodes.writer.edit;

import java.io.IOException;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
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

public class ColumnNodeEdit extends TreeNodeEdit {

	public static final long UNKNOWN_ROW_COUNT = -1;
	
	private static final int NO_COLUMN_INDEX = -1;

	private final int m_inputColumnIndex;
	
	private final HdfDataType m_inputType;
	
	private long m_inputRowCount;
	
	private ColumnNodeEdit(DataSetNodeEdit parent, ColumnNodeEdit copyColumn) {
		this(parent, copyColumn.getInputPathFromFileWithName(), copyColumn.getInputColumnIndex(),
				copyColumn.getName(), copyColumn.getInputType(), copyColumn.getInputRowCount(),
				copyColumn.getEditAction() == EditAction.CREATE ? EditAction.CREATE : EditAction.COPY);
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
		super(inputPathFromFileWithName, parent.getOutputPathFromFileWithName(), name, editAction);
		setTreeNodeMenu(new ColumnNodeMenu());
		m_inputColumnIndex = inputColumnIndex;
		m_inputType = inputType;
		m_inputRowCount = inputRowCount;
		parent.addColumnNodeEdit(this);
	}
	
	public ColumnNodeEdit copyColumnEditTo(DataSetNodeEdit parent) {
		return new ColumnNodeEdit(parent, this);
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
	protected TreeNodeEdit[] getAllChildren() {
		return new TreeNodeEdit[0];
	}
	
	@Override
	protected void removeFromParent() {
		((DataSetNodeEdit) getParent()).removeColumnNodeEdit(this);
		setParent(null);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);

		settings.addInt(SettingsKey.INPUT_TYPE.getKey(), m_inputType.getTypeId());
		settings.addLong(SettingsKey.INPUT_ROW_COUNT.getKey(), m_inputRowCount);
		settings.addInt(SettingsKey.INPUT_COLUMN_INDEX.getKey(), m_inputColumnIndex);
	}
	
	@Override
	protected void loadSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		// nothing to do here
	}
	
	@Override
	protected InvalidCause validateEditInternal(BufferedDataTable inputTable) {
		InvalidCause cause = m_inputRowCount == UNKNOWN_ROW_COUNT || m_inputRowCount == ((DataSetNodeEdit) getParent()).getInputRowCount()
				? null : InvalidCause.ROW_COUNT;
		
		if (cause == null) {
			DataSetNodeEdit parent = (DataSetNodeEdit) getParent();
			
			if (getEditAction() != EditAction.CREATE || inputTable != null) {
				Object[] values = null;
				
				if (getEditAction() == EditAction.CREATE) {
					try {
						DataTableSpec tableSpec = inputTable.getDataTableSpec();
						int columnIndex = tableSpec.findColumnIndex(getInputPathFromFileWithName());
						values = Hdf5KnimeDataType.getKnimeDataType(tableSpec.getColumnSpec(columnIndex).getType()).createArray((int) inputTable.size());
						CloseableRowIterator iter = inputTable.iterator();
						int rowIndex = 0;
						while (iter.hasNext()) {
							DataRow row = iter.next();
							DataCell cell = row.getCell(columnIndex);
							values[rowIndex] = getValueFromDataCell(cell);
							rowIndex++;
						}
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger(getClass()).error("Validation of dataType of new column \""
								+ getOutputPathFromFileWithName() +  "\" could not be checked: " + udte.getMessage(), udte);
					}
				} else {
					try {
						Hdf5DataSet<?> dataSet = ((Hdf5File) getRoot().getHdfObject()).getDataSetByPath(getInputPathFromFileWithName());
						// only supported for dataSets with max. 2 dimensions
						values = dataSet.readColumn(dataSet.getDimensions().length <= 1 ? new long[0] : new long[] { getInputColumnIndex() });
						
					} catch (IOException ioe) {
						NodeLogger.getLogger(getClass()).error("Validation of dataType of new column \""
								+ getOutputPathFromFileWithName() +  "\" could not be checked: " + ioe.getMessage(), ioe);
					}
				}
				
				Integer stringLength = parent.isFixed() ? parent.getStringLength() : HdfDataType.AUTO_STRING_LENGTH;
				cause = parent.getOutputType().areValuesConvertible(values, m_inputType, stringLength) ? cause : InvalidCause.DATA_TYPE;
				if (!parent.isFixed() && stringLength != HdfDataType.AUTO_STRING_LENGTH) {
					parent.setStringLength(stringLength);
				}
			}
		}
		
		return cause;
	}
	
	private static Object getValueFromDataCell(DataCell dataCell) {
		DataType type = dataCell.getType();
		if (type.equals(IntCell.TYPE)) {	
			return (Integer) ((IntCell) dataCell).getIntValue();
		} else if (type.equals(LongCell.TYPE)) {	
			return (Long) ((LongCell) dataCell).getLongValue();
		} else if (type.equals(DoubleCell.TYPE)) {	
			return (Double) ((DoubleCell) dataCell).getDoubleValue();
		} else {
			return dataCell.toString();
		}
	}
	
	public boolean isMaybeInvalid() {
		DataSetNodeEdit parent = (DataSetNodeEdit) getParent();
		return !m_inputType.getAlwaysConvertibleHdfTypes().contains(parent.getOutputType()) || !parent.getOutputType().isNumber() && parent.isFixed();
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

package org.knime.hdf5.nodes.writer.edit;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

public class ColumnNodeEdit extends TreeNodeEdit {

	private final DataColumnSpec m_columnSpec;

	public ColumnNodeEdit(DataColumnSpec columnSpec) throws UnsupportedDataTypeException {
		super(columnSpec.getName());
		m_columnSpec = columnSpec;
	}

	DataColumnSpec getColumnSpec() {
		return m_columnSpec;
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		settings.addDataType("columnSpec", m_columnSpec.getType());
	}

	public static ColumnNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		ColumnNodeEdit edit = null;
		
		try {
			edit = new ColumnNodeEdit(new DataColumnSpecCreator(settings.getString("name"), settings.getDataType("columnSpec")).createSpec());
		
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
		
		return edit;
	}
	
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		parentNode.add(new DefaultMutableTreeNode(this));
	}
}

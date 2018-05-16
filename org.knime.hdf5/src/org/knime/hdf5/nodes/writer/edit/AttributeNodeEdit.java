package org.knime.hdf5.nodes.writer.edit;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class AttributeNodeEdit extends TreeNodeEdit {

	private final Hdf5KnimeDataType m_knimeType;
	
	private Hdf5HdfDataType m_hdfType;

	public AttributeNodeEdit(DefaultMutableTreeNode parent, FlowVariable var) {
		super(parent, var.getName());
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
	}

	public AttributeNodeEdit(FlowVariable var) {
		super(var.getName());
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(var.getType());
	}

	private AttributeNodeEdit(String pathFromFile, String name, Hdf5KnimeDataType knimetype) {
		super(pathFromFile, name);
		m_knimeType = knimetype;
	}
	
	private AttributeNodeEdit(String name, Hdf5KnimeDataType knimetype) {
		super(name);
		m_knimeType = knimetype;
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}

	public Hdf5HdfDataType getHdfType() {
		return m_hdfType;
	}

	private void setHdfType(Hdf5HdfDataType hdfType) {
		m_hdfType = hdfType;
	}

	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		try {
			settings.addDataType("knimeType", m_knimeType.getColumnDataType());
		} catch (UnsupportedDataTypeException udte) {
			// TODO exception
		}
	}

	public static AttributeNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			return new AttributeNodeEdit(settings.getString("pathFromFile"), settings.getString("name"), Hdf5KnimeDataType.getKnimeDataType(settings.getDataType("knimeType")));
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	public static AttributeNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		try {
			return new AttributeNodeEdit(settings.getString("name"), Hdf5KnimeDataType.getKnimeDataType(settings.getDataType("knimeType")));
		} catch (UnsupportedDataTypeException udte) {
			throw new InvalidSettingsException(udte.getMessage());
		}
	}
	
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		parentNode.add(new DefaultMutableTreeNode(this));
	}
}

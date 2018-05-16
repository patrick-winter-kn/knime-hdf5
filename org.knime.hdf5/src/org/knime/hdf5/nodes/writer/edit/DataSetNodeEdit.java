package org.knime.hdf5.nodes.writer.edit;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

public class DataSetNodeEdit extends TreeNodeEdit {

	private Hdf5KnimeDataType m_knimeType;
	
	private Hdf5HdfDataType m_hdfType;
	
	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();
	
	private final List<DataColumnSpec> m_columnSpecs = new ArrayList<>();

	public DataSetNodeEdit(DefaultMutableTreeNode parent, String name) {
		super(parent, name);
	}

	public DataSetNodeEdit(String name) {
		super(name);
	}
	
	private DataSetNodeEdit(String pathFromFile, String name) {
		super(pathFromFile, name);
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}

	public Hdf5HdfDataType getHdfType() {
		return m_hdfType;
	}
	
	public AttributeNodeEdit[] getAttributeNodeEdits() {
		return m_attributeEdits.toArray(new AttributeNodeEdit[] {});
	}
	
	public ColumnNodeEdit[] getColumnNodeEdits() {
		return m_columnEdits.toArray(new ColumnNodeEdit[] {});
	}	
	
	public DataColumnSpec[] getColumnSpecs() {
		return m_columnSpecs.toArray(new DataColumnSpec[] {});
	}

	private void setHdfType(Hdf5HdfDataType hdfType) {
		m_hdfType = hdfType;
	}

	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
	}

	public void addColumnNodeEdit(ColumnNodeEdit edit) throws UnsupportedDataTypeException {
		DataColumnSpec spec = edit.getColumnSpec();
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(spec.getType());
		m_columnSpecs.add(spec);
		m_columnEdits.add(edit);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
	    NodeSettingsWO attributeSettings = settings.addNodeSettings("attributes");
	    NodeSettingsWO columnSettings = settings.addNodeSettings("columns");
	    
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        NodeSettingsWO editSettings = attributeSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        NodeSettingsWO editSettings = columnSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
	}

	@SuppressWarnings("unchecked")
	public static DataSetNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString("pathFromFile"), settings.getString("name"));
		
		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
		Enumeration<NodeSettingsRO> columnEnum = columnSettings.children();
		while (columnEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = columnEnum.nextElement();
			try {
				edit.addColumnNodeEdit(ColumnNodeEdit.getEditFromSettings(editSettings));
				
			} catch (UnsupportedDataTypeException udte) {
				throw new InvalidSettingsException(udte.getMessage());
			}
		}
		
		return edit;
	}

	@SuppressWarnings("unchecked")
	public static DataSetNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString("name"));

		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
		Enumeration<NodeSettingsRO> columnEnum = columnSettings.children();
		while (columnEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = columnEnum.nextElement();
			try {
				edit.addColumnNodeEdit(ColumnNodeEdit.getEditFromSettings(editSettings));
				
			} catch (UnsupportedDataTypeException udte) {
				throw new InvalidSettingsException(udte.getMessage());
			}
		}
		
		return edit;
	}

	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		parentNode.add(node);
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        edit.addEditToNode(node);
		}
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        edit.addEditToNode(node);
		}
	}
}

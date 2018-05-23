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
	
	/**
	 * only those specs that are already physically existing
	 */
	private final List<DataColumnSpec> m_columnSpecs = new ArrayList<>();
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();

	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
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
	
	/**
	 * Returns the specs of m_columnEdits and m_columnSpecs.
	 * 
	 * @return 
	 */
	public DataColumnSpec[] getColumnSpecs() {
		List<DataColumnSpec> specs = new ArrayList<>();
		specs.addAll(m_columnSpecs);
		for (ColumnNodeEdit edit : m_columnEdits) {
			specs.add(edit.getColumnSpec());
		}
		return specs.toArray(new DataColumnSpec[] {});
	}
	
	public ColumnNodeEdit[] getColumnNodeEdits() {
		return m_columnEdits.toArray(new ColumnNodeEdit[] {});
	}	

	public AttributeNodeEdit[] getAttributeNodeEdits() {
		return m_attributeEdits.toArray(new AttributeNodeEdit[] {});
	}
	
	private void setHdfType(Hdf5HdfDataType hdfType) {
		m_hdfType = hdfType;
	}

	public void addColumnNodeEdit(ColumnNodeEdit edit) throws UnsupportedDataTypeException {
		DataColumnSpec spec = edit.getColumnSpec();
		m_knimeType = Hdf5KnimeDataType.getKnimeDataType(spec.getType());
		m_columnEdits.add(edit);
	}

	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
	}

	public void removeColumnNodeEdit(ColumnNodeEdit edit) {
		DataColumnSpec spec = edit.getColumnSpec();
		// TODO update m_knimeType
		
		m_columnEdits.remove(edit);
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
	    NodeSettingsWO columnSettings = settings.addNodeSettings("columns");
	    NodeSettingsWO attributeSettings = settings.addNodeSettings("attributes");
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        NodeSettingsWO editSettings = columnSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
	    
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        NodeSettingsWO editSettings = attributeSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
	}

	@SuppressWarnings("unchecked")
	public static DataSetNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString("pathFromFile"), settings.getString("name"));
		
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
		
		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		return edit;
	}

	@SuppressWarnings("unchecked")
	public static DataSetNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString("name"));
		
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

		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			edit.addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
		
		return edit;
	}

	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		parentNode.add(node);
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        edit.addEditToNode(node);
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        edit.addEditToNode(node);
		}
	}
}

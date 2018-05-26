package org.knime.hdf5.nodes.writer.edit;

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class DataSetNodeEdit extends TreeNodeEdit {

	public static final DataSetNodeMenu DATASET_EDIT_MENU = new DataSetNodeMenu(true);
	
	public static final DataSetNodeMenu DATASET_MENU = new DataSetNodeMenu(false);
	
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
		Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(spec.getType());
		// TODO check if the possible types need to be changed
		m_knimeType = knimeType.getConvertibleTypes().contains(m_knimeType) ? m_knimeType : knimeType;
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
	
	public static class DataSetNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = -6418394582185524L;

		private static DataSetPropertiesDialog propertiesDialog;
    	
    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private DataSetNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit dataSet properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (propertiesDialog == null) {
							propertiesDialog = new DataSetPropertiesDialog("DataSet properties");
						}
						
						propertiesDialog.initPropertyItems((DataSetNodeEdit) m_node.getUserObject());
						propertiesDialog.setVisible(true);
					}
				});
	    		add(itemEdit);
    		}
    		
    		if (fromTreeNodeEdit) {
        		JMenuItem itemDelete = new JMenuItem("Delete dataSet");
        		itemDelete.addActionListener(new ActionListener() {
    				
    				@Override
    				public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof DataSetNodeEdit) {
							DataSetNodeEdit edit = (DataSetNodeEdit) userObject;
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
	                    	Object parentObject = parent.getUserObject();
	                    	if (parentObject instanceof GroupNodeEdit) {
	    						((GroupNodeEdit) parentObject).removeDataSetNodeEdit(edit);
	                    		
	                		} else {
		                    	m_editTreeConfig.removeDataSetNodeEdit(edit);
	                		}
	                    	parent.remove(m_node);
	        				((DefaultTreeModel) (m_tree.getModel())).reload();
	        				m_tree.makeVisible(new TreePath(parent.getPath()));
						}
    				}
    			});
        		add(itemDelete);
    		}
    	}
		
		public void initMenu(JTree tree, EditTreeConfiguration editTreeConfig, DefaultMutableTreeNode node) {
			m_tree = tree;
			m_editTreeConfig = editTreeConfig;
			m_node = node;
		}
		
		private class DataSetPropertiesDialog extends PropertiesDialog<DataSetNodeEdit> {
	    	
	    	private static final long serialVersionUID = 1254593831386973543L;
	    	
			private JTextField m_nameField = new JTextField(15);
	    	
			private DataSetPropertiesDialog(String title) {
				// TODO the owner of the frame should be the whole dialog
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(300, 400));

				addProperty("Name: ", m_nameField, false);

				JComboBox<HdfDataType> typeField = new JComboBox<>(((DataSetNodeEdit) m_node.getUserObject()).getKnimeType().getConvertibleTypes().toArray(new HdfDataType[] {}));
				addProperty("Type: ", typeField, false);

				JComboBox<String> endianField = new JComboBox<>(new String[] {"little endian", "big endian"});
				addProperty("Endian: ", endianField, false);
				
				JPanel sizeField = new JPanel();
				ButtonGroup sizeGroup = new ButtonGroup();
				JRadioButton auto = new JRadioButton("auto");
				sizeField.add(auto);
				sizeGroup.add(auto);
				auto.setSelected(true);
				JRadioButton fixed = new JRadioButton("fixed");
				sizeField.add(fixed);
				sizeGroup.add(fixed);
				JSpinner size = new JSpinner();
				sizeField.add(size);
				size.setEnabled(false);
				fixed.addChangeListener(new ChangeListener() {
					
					@Override
					public void stateChanged(ChangeEvent e) {
						size.setEnabled(fixed.isSelected());
					}
				});
				addProperty("Size: ", sizeField, false);

				JSpinner compressionField = new JSpinner();
				addProperty("Compression: ", compressionField, true);

				JSpinner chunkField = new JSpinner();
				addProperty("Chunk row size: ", chunkField, false);
				
				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				JRadioButton no = new JRadioButton("no");
				overwriteField.add(no);
				overwriteGroup.add(no);
				no.setSelected(true);
				JRadioButton yes = new JRadioButton("yes");
				overwriteField.add(yes);
				overwriteGroup.add(yes);
				JRadioButton append = new JRadioButton("append");
				overwriteField.add(append);
				overwriteGroup.add(append);
				addProperty("Overwrite: ", overwriteField, false);
				
				JList<DataColumnSpec> columnField = new JList<>(((DataSetNodeEdit) m_node.getUserObject()).getColumnSpecs());
				addProperty("Columns: ", columnField, false);
			}
			
			@Override
			protected void initPropertyItems(DataSetNodeEdit edit) {
				m_nameField.setText(edit.getName());
			}

			@Override
			protected void editPropertyItems() {
				TreeNodeEdit edit = (TreeNodeEdit) m_node.getUserObject();
				edit.setName(m_nameField.getText());
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(m_node.getPath()));
			}
		}
    }
}

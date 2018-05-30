package org.knime.hdf5.nodes.writer.edit;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DropMode;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
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
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;

public class DataSetNodeEdit extends TreeNodeEdit {

	private final DataSetNodeMenu m_dataSetNodeMenu;
	
	private Hdf5KnimeDataType m_knimeType;
	
	private HdfDataType m_hdfType;

	// TODO use an enum here
	private boolean m_littleEndian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private int m_compression;
	
	private int m_chunkRowSize;

	// TODO use an enum here; also include "append" here
	private boolean m_overwrite;
	
	/**
	 * only those specs that are already physically existing
	 */
	private final List<DataColumnSpec> m_columnSpecs = new ArrayList<>();
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();

	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	public DataSetNodeEdit(DefaultMutableTreeNode parent, String name) {
		super(parent, name);
		m_dataSetNodeMenu = new DataSetNodeMenu(true);
	}

	public DataSetNodeEdit(String name) {
		super(name);
		m_dataSetNodeMenu = new DataSetNodeMenu(true);
	}
	
	private DataSetNodeEdit(String pathFromFile, String name) {
		super(pathFromFile, name);
		m_dataSetNodeMenu = new DataSetNodeMenu(true);
	}
	
	public DataSetNodeMenu getDataSetEditMenu() {
		return m_dataSetNodeMenu;
	}

	public Hdf5KnimeDataType getKnimeType() {
		return m_knimeType;
	}

	public HdfDataType getHdfType() {
		return m_hdfType;
	}

	private void setHdfType(HdfDataType hdfType) {
		m_hdfType = hdfType;
	}

	public boolean isLittleEndian() {
		return m_littleEndian;
	}

	private void setLittleEndian(boolean littleEndian) {
		m_littleEndian = littleEndian;
	}

	public boolean isFixed() {
		return m_fixed;
	}

	private void setFixed(boolean fixed) {
		m_fixed = fixed;
	}

	public int getStringLength() {
		return m_stringLength;
	}

	private void setStringLength(int stringLength) {
		m_stringLength = stringLength;
	}

	public int getCompression() {
		return m_compression;
	}

	private void setCompression(int compression) {
		m_compression = compression;
	}

	public int getChunkRowSize() {
		return m_chunkRowSize;
	}

	private void setChunkRowSize(int chunkRowSize) {
		m_chunkRowSize = chunkRowSize;
	}

	public boolean isOverwrite() {
		return m_overwrite;
	}

	private void setOverwrite(boolean overwrite) {
		m_overwrite = overwrite;
	}
	
	/**
	 * Returns the specs of m_columnEdits and m_columnSpecs.
	 * 
	 * @return 
	 */
	public DataColumnSpec[] getColumnSpecs() {
		// TODO use the correct order based on possible changes made in the Dialog
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

	public void addColumnNodeEdit(ColumnNodeEdit edit) throws UnsupportedDataTypeException {
		Hdf5KnimeDataType knimeType = Hdf5KnimeDataType.getKnimeDataType(edit.getColumnSpec().getType());
		// TODO check if the possible types need to be changed
		m_knimeType = m_knimeType != null && knimeType.getConvertibleHdfTypes().contains(m_knimeType.getEquivalentHdfType()) ? m_knimeType : knimeType;
		m_hdfType = m_knimeType.getConvertibleHdfTypes().contains(m_hdfType) ? m_hdfType : m_knimeType.getEquivalentHdfType();
		m_columnEdits.add(edit);
	}

	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
	}

	public void removeColumnNodeEdit(ColumnNodeEdit edit) {
		m_columnEdits.remove(edit);

		// TODO find better algorithm here
		try {
			for (Hdf5KnimeDataType knimeType : Hdf5KnimeDataType.values()) {
				boolean convertible = true;
				for (DataColumnSpec spec : getColumnSpecs()) {
					Hdf5KnimeDataType specKnimeType = Hdf5KnimeDataType.getKnimeDataType(spec.getType());
					if (!specKnimeType.getConvertibleHdfTypes().contains(knimeType.getEquivalentHdfType())) {
						convertible = false;
						break;
					}
				}
				if (convertible) {
					m_knimeType = knimeType;
					break;
				}
			}
		} catch (UnsupportedDataTypeException udte) {
			NodeLogger.getLogger(getClass()).warn(udte.getMessage(), udte);
		}
	}
	
	public void removeAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.remove(edit);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		try {
			settings.addDataType("knimeType", m_knimeType.getColumnDataType());
		} catch (UnsupportedDataTypeException udte) {
			settings.addDataType("knimeType", null);
		}
		
		settings.addString("hdfType", m_hdfType.toString());
		settings.addBoolean("littleEndian", m_littleEndian);
		settings.addBoolean("fixed", m_fixed);
		settings.addInt("stringLength", m_stringLength);
		settings.addInt("compression", m_compression);
		settings.addInt("chunkRowSize", m_chunkRowSize);
		settings.addBoolean("overwrite", m_overwrite);
		// TODO add order of specs
		
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

	public static DataSetNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString("pathFromFile"), settings.getString("name"));

		edit.loadProperties(settings);
		
		return edit;
	}

	public static DataSetNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString("name"));
		
		edit.loadProperties(settings);
		
		return edit;
	}
	
	@SuppressWarnings("unchecked")
	private void loadProperties(final NodeSettingsRO settings) throws InvalidSettingsException {
		setHdfType(HdfDataType.valueOf(settings.getString("hdfType")));
		setLittleEndian(settings.getBoolean("littleEndian"));
		setFixed(settings.getBoolean("fixed"));
		setStringLength(settings.getInt("stringLength"));
		setStringLength(settings.getInt("compression"));
		setStringLength(settings.getInt("chunkRowSize"));
		setOverwrite(settings.getBoolean("overwrite"));
		
		NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
		Enumeration<NodeSettingsRO> columnEnum = columnSettings.children();
		while (columnEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = columnEnum.nextElement();
			try {
				addColumnNodeEdit(ColumnNodeEdit.getEditFromSettings(editSettings));
				
			} catch (UnsupportedDataTypeException udte) {
				throw new InvalidSettingsException(udte.getMessage());
			}
		}

		NodeSettingsRO attributeSettings = settings.getNodeSettings("attributes");
		Enumeration<NodeSettingsRO> attributeEnum = attributeSettings.children();
		while (attributeEnum.hasMoreElements()) {
			NodeSettingsRO editSettings = attributeEnum.nextElement();
			addAttributeNodeEdit(AttributeNodeEdit.getEditFromSettings(editSettings));
        }
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
	
	public class DataSetNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = -6418394582185524L;

		private DataSetPropertiesDialog m_propertiesDialog;
    	
    	private JTree m_tree;
    	
    	private EditTreeConfiguration m_editTreeConfig;
    	
    	private DefaultMutableTreeNode m_node;
    	
		private DataSetNodeMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit dataSet properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (m_propertiesDialog == null) {
							m_propertiesDialog = new DataSetPropertiesDialog("DataSet properties");
						}
						
						m_propertiesDialog.initPropertyItems((DataSetNodeEdit) m_node.getUserObject());
						m_propertiesDialog.setVisible(true);
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
	    	
			private static final long serialVersionUID = -9060929832634560737L;

			private JTextField m_nameField = new JTextField(15);
			private JComboBox<HdfDataType> m_typeField = new JComboBox<>();
			// TODO this should also be an enum
			private JComboBox<String> m_endianField = new JComboBox<>(new String[] {"little endian", "big endian"});
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner();
			private JSpinner m_compressionField = new JSpinner();
			private JSpinner m_chunkField = new JSpinner();
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			// TODO implement option to append
			private JRadioButton m_overwriteAppend = new JRadioButton("append");
			private JList<DataColumnSpec> m_columnField = new JList<>(getColumnSpecs());
	    	
			private DataSetPropertiesDialog(String title) {
				// TODO the owner of the frame should be the whole dialog
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(300, 400));

				addProperty("Name: ", m_nameField, false);
				addProperty("Type: ", m_typeField, false);
				addProperty("Endian: ", m_endianField, false);
				
				JPanel stringLengthField = new JPanel();
				ButtonGroup stringLengthGroup = new ButtonGroup();
				stringLengthField.add(m_stringLengthAuto);
				stringLengthGroup.add(m_stringLengthAuto);
				m_stringLengthAuto.setSelected(true);
				stringLengthField.add(m_stringLengthFixed);
				stringLengthGroup.add(m_stringLengthFixed);
				stringLengthField.add(m_stringLengthSpinner);
				m_stringLengthSpinner.setEnabled(false);
				m_stringLengthFixed.addChangeListener(new ChangeListener() {
					
					@Override
					public void stateChanged(ChangeEvent e) {
						m_stringLengthSpinner.setEnabled(m_stringLengthFixed.isSelected());
					}
				});
				addProperty("String length: ", stringLengthField, false);

				// TODO implement option for compression
				addProperty("Compression: ", m_compressionField, true);
				// TODO implement option for row chunk size
				addProperty("Chunk row size: ", m_chunkField, false);
				
				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				overwriteField.add(m_overwriteNo);
				overwriteGroup.add(m_overwriteNo);
				m_overwriteNo.setSelected(true);
				overwriteField.add(m_overwriteYes);
				overwriteGroup.add(m_overwriteYes);
				overwriteField.add(m_overwriteAppend);
				overwriteGroup.add(m_overwriteAppend);
				addProperty("Overwrite: ", overwriteField, false);
				
				m_columnField.setVisibleRowCount(-1);
				m_columnField.setCellRenderer(new DefaultListCellRenderer() {

					private static final long serialVersionUID = -3499393207598804129L;

					@Override
					public Component getListCellRendererComponent(JList<?> list,
							Object value, int index, boolean isSelected,
							boolean cellHasFocus) {
						super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);

						if (value instanceof DataColumnSpec) {
							DataColumnSpec spec = (DataColumnSpec) value;

							setText(spec.getName());
							setIcon(spec.getType().getIcon());
						}
						
						return this;
					}
				});
				
				m_columnField.setDragEnabled(true);
				/*m_columnField.setTransferHandler(new TransferHandler() {

					private static final long serialVersionUID = 1192996062714565516L;

		            public int getSourceActions(JComponent comp) {
		                return MOVE;
		            }
				});*/
				m_columnField.setDropMode(DropMode.INSERT);
				final JScrollPane jsp = new JScrollPane(m_columnField);
				// try that the gridLayoutConstraints
				//jsp.setMinimumSize(new Dimension(0, 100));
				addProperty("Columns: ", jsp, false);
			}
			
			@Override
			protected void initPropertyItems(DataSetNodeEdit edit) {
				m_nameField.setText(edit.getName());
				m_typeField.setModel(new DefaultComboBoxModel<HdfDataType>(edit.getKnimeType().getConvertibleHdfTypes().toArray(new HdfDataType[] {})));
				if (edit.getHdfType() == null) {
					edit.setHdfType(edit.getKnimeType().getEquivalentHdfType());
				}
				m_typeField.setSelectedItem(edit.getHdfType());
				m_endianField.setSelectedItem(edit.isLittleEndian() ? "little endian" : "big endian");
				m_stringLengthAuto.setSelected(!edit.isFixed());
				m_stringLengthFixed.setSelected(edit.isFixed());
				m_stringLengthSpinner.setValue(edit.getStringLength());
				m_compressionField.setValue(edit.getCompression());
				m_chunkField.setValue(edit.getChunkRowSize());
				m_overwriteNo.setSelected(!edit.isOverwrite());
				m_overwriteYes.setSelected(edit.isOverwrite());
				m_overwriteAppend.setSelected(false);
				m_columnField.setListData(getColumnSpecs());
			}

			@Override
			protected void editPropertyItems() {
				DataSetNodeEdit edit = (DataSetNodeEdit) m_node.getUserObject();
				edit.setName(m_nameField.getText());
				edit.setHdfType((HdfDataType) m_typeField.getSelectedItem());
				edit.setLittleEndian(m_endianField.getSelectedItem().equals("little endian"));
				edit.setFixed(m_stringLengthFixed.isSelected());
				edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				edit.setCompression((Integer) m_compressionField.getValue());
				edit.setChunkRowSize((Integer) m_chunkField.getValue());
				edit.setOverwrite(m_overwriteYes.isSelected());
				// TODO also edit column order here
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(m_node.getPath()));
			}
		}
    }
}

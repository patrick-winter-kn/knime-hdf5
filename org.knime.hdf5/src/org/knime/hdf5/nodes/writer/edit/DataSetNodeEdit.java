package org.knime.hdf5.nodes.writer.edit;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.DropMode;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.TransferHandler;
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
import org.knime.hdf5.lib.types.Hdf5HdfDataType.Endian;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.EditTreeConfiguration;
import org.knime.hdf5.nodes.writer.HDF5OverwritePolicy;

public class DataSetNodeEdit extends TreeNodeEdit {

	private final DataSetNodeMenu m_dataSetNodeMenu;
	
	private Hdf5KnimeDataType m_knimeType;
	
	private HdfDataType m_hdfType;

	private Endian m_endian;
	
	private boolean m_fixed;
	
	private int m_stringLength;
	
	private int m_compression;
	
	private int m_chunkRowSize = 1;

	private HDF5OverwritePolicy m_overwritePolicy = HDF5OverwritePolicy.ABORT;
	
	private final List<ColumnNodeEdit> m_columnEdits = new ArrayList<>();

	private final List<AttributeNodeEdit> m_attributeEdits = new ArrayList<>();
	
	public DataSetNodeEdit(DefaultMutableTreeNode parent, String name) {
		super(parent, name);
		m_dataSetNodeMenu = new DataSetNodeMenu(true);
		m_endian = Endian.LITTLE_ENDIAN;
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

	public Endian getEndian() {
		return m_endian;
	}

	private void setEndian(Endian endian) {
		m_endian = endian;
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

	public HDF5OverwritePolicy getOverwritePolicy() {
		return m_overwritePolicy;
	}

	private void setOverwritePolicy(HDF5OverwritePolicy overwritePolicy) {
		m_overwritePolicy = overwritePolicy;
	}
	
	public DataColumnSpec[] getColumnSpecs() {
		List<DataColumnSpec> specs = new ArrayList<>();
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
		m_knimeType = m_knimeType != null && knimeType.getConvertibleHdfTypes().contains(m_knimeType.getEquivalentHdfType()) ? m_knimeType : knimeType;
		m_hdfType = m_knimeType.getConvertibleHdfTypes().contains(m_hdfType) ? m_hdfType : m_knimeType.getEquivalentHdfType();
		m_columnEdits.add(edit);
		edit.validate();
	}

	public void addAttributeNodeEdit(AttributeNodeEdit edit) {
		m_attributeEdits.add(edit);
		edit.validate();
	}

	public void removeColumnNodeEdit(ColumnNodeEdit edit) {
		m_columnEdits.remove(edit);
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}

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
		if (getTreeNode() != null) {
			getTreeNode().remove(edit.getTreeNode());
		}
	}
	
	private void setColumnEdits(List<ColumnNodeEdit> edits) {
		m_columnEdits.clear();
		m_columnEdits.addAll(edits);
	}
	
	@Override
	public void saveSettings(NodeSettingsWO settings) {
		super.saveSettings(settings);
		
		try {
			settings.addDataType(SettingsKey.KNIME_TYPE.getKey(), m_knimeType.getColumnDataType());
		} catch (UnsupportedDataTypeException udte) {
			settings.addDataType(SettingsKey.KNIME_TYPE.getKey(), null);
		}
		
		settings.addString(SettingsKey.HDF_TYPE.getKey(), m_hdfType.toString());
		settings.addBoolean(SettingsKey.LITTLE_ENDIAN.getKey(), m_endian == Endian.LITTLE_ENDIAN);
		settings.addBoolean(SettingsKey.FIXED.getKey(), m_fixed);
		settings.addInt(SettingsKey.STRING_LENGTH.getKey(), m_stringLength);
		settings.addInt(SettingsKey.COMPRESSION.getKey(), m_compression);
		settings.addInt(SettingsKey.CHUNK_ROW_SIZE.getKey(), m_chunkRowSize);
		settings.addString(SettingsKey.OVERWRITE_POLICY.getKey(), m_overwritePolicy.getName());
		
	    NodeSettingsWO columnSettings = settings.addNodeSettings(SettingsKey.COLUMNS.getKey());
	    NodeSettingsWO attributeSettings = settings.addNodeSettings(SettingsKey.ATTRIBUTES.getKey());
		
		for (int i = 0; i < m_columnEdits.size(); i++) {
			ColumnNodeEdit edit = m_columnEdits.get(i);
	        NodeSettingsWO editSettings = columnSettings.addNodeSettings(i + ": " + edit.getName());
			edit.saveSettings(editSettings);
		}
	    
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        NodeSettingsWO editSettings = attributeSettings.addNodeSettings(edit.getName());
			edit.saveSettings(editSettings);
		}
	}

	public static DataSetNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString(SettingsKey.PATH_FROM_FILE.getKey()), settings.getString(SettingsKey.NAME.getKey()));

		edit.loadProperties(settings);
		
		return edit;
	}

	public static DataSetNodeEdit getEditFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		DataSetNodeEdit edit = new DataSetNodeEdit(settings.getString(SettingsKey.NAME.getKey()));
		
		edit.loadProperties(settings);
		
		return edit;
	}
	
	@SuppressWarnings("unchecked")
	private void loadProperties(final NodeSettingsRO settings) throws InvalidSettingsException {
		setHdfType(HdfDataType.valueOf(settings.getString(SettingsKey.HDF_TYPE.getKey())));
		setEndian(settings.getBoolean(SettingsKey.LITTLE_ENDIAN.getKey()) ? Endian.LITTLE_ENDIAN : Endian.BIG_ENDIAN);
		setFixed(settings.getBoolean(SettingsKey.FIXED.getKey()));
		setStringLength(settings.getInt(SettingsKey.STRING_LENGTH.getKey()));
		setCompression(settings.getInt(SettingsKey.COMPRESSION.getKey()));
		setChunkRowSize(settings.getInt(SettingsKey.CHUNK_ROW_SIZE.getKey()));
		setOverwritePolicy(HDF5OverwritePolicy.get(settings.getString(SettingsKey.OVERWRITE_POLICY.getKey())));
		
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

	@Override
	public void addEditToNode(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode node = new DefaultMutableTreeNode(this);
		parentNode.add(node);
		m_treeNode = node;
		
		for (ColumnNodeEdit edit : m_columnEdits) {
	        edit.addEditToNode(node);
		}
		
		for (AttributeNodeEdit edit : m_attributeEdits) {
	        edit.addEditToNode(node);
		}
	}
	
	@Override
	protected boolean getValidation() {
		return super.getValidation() && !getName().contains("/");
	}

	@Override
	protected boolean isInConflict(TreeNodeEdit edit) {
		return (edit instanceof DataSetNodeEdit || edit instanceof GroupNodeEdit) && !edit.equals(this) && edit.getName().equals(getName());
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
	        				((DefaultTreeModel) (m_tree.getModel())).reload();
	        				TreePath path = new TreePath(parent.getPath());
	        				path = parent.getChildCount() > 0 ? path.pathByAddingChild(parent.getFirstChild()) : path;
	        				m_tree.makeVisible(path);
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
			private JComboBox<Endian> m_endianField = new JComboBox<>(Endian.values());
			private JRadioButton m_stringLengthAuto = new JRadioButton("auto");
			private JRadioButton m_stringLengthFixed = new JRadioButton("fixed");
			private JSpinner m_stringLengthSpinner = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 1));
			private JSpinner m_compressionField = new JSpinner(new SpinnerNumberModel(0, 0, 9, 1));
			private JSpinner m_chunkField = new JSpinner(new SpinnerNumberModel(1, 1, Integer.MAX_VALUE, 1));
			private JRadioButton m_overwriteNo = new JRadioButton("no");
			private JRadioButton m_overwriteYes = new JRadioButton("yes");
			// TODO implement option to insert columns
			private JRadioButton m_overwriteInsert = new JRadioButton("insert");
			private JList<ColumnNodeEdit> m_editList = new JList<>(new DefaultListModel<>());
	    	
			private DataSetPropertiesDialog(String title) {
				super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, m_tree), title);
				setMinimumSize(new Dimension(400, 500));

				addProperty("Name: ", m_nameField);
				
				m_typeField.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						boolean isString = (HdfDataType) m_typeField.getSelectedItem() == HdfDataType.STRING;
						m_endianField.setEnabled(!isString);
						m_stringLengthAuto.setEnabled(isString);
						m_stringLengthFixed.setEnabled(isString);
						m_stringLengthSpinner.setEnabled(isString && m_stringLengthFixed.isSelected());
					}
				});
				
				addProperty("Type: ", m_typeField);
				addProperty("Endian: ", m_endianField);
				
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
				addProperty("String length: ", stringLengthField);

				m_compressionField.setEnabled(false);
				addProperty("Compression: ", m_compressionField, new ChangeListener() {

					@Override
					public void stateChanged(ChangeEvent e) {
						boolean selected = ((JCheckBox) e.getSource()).isSelected();
						m_compressionField.setEnabled(selected);
						m_chunkField.setEnabled(selected);
					}
				});
				m_chunkField.setEnabled(false);
				addProperty("Chunk row size: ", m_chunkField);
				
				JPanel overwriteField = new JPanel();
				ButtonGroup overwriteGroup = new ButtonGroup();
				overwriteField.add(m_overwriteNo);
				overwriteGroup.add(m_overwriteNo);
				m_overwriteNo.setSelected(true);
				overwriteField.add(m_overwriteYes);
				overwriteGroup.add(m_overwriteYes);
				overwriteField.add(m_overwriteInsert);
				overwriteGroup.add(m_overwriteInsert);
				addProperty("Overwrite: ", overwriteField);
				
				DefaultListModel<ColumnNodeEdit> editModel = (DefaultListModel<ColumnNodeEdit>) m_editList.getModel();
				editModel.clear();
				for (ColumnNodeEdit edit : getColumnNodeEdits()) {
					editModel.addElement(edit);
				}
				
				m_editList.setVisibleRowCount(-1);
				m_editList.setCellRenderer(new DefaultListCellRenderer() {

					private static final long serialVersionUID = -3499393207598804129L;

					@Override
					public Component getListCellRendererComponent(JList<?> list,
							Object value, int index, boolean isSelected,
							boolean cellHasFocus) {
						super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);

						if (value instanceof ColumnNodeEdit) {
							DataColumnSpec spec = ((ColumnNodeEdit) value).getColumnSpec();

							setText(spec.getName());
							setIcon(spec.getType().getIcon());
						}
						
						return this;
					}
				});
				
				m_editList.setDragEnabled(true);
				m_editList.setTransferHandler(new TransferHandler() {

					private static final long serialVersionUID = 1192996062714565516L;
					
					private int[] transferableIndices;

		            public int getSourceActions(JComponent comp) {
		                return MOVE;
		            }
		            
		            protected Transferable createTransferable(JComponent comp) {
		                if (comp instanceof JList) {
		                	JList<?> list = (JList<?>) comp;
		                	transferableIndices = list.getSelectedIndices();
		                }
		                return new StringSelection("");
		            }
		            
		            public boolean canImport(TransferHandler.TransferSupport info) {
		            	JList.DropLocation dl = (JList.DropLocation) info.getDropLocation();
		                return dl.getIndex() != -1;
		            }

					public boolean importData(TransferHandler.TransferSupport info) {
		                if (!info.isDrop()) {
		                    return false;
		                }
		                
		                JList.DropLocation dl = (JList.DropLocation) info.getDropLocation();
		                int startIndex = dl.getIndex();
		                int transferableCount = transferableIndices.length;
		                List<ColumnNodeEdit> edits = Collections.list(editModel.elements());
		                for (int i = 0; i < transferableCount; i++) {
			                editModel.add(startIndex + i, edits.get(transferableIndices[i]));
		                }
		                for (int i = transferableCount - 1; i >= 0; i--) {
		                	int index = transferableIndices[i];
		                	index += startIndex > index ? 0 : transferableCount;
		                	editModel.remove(index);
		                }
		                m_editList.setModel(editModel);
		                
		                return true;
		            }
				});
				m_editList.setDropMode(DropMode.INSERT);
				final JScrollPane jsp = new JScrollPane(m_editList);
				addProperty("Columns: ", jsp, 1.0);
			}
			
			@Override
			protected void initPropertyItems(DataSetNodeEdit edit) {
				m_nameField.setText(edit.getName());
				m_typeField.setModel(new DefaultComboBoxModel<HdfDataType>(edit.getKnimeType().getConvertibleHdfTypes().toArray(new HdfDataType[] {})));
				m_typeField.setSelectedItem(edit.getHdfType());
				m_endianField.setSelectedItem(edit.getEndian());
				m_stringLengthAuto.setSelected(!edit.isFixed());
				m_stringLengthFixed.setSelected(edit.isFixed());
				m_stringLengthSpinner.setValue(edit.getStringLength());
				m_compressionField.setValue(edit.getCompression());
				m_chunkField.setValue(edit.getChunkRowSize());
				m_overwriteNo.setSelected(edit.getOverwritePolicy() == HDF5OverwritePolicy.ABORT);
				m_overwriteYes.setSelected(edit.getOverwritePolicy() == HDF5OverwritePolicy.OVERWRITE);
				m_overwriteInsert.setSelected(edit.getOverwritePolicy() == HDF5OverwritePolicy.INSERT);
				
				DefaultListModel<ColumnNodeEdit> columnModel = (DefaultListModel<ColumnNodeEdit>) m_editList.getModel();
				columnModel.clear();
				for (ColumnNodeEdit columnEdit : getColumnNodeEdits()) {
					columnModel.addElement(columnEdit);
				}
			}

			@Override
			protected void editPropertyItems() {
				DataSetNodeEdit edit = (DataSetNodeEdit) m_node.getUserObject();
				edit.setName(m_nameField.getText());
				edit.setHdfType((HdfDataType) m_typeField.getSelectedItem());
				edit.setEndian((Endian) m_endianField.getSelectedItem());
				edit.setFixed(m_stringLengthFixed.isSelected());
				edit.setStringLength((Integer) m_stringLengthSpinner.getValue());
				edit.setCompression(m_compressionField.isEnabled() ? (Integer) m_compressionField.getValue() : 0);
				edit.setChunkRowSize((Integer) m_chunkField.getValue());
				edit.setOverwritePolicy(m_overwriteYes.isSelected() ? HDF5OverwritePolicy.OVERWRITE
						: (m_overwriteNo.isSelected() ? HDF5OverwritePolicy.ABORT : HDF5OverwritePolicy.INSERT));
				edit.setColumnEdits(Collections.list(((DefaultListModel<ColumnNodeEdit>) m_editList.getModel()).elements()));
				
				m_node.removeAllChildren();
				for (ColumnNodeEdit columnEdit : edit.getColumnNodeEdits()) {
					m_node.add(new DefaultMutableTreeNode(columnEdit));
				}
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(new TreePath(((DefaultMutableTreeNode) m_node.getFirstChild()).getPath()));
				
				edit.validate();
			}
		}
    }
}

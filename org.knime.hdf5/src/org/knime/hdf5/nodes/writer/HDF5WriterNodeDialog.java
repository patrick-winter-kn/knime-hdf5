package org.knime.hdf5.nodes.writer;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.TransferHandler;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {

	private enum SpecInfo {
		COLUMN_SPECS("colSpecs"),
		FLOW_VARIABLE_SPECS("varSpecs");
		
		private final String m_specName;
		
		private SpecInfo(String specName) {
			m_specName = specName;
		}
		
		private String getSpecName() {
			return m_specName;
		}
	}
	
	private SettingsModelString m_filePathSettings;

	private DefaultListModel<DataColumnSpec> m_columnSpecModel = new DefaultListModel<>();
	
	private DefaultListModel<DataColumnSpec> m_flowVariableSpecModel = new DefaultListModel<>();
	
	// TODO this should be in the SettingsFactory later
	private final Map<String, List<DataColumnSpec>> m_transfer = new HashMap<>();
	
	private JPanel m_panel = new JPanel();
	
	private JTree m_tree = new JTree(new DefaultMutableTreeNode("(null)"));
	
    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5WriterNodeDialog() {
		createFileChooser();
		
		addTab("Data", m_panel);
		m_panel.setLayout(new BoxLayout(m_panel, BoxLayout.X_AXIS));

		JPanel input = new JPanel();
		m_panel.add(input);
		input.setLayout(new BoxLayout(input, BoxLayout.Y_AXIS));
		input.setBorder(BorderFactory.createTitledBorder(" Input "));
		addListToPanel(SpecInfo.COLUMN_SPECS, input);
		addListToPanel(SpecInfo.FLOW_VARIABLE_SPECS, input);
    	
    	JPanel output = new JPanel();
		m_panel.add(output);
		output.setLayout(new BoxLayout(output, BoxLayout.Y_AXIS));
		output.setBorder(BorderFactory.createTitledBorder(" Output "));
		addTreeToPanel(output);
	}
    
    private void createFileChooser() {
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "outputFilePathHistory",
				JFileChooser.SAVE_DIALOG, false, filePathFvm, ".h5|.hdf5");
		fileChooser.setBorderTitle("Output file:");
		addDialogComponent(fileChooser);
		fileChooser.getModel().addChangeListener(new ChangeListener() {

			private boolean m_init = false;
			
			@Override
			public void stateChanged(ChangeEvent e) {
				if (new File(m_filePathSettings.getStringValue()).isFile() || !m_init) {
			        updateTree();
			        m_init = true;
				}
			}
		});
	}
    
    private void addListToPanel(SpecInfo specInfo, JPanel panel) {
    	JPanel listPanel = new JPanel();
    	panel.add(listPanel);
    	
    	listPanel.setLayout(new BoxLayout(listPanel, BoxLayout.Y_AXIS));
		listPanel.setBorder(BorderFactory.createTitledBorder(specInfo == SpecInfo.COLUMN_SPECS ? "Columns:" : "Flow Variables:"));
    	
    	JList<DataColumnSpec> list = new JList<>(specInfo == SpecInfo.COLUMN_SPECS ? m_columnSpecModel : m_flowVariableSpecModel);
    	listPanel.add(list);
		list.setVisibleRowCount(-1);
		
		final JScrollPane jsp = new JScrollPane(list);
		jsp.setMinimumSize(new Dimension(50, 100));
		listPanel.add(jsp);
		
		list.setCellRenderer(new DefaultListCellRenderer() {

			private static final long serialVersionUID = 4119451757237000581L;

			@Override
			public Component getListCellRendererComponent(JList<?> list,
					Object value, int index, boolean isSelected,
					boolean cellHasFocus) {
				super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
				
				if (value instanceof DataColumnSpec) {
					DataColumnSpec spec = (DataColumnSpec) value;
					
					setIcon(spec.getType().getIcon());
					setText(spec.getName());
				}
				
				return this;
			}
		});
		
		list.setDragEnabled(true);
		list.setTransferHandler(new TransferHandler() {

			private static final long serialVersionUID = -4233815652319877595L;

            public int getSourceActions(JComponent c) {
                return COPY;
            }
             
            @SuppressWarnings("unchecked")
			protected Transferable createTransferable(JComponent c) {
                if (c instanceof JList) {
                	JList<DataColumnSpec> list = (JList<DataColumnSpec>) c;
    				m_transfer.put(specInfo.getSpecName(), list.getSelectedValuesList());
					
					return new StringSelection(specInfo.getSpecName());
                }
                return null;
            }
		});
    }
    
    private Hdf5File createFile(OverwritePolicy policy) throws IOException {
		String filePath = m_filePathSettings.getStringValue();
		
		try {
			return Hdf5File.createFile(filePath);
			
		} catch (IOException ioe) {
			if (policy == OverwritePolicy.ABORT) {
				throw new IOException("Abort: " + ioe.getMessage());
			} else {
				return Hdf5File.openFile(filePath, Hdf5File.READ_WRITE_ACCESS);
			}
		}
	}
    
    private void addTreeToPanel(JPanel panel) {
    	JPanel treePanel = new JPanel();
    	panel.add(treePanel);
    	
    	treePanel.setLayout(new BoxLayout(treePanel, BoxLayout.Y_AXIS));
		treePanel.setBorder(BorderFactory.createTitledBorder("File:"));
    	treePanel.add(m_tree);
		
		final JScrollPane jsp = new JScrollPane(m_tree);
		jsp.setMinimumSize(new Dimension(50, 100));
		treePanel.add(jsp);
		
    	m_tree.setCellRenderer(new DefaultTreeCellRenderer() {

			private static final long serialVersionUID = -2424225988962935310L;

			private final String dir = "C:\\Users\\UK\\Documents\\GitHub\\knime-hdf5\\org.knime.hdf5\\";
			private final Icon columnIcon = new ImageIcon(dir + "icons\\column.png");
			private final Icon dataSetIcon = new ImageIcon(dir + "icons\\dataSet.png");
			private final Icon groupIcon = new ImageIcon(dir + "icons\\group.png");
			private final Icon fileIcon = new ImageIcon(dir + "icons\\file.png");
			
			@Override
			public Component getTreeCellRendererComponent(final JTree tree,
					final Object value, final boolean sel, final boolean expanded,
					final boolean leaf, final int row, final boolean hasfocus) {
				super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row,
						hasfocus);
				
				if (value instanceof DefaultMutableTreeNode) {
					DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;
					
					Icon icon = null;
					if (!node.getAllowsChildren()) {
						icon = columnIcon;
					} else if (node.getChildCount() != 0 && !node.getFirstChild().getAllowsChildren()) {
						icon = dataSetIcon;
					} else if (!node.isRoot()) {
						icon = groupIcon;
					} else {
						icon = fileIcon;
					}
					setIcon(icon);
				}
				
				return this;
			}
		});
		
    	m_tree.setDragEnabled(true);
    	m_tree.setTransferHandler(new TransferHandler() {

			private static final long serialVersionUID = -4233815652319877595L;
        	
			public boolean canImport(TransferHandler.TransferSupport info) {
				// we only import Strings
                if (!info.isDataFlavorSupported(DataFlavor.stringFlavor)) {
                    return false;
                }
                
                JTree.DropLocation dl = (JTree.DropLocation) info.getDropLocation();
                if (dl.getPath() == null) {
                    return false;
                }
                return true;
            }

			public boolean importData(TransferHandler.TransferSupport info) {
                if (!info.isDrop()) {
                    return false;
                }

                // Check for String flavor
                if (!info.isDataFlavorSupported(DataFlavor.stringFlavor)) {
                    return false;
                }
                
                JTree.DropLocation dl = (JTree.DropLocation) info.getDropLocation();
                TreePath path = dl.getPath();
                DefaultMutableTreeNode parent = (DefaultMutableTreeNode) path.getLastPathComponent();
                
                if (!parent.getAllowsChildren()) {
                	path = path.getParentPath();
                	parent = (DefaultMutableTreeNode) parent.getParent();
                	
                } else if (parent.getChildCount() == 0 || parent.getFirstChild().getAllowsChildren()) {
                	DefaultMutableTreeNode newChild = new DefaultMutableTreeNode("dataSet[" + parent.getChildCount() + "]");
                	parent.add(newChild);
                	path = path.pathByAddingChild(newChild);
                	parent = newChild;
                }
 
                // Get the string that is being dropped.
                Transferable t = info.getTransferable();
                List<DataColumnSpec> data = null;
                try {
                    data = m_transfer.get((String) t.getTransferData(DataFlavor.stringFlavor));
                } catch (Exception e) {
                	return false;
                }
                
                for (int i = 0; i < data.size(); i++) {
                    DefaultMutableTreeNode newChild = new DefaultMutableTreeNode(data.get(i));
    				newChild.setAllowsChildren(false);
    				parent.add(newChild);
                }
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
                
                return true;
            }
        });
    	m_tree.setDropMode(DropMode.ON_OR_INSERT);
    }
    
    private void updateTree() {
    	Hdf5File file = null;
		try {
			file = createFile(OverwritePolicy.OVERWRITE);
			
			DefaultMutableTreeNode root = new DefaultMutableTreeNode(file.getName());
			((DefaultTreeModel) m_tree.getModel()).setRoot(root);
			addChildrenToNode(root, file);
			
		} catch (IOException ioe) {
			// TODO exception (should never occur)
		} finally {
			file.close();
		}
    }
    
    private void addChildrenToNode(DefaultMutableTreeNode parentNode, Hdf5TreeElement treeElement) {
    	try {
        	if (treeElement.isGroup()) {
        		for (String groupName : ((Hdf5Group) treeElement).loadGroupNames()) {
        			Hdf5Group group = ((Hdf5Group) treeElement).getGroup(groupName);
        			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(group.getName());
        			parentNode.add(childNode);
        			addChildrenToNode(childNode, group);
        		}
        		for (String dataSetName : ((Hdf5Group) treeElement).loadDataSetNames()) {
        			Hdf5DataSet<?> dataSet = ((Hdf5Group) treeElement).getDataSet(dataSetName);
        			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(dataSet.getName());
        			parentNode.add(childNode);
        			addChildrenToNode(childNode, dataSet);
        		}
        	}
    	} catch (Exception e) {
    		// TODO exception
    	}
    	
    	if (treeElement.isDataSet()) {
    		Hdf5DataSet<?> dataSet = (Hdf5DataSet<?>) treeElement;
    		// TODO what to do with 1 dimension?
    		if (dataSet.getDimensions().length == 2) {
    			for (int i = 0; i < dataSet.getDimensions()[1]; i++) {
    				try {
						DataColumnSpec spec = new DataColumnSpecCreator("col" + (i + 1), dataSet.getType().getKnimeType().getColumnDataType()).createSpec();
		    			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(spec);
		    			childNode.setAllowsChildren(false);
	    				parentNode.add(childNode);
	        			
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
					}
    			}
    		}
    	}
    }
    
    /**
     * Calls the update method of the underlying filter panel.
     * @param settings the node settings to read from
     * @param specs the input specs
     * @throws NotConfigurableException if no columns are available for
     *             filtering
     */
    @Override
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
    	m_columnSpecModel.clear();
    	for (int i = 0; i < specs[0].getNumColumns(); i++) {
        	m_columnSpecModel.addElement(specs[0].getColumnSpec(i));
    	}

    	m_flowVariableSpecModel.clear();
    	FlowVariable[] flowVariables = getAvailableFlowVariables().values().toArray(new FlowVariable[] {});
		for (FlowVariable flowVariable : flowVariables) {
			try {
				m_flowVariableSpecModel.add(0, new DataColumnSpecCreator(flowVariable.getName(), Hdf5KnimeDataType.getColumnDataType(flowVariable.getType())).createSpec());
				
			} catch (UnsupportedDataTypeException udte) {
				NodeLogger.getLogger("HDF5 Files").error("Type of FlowVariable \"" + flowVariable.getName() + "\" is not supported", udte);
			}
		}
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    }
}

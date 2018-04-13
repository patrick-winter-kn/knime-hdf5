package org.knime.hdf5.nodes.writer;

import java.awt.Component;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.DefaultListModel;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JList;
import javax.swing.JPanel;
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
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelString m_groupPathSettings;
	
	private SettingsModelString m_groupNameSettings;

	private DefaultListModel<DataColumnSpec> m_columnSpecModel = new DefaultListModel<>();
	
	private DefaultListModel<DataColumnSpec> m_flowVariableSpecModel = new DefaultListModel<>();
	
	// TODO this should be in the SettingsFactory later
	private Map<String, List<DataColumnSpec>> m_transfer = new HashMap<>();
	
	private static final String COL_TRANSFER = "colSpecs";
	
    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5WriterNodeDialog() {
		createFileChooser();
		createGroupEntryBox();
		
		JPanel panel = new JPanel();
		addTab("Data", panel);
		
		JPanel select = new JPanel();
		panel.add(select);

		JList<DataColumnSpec> colList = new JList<>(m_columnSpecModel);
		colList.setVisibleRowCount(-1);
		colList.setDragEnabled(true);
		colList.setTransferHandler(new TransferHandler() {

			private static final long serialVersionUID = -4233815652319877595L;

            public int getSourceActions(JComponent c) {
                return COPY;
            }
             
            @SuppressWarnings("unchecked")
			protected Transferable createTransferable(JComponent c) {
                if (c instanceof JList) {
                	JList<DataColumnSpec> list = (JList<DataColumnSpec>) c;
    				m_transfer.put(COL_TRANSFER, list.getSelectedValuesList());
					
					return new StringSelection(COL_TRANSFER);
                }
                return null;
            }
		});
		select.add(colList);
        
		JList<DataColumnSpec> varList = new JList<>(m_flowVariableSpecModel);
		varList.setVisibleRowCount(-1);
		varList.setDragEnabled(true);
		select.add(varList);
        
		JPanel output = new JPanel();
		panel.add(output);
		
		DefaultMutableTreeNode root = new DefaultMutableTreeNode("group");
		JTree tree = new JTree(root);
		output.add(tree);
		/*
		tree.setCellRenderer(new DefaultTreeCellRenderer() {

			private static final long serialVersionUID = -2424225988962935310L;

			
			@Override
			public Component getTreeCellRendererComponent(final JTree tree,
					final Object value, final boolean sel, final boolean expanded,
					final boolean leaf, final int row, final boolean hasfocus) {
				super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row,
						hasfocus);
				
				if (value instanceof DefaultMutableTreeNode) {
					DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;
					String name = (String) node.getUserObject();
					
					Icon icon = null;
					if (name.equals("group")) {
						icon = new ImageIcon("icons/group.png");
					} else if (name.startsWith("dataSet")) {
						icon = new ImageIcon("icons" + File.separator + "dataSet.png");
					} else {
						icon = new ImageIcon("icons" + File.separator + "column.png");
					}
					setLeafIcon(icon);
					setClosedIcon(icon);
					setOpenIcon(icon);
				} else {
					// TODO exception
				}
				return this;
			}
		});
		*/
		tree.setDragEnabled(true);
        tree.setTransferHandler(new TransferHandler() {

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
                
                if (parent.isRoot()) {
                	DefaultMutableTreeNode newChild = new DefaultMutableTreeNode("dataSet[" + parent.getChildCount() + "]");
                	parent.add(newChild);
                	path = path.pathByAddingChild(newChild);
                	parent = newChild;
                	
                } else if (!parent.getAllowsChildren()) {
                	path = path.getParentPath();
                	parent = (DefaultMutableTreeNode) parent.getParent();
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
				
				((DefaultTreeModel) (tree.getModel())).reload();
				tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
                
                return true;
            }
        });
        tree.setDropMode(DropMode.ON_OR_INSERT);
	}
    
	private void createFileChooser() {
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "outputFilePathHistory",
				JFileChooser.SAVE_DIALOG, false, filePathFvm, ".h5|.hdf5");
		fileChooser.setBorderTitle("Output file:");
		addDialogComponent(fileChooser);
		fileChooser.getModel().addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				
			}
		});
	}
	
	private void createGroupEntryBox() {
		// Border groupBorder = BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output group:");

		m_groupPathSettings = SettingsFactory.createGroupPathSettings();
		DialogComponentString groupPathSettings = new DialogComponentString(m_groupPathSettings,
				"Path to output group");
		addDialogComponent(groupPathSettings);
		
		m_groupNameSettings = SettingsFactory.createGroupNameSettings();
		DialogComponentString groupNameSettings = new DialogComponentString(m_groupNameSettings,
				"Output group");
		addDialogComponent(groupNameSettings);
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
    	for (int i = 0; i < specs[0].getNumColumns(); i++) {
        	m_columnSpecModel.addElement(specs[0].getColumnSpec(i));
    	}

    	FlowVariable[] flowVariables = getAvailableFlowVariables().values().toArray(new FlowVariable[] {});
		for (FlowVariable flowVariable : flowVariables) {
			try {
				m_flowVariableSpecModel.addElement(new DataColumnSpecCreator(flowVariable.getName(), Hdf5KnimeDataType.getColumnDataType(flowVariable.getType())).createSpec());
				
			} catch (UnsupportedDataTypeException udte) {
				NodeLogger.getLogger("HDF5 Files").error("Type of FlowVariable \"" + flowVariable.getName() + "\" is not supported", udte);
			}
		}
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {}
}

package org.knime.hdf5.nodes.writer;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.IOException;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.TransferHandler;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;

class EditTreePanel extends JPanel {

	private static final long serialVersionUID = -9129152385004241047L;

	private final EditTreeConfiguration m_editTreeConfig = new EditTreeConfiguration("temp");
	
	private final JTree m_tree = new JTree(new DefaultMutableTreeNode("(null)"));

	EditTreePanel() {
		setLayout(new BorderLayout());
		setBorder(BorderFactory.createTitledBorder("File:"));
		
		final JScrollPane jsp = new JScrollPane(m_tree);
		add(jsp, BorderLayout.CENTER);
		
    	m_tree.setCellRenderer(new DefaultTreeCellRenderer() {

			private static final long serialVersionUID = -2424225988962935310L;

			private final Icon columnIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/column.png");
			private final Icon attributeIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/attribute.png");
			private final Icon dataSetIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/dataSet.png");
			private final Icon groupIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/group.png");
			private final Icon fileIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/file.png");
			
			private Icon loadIcon(
		            final Class<?> className, final String path) {
		        ImageIcon icon;
		        try {
		            ClassLoader loader = className.getClassLoader();
		            String packagePath =
		                className.getPackage().getName().replace('.', '/');
		            String correctedPath = path;
		            if (!path.startsWith("/")) {
		                correctedPath = "/" + path;
		            }
		            icon = new ImageIcon(
		                    loader.getResource(packagePath + correctedPath));
		        } catch (Exception e) {
		            NodeLogger.getLogger(FlowVariableListCellRenderer.class).debug(
		                    "Unable to load icon at path " + path, e);
		            icon = null;
		        }
		        return icon;
		    }
			
			@Override
			public Component getTreeCellRendererComponent(final JTree tree,
					final Object value, final boolean sel, final boolean expanded,
					final boolean leaf, final int row, final boolean hasfocus) {
				super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row,
						hasfocus);
				
				if (value instanceof DefaultMutableTreeNode) {
					DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;
					Object userObject = node.getUserObject();

					String text = null;
					Icon icon = null;
					
					if (userObject instanceof DataColumnSpec) {
						text = ((DataColumnSpec) userObject).getName();
						icon = columnIcon;
						
					} else if (userObject instanceof FlowVariable) {
						text = ((FlowVariable) userObject).getName();
						icon = attributeIcon;
						
					} else if (userObject instanceof String) {
						text = ((String) userObject);
						if (!node.getAllowsChildren()) {
							icon = columnIcon; // TODO unreachable
						} else if (node.getChildCount() != 0 && !node.getFirstChild().getAllowsChildren()) {
							icon = dataSetIcon;
						} else if (!node.isRoot()) {
							icon = groupIcon;
						} else {
							icon = fileIcon;
						}
					} else if (userObject instanceof Hdf5Attribute) {
						text = ((Hdf5Attribute<?>) userObject).getName();
						icon = attributeIcon;
					
					} else if (userObject instanceof Hdf5TreeElement) {
						Hdf5TreeElement treeElement = (Hdf5TreeElement) userObject;
						text = treeElement.getName();
						if (treeElement.isFile()) {
							icon = fileIcon;
						} else if (treeElement.isGroup()) {
							icon = groupIcon;
						} else if (treeElement.isDataSet()) {
							icon = dataSetIcon;
						}  
					}

					setText(text);
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
                
                // Get the string that is being dropped.
                Transferable t = info.getTransferable();
                List<?> data = null;
                try {
                	data = SpecInfo.get((String) t.getTransferData(DataFlavor.stringFlavor)).getSpecList();
                	if (data == null || data.isEmpty()) {
                		return false;
                	}
                } catch (UnsupportedFlavorException | IOException ufioe) {
                	return false;
                }
                
                if (!parent.getAllowsChildren()) {
                	path = path.getParentPath();
                	parent = (DefaultMutableTreeNode) parent.getParent();
                	
                } else if (parent.getChildCount() == 0 || parent.getFirstChild().getAllowsChildren()) {
                	if (data.get(0) instanceof DataColumnSpec) {
                		String newName = "dataSet[" + parent.getChildCount() + "]";
                    	DefaultMutableTreeNode newChild = new DefaultMutableTreeNode(newName);
                    	parent.add(newChild);
                    	m_editTreeConfig.addTreeNodeEdit(new TreeNodeEdit(parent.getPath(), newName, null, TreeNodeEdit.HDF5_DATASET));
                    	path = path.pathByAddingChild(newChild);
                    	parent = newChild;
                	}
                }
                
                for (int i = 0; i < data.size(); i++) {
                	Object newData = data.get(i);
                    DefaultMutableTreeNode newChild = new DefaultMutableTreeNode(newData);
    				newChild.setAllowsChildren(false);
    				parent.add(newChild);
    				if (newData instanceof DataColumnSpec) {
                    	m_editTreeConfig.addTreeNodeEdit(new TreeNodeEdit(parent.getPath(), (DataColumnSpec) newData));
                    	
    				} else if (newData instanceof FlowVariable) {
                    	m_editTreeConfig.addTreeNodeEdit(new TreeNodeEdit(parent.getPath(), (FlowVariable) newData));
    				}
                }
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
                
                return true;
            }
        });
    	m_tree.setDropMode(DropMode.ON_OR_INSERT);
	}
	
	JTree getTree() {
		return m_tree;
	}

	void saveConfiguration(EditTreeConfiguration editTreeConfig) {
		for (TreeNodeEdit edit : m_editTreeConfig.getEdits()) {
			editTreeConfig.addTreeNodeEdit(edit);
		}
	}
	
	void loadConfiguration(EditTreeConfiguration editTreeConfig) {
		/* use all the edits on the JTree */
	}
}

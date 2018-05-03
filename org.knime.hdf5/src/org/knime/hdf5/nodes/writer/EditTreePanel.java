package org.knime.hdf5.nodes.writer;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Frame;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.BorderFactory;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.TransferHandler;
import javax.swing.WindowConstants;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;
import org.knime.hdf5.nodes.writer.TreeNodeEdit.EditClass;

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
			private final Icon fileIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/file.png");
			private final Icon groupIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/group.png");
			private final Icon newColumnIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/column_new.png");
			private final Icon newAttributeIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/attribute_new.png");
			private final Icon newDataSetIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/dataSet_new.png");
			private final Icon newGroupIcon = loadIcon(HDF5WriterNodeDialog.class, "/icon/group_new.png");
			
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
						
					} else if (userObject instanceof Hdf5Attribute) {
						text = ((Hdf5Attribute<?>) userObject).getName();
						icon = attributeIcon;
					
					} else if (userObject instanceof Hdf5TreeElement) {
						Hdf5TreeElement treeElement = (Hdf5TreeElement) userObject;
						text = treeElement.getName();
						if (treeElement.isDataSet()) {
							icon = dataSetIcon;
						} else if (treeElement.isFile()) {
							icon = fileIcon;
						} else if (treeElement.isGroup()) {
							icon = groupIcon;
						}
					} else if (userObject instanceof TreeNodeEdit) {
						TreeNodeEdit edit = (TreeNodeEdit) userObject;
						text = edit.getName();
						switch (edit.getEditClass()) {
						case DATA_COLUMN_SPEC:
							icon = newColumnIcon;
							break;
						case FLOW_VARIABLE:
							icon = newAttributeIcon;
							break;
						case HDF5_DATASET:
							icon = newDataSetIcon;
							break;
						case HDF5_GROUP:
							icon = newGroupIcon;
							break;
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
                
                // TODO maybe do it without the properties of the parent node
                if (!parent.getAllowsChildren()) {
                	path = path.getParentPath();
                	parent = (DefaultMutableTreeNode) parent.getParent();
                	
                } else if (parent.getChildCount() == 0 || parent.getFirstChild().getAllowsChildren()) {
                	if (data.get(0) instanceof DataColumnSpec) {
                		String newName = "dataSet[" + parent.getChildCount() + "]";
                		TreeNodeEdit edit = new TreeNodeEdit(parent.getPath(), newName, null, EditClass.HDF5_DATASET);
                    	DefaultMutableTreeNode newChild = new DefaultMutableTreeNode(edit);
                    	parent.add(newChild);
                    	m_editTreeConfig.addTreeNodeEdit(edit);
                    	path = path.pathByAddingChild(newChild);
                    	parent = newChild;
                	}
                }
                
                for (int i = 0; i < data.size(); i++) {
                	Object newData = data.get(i);
                	
                	TreeNodeEdit edit = null;
    				if (newData instanceof DataColumnSpec) {
                    	edit = new TreeNodeEdit(parent.getPath(), (DataColumnSpec) newData);
                    	
    				} else if (newData instanceof FlowVariable) {
                    	edit = new TreeNodeEdit(parent.getPath(), (FlowVariable) newData);
    				}
    				
    				if (edit != null) {
                        DefaultMutableTreeNode newChild = new DefaultMutableTreeNode(edit);
        				newChild.setAllowsChildren(false);
        				parent.add(newChild);
                    	m_editTreeConfig.addTreeNodeEdit(edit);
    				}
                }
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
                
                return true;
            }
        });
    	m_tree.setDropMode(DropMode.ON_OR_INSERT);

    	final GroupMenu groupMenu = new GroupMenu(false);
    	final GroupMenu editTreeNodeGroupMenu = new GroupMenu(true);
    	m_tree.addMouseListener(new MouseAdapter() {
    		
    		@Override
    		public void mouseReleased(MouseEvent e) {
    			if (e.isPopupTrigger()) {
    				TreePath path = m_tree.getPathForLocation(e.getX(), e.getY());
    				DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
    				Object userObject = node.getUserObject();
					m_tree.setSelectionPath(path);
					if (userObject instanceof Hdf5Group) {
    					groupMenu.show(e.getComponent(), e.getX(), e.getY());
        				groupMenu.initMenu(node);
    				} else if (userObject instanceof TreeNodeEdit && ((TreeNodeEdit) userObject).getEditClass() == EditClass.HDF5_GROUP) {
    					editTreeNodeGroupMenu.show(e.getComponent(), e.getX(), e.getY());
    					editTreeNodeGroupMenu.initMenu(node);
    				}
    			}
    		}
		});
	}
	
	JTree getTree() {
		return m_tree;
	}

	void updateTreeWithFile(Hdf5File file) {
		DefaultMutableTreeNode root = new DefaultMutableTreeNode(file);
		((DefaultTreeModel) m_tree.getModel()).setRoot(root);
		addChildrenToNode(root);
	}
    
    private void addChildrenToNode(DefaultMutableTreeNode parentNode) {
    	Object userObject = parentNode.getUserObject();
    	
    	if (userObject instanceof Hdf5TreeElement) {
        	Hdf5TreeElement parent = (Hdf5TreeElement) userObject;

        	if (parent.isGroup()) {
        		Hdf5Group parentGroup = (Hdf5Group) parent;
        		
            	try {
            		for (String groupName : parentGroup.loadGroupNames()) {
            			Hdf5Group group = parentGroup.getGroup(groupName);
            			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(group);
            			parentNode.add(childNode);
            			
            			addChildrenToNode(childNode);
            		}
            		for (String dataSetName : parentGroup.loadDataSetNames()) {
            			Hdf5DataSet<?> dataSet = parentGroup.getDataSet(dataSetName);
            			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(dataSet);
            			parentNode.add(childNode);
            			addChildrenToNode(childNode);
            		}
            	} catch (IOException ioe) {
            		// TODO exception
            	}
        	} else if (parent.isDataSet()) {
        		Hdf5DataSet<?> parentDS = (Hdf5DataSet<?>) parent;
        		
        		// TODO what to do with 1 dimension?
        		if (parentDS.getDimensions().length == 2) {
        			for (int i = 0; i < parentDS.getDimensions()[1]; i++) {
        				try {
    						DataColumnSpec spec = new DataColumnSpecCreator("col" + (i + 1), parentDS.getType().getKnimeType().getColumnDataType()).createSpec();
    		    			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(spec);
    		    			childNode.setAllowsChildren(false);
    	    				parentNode.add(childNode);
    	        			
    					} catch (UnsupportedDataTypeException udte) {
    						NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
    					}
        			}
        		}
        	}
        	
        	try {
        		for (String attributeName : parent.loadAttributeNames()) {
        			Hdf5Attribute<?> attribute = parent.getAttribute(attributeName);
        			DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(attribute);
        			parentNode.add(childNode);
        		}
        	} catch (IOException ioe) {
        		// TODO exception
        	}
    	}
    }
    
    private class GroupMenu extends JPopupMenu {

    	private static final long serialVersionUID = -7709804406752499090L;

    	private DefaultMutableTreeNode m_node;
    	
		private GroupMenu(boolean fromTreeNodeEdit) {
    		if (fromTreeNodeEdit) {
	    		JMenuItem itemEdit = new JMenuItem("Edit group properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof TreeNodeEdit) {
							TreeNodeEdit edit = (TreeNodeEdit) userObject;
							Frame parent = (Frame)SwingUtilities.getAncestorOfClass(
									Frame.class, m_tree);
							JDialog dialog = new JDialog(parent, "Group properties");
				            dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
							dialog.setSize(200, 200);
							dialog.setLocation(400, 400);
							dialog.setVisible(true);
							JPanel panel = new JPanel(new BorderLayout());
							dialog.add(panel, BorderLayout.PAGE_START);
							JLabel nameLabel = new JLabel("Name: ");
							JTextField nameField = new JTextField(4);
							nameField.setText(edit.getName());
							panel.add(nameLabel, BorderLayout.WEST);
							panel.add(nameField, BorderLayout.CENTER);
							JButton okButton = new JButton("OK");
							okButton.addActionListener(new ActionListener() {
								
								@Override
								public void actionPerformed(ActionEvent e) {
									edit.setName(nameField.getText());
									dialog.setVisible(false);
				    				((DefaultTreeModel) (m_tree.getModel())).reload();
								}
							});
							dialog.add(okButton, BorderLayout.PAGE_END);
						}
					}
				});
	    		add(itemEdit);
    		}
    		
    		JMenuItem itemCreate = new JMenuItem("Create group");
    		itemCreate.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					String newName = "group[" + m_node.getChildCount() + "]";
            		TreeNodeEdit edit = new TreeNodeEdit(m_node.getPath(), newName, null, EditClass.HDF5_GROUP);
                	m_node.add(new DefaultMutableTreeNode(edit));
                	m_editTreeConfig.addTreeNodeEdit(edit);
    				((DefaultTreeModel) (m_tree.getModel())).reload();
    				m_tree.makeVisible(new TreePath(m_node.getPath()).pathByAddingChild(m_node.getFirstChild()));
				}
			});
    		add(itemCreate);
    		
    		if (fromTreeNodeEdit) {
        		JMenuItem itemDelete = new JMenuItem("Delete group");
        		itemDelete.addActionListener(new ActionListener() {
    				
    				@Override
    				public void actionPerformed(ActionEvent e) {
						Object userObject = m_node.getUserObject();
						if (userObject instanceof TreeNodeEdit) {
							TreeNodeEdit edit = (TreeNodeEdit) userObject;
	                    	m_editTreeConfig.removeTreeNodeEdit(edit);
	                    	DefaultMutableTreeNode parent = (DefaultMutableTreeNode) m_node.getParent();
	                    	parent.remove(m_node);
	        				((DefaultTreeModel) (m_tree.getModel())).reload();
	        				m_tree.makeVisible(new TreePath(parent.getPath()));
						}
    				}
    			});
        		add(itemDelete);
    		}
    	}
		
		private void initMenu(DefaultMutableTreeNode node) {
			m_node = node;
		}
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

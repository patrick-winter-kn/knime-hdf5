package org.knime.hdf5.nodes.writer;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.BorderFactory;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.TransferHandler;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;
import org.knime.hdf5.nodes.writer.edit.AttributeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.ColumnNodeEdit;
import org.knime.hdf5.nodes.writer.edit.DataSetNodeEdit;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;
import org.knime.hdf5.nodes.writer.edit.GroupNodeEdit;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.EditAction;

public class EditTreePanel extends JPanel {

	private static final long serialVersionUID = -9129152385004241047L;

	private EditTreeConfiguration m_editTreeConfig = new EditTreeConfiguration("temp");
	
	private final JTree m_tree = new JTree(new DefaultMutableTreeNode("(null)"));

	public EditTreePanel() {
		setLayout(new BorderLayout());
		setBorder(BorderFactory.createTitledBorder("File:"));
		
		final JScrollPane jsp = new JScrollPane(m_tree);
		add(jsp, BorderLayout.CENTER);
		
    	m_tree.setCellRenderer(new DefaultTreeCellRenderer() {

			private static final long serialVersionUID = -2424225988962935310L;

			private final String[] itemNames = { "column", "attribute", "dataSet", "file", "group" };
			
			private final String[] stateNames = { "idle", "new", "modify", "delete", "invalid" };
			
			private final Icon[][] icons = loadAllIcons();
			
			private Icon[][] loadAllIcons() {
				Icon[][] loadedIcons = new Icon[itemNames.length][stateNames.length];
				
				for (int i = 0; i < itemNames.length; i++) {
					for (int j = 0; j < stateNames.length; j++) {
						loadedIcons[i][j] = loadIcon(EditTreePanel.class, "/icon/" + itemNames[i] + "_" + stateNames[j] + ".png");
					}
				}
				
				return loadedIcons;
			}
			
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
		            NodeLogger.getLogger(EditTreePanel.class).debug(
		                    "Unable to load icon at path " + path, e);
		            icon = null;
		        }
		        return icon;
		    }
			
			private int getItemId(TreeNodeEdit edit) {
				return edit instanceof ColumnNodeEdit ? 0 : 
					(edit instanceof AttributeNodeEdit ? 1 :
					(edit instanceof DataSetNodeEdit ? 2 :
					(edit instanceof FileNodeEdit ? 3 : 
					(edit instanceof GroupNodeEdit ? 4 : -1))));
			}
			
			private int getStateId(TreeNodeEdit edit) {
				EditAction editAction = edit.getEditAction();
				return !edit.isValid() ? 4 : 
					(editAction == TreeNodeEdit.EditAction.NO_ACTION ? 0 : 
					(editAction.isCreateOrCopyAction() ? 1 :
					(editAction == TreeNodeEdit.EditAction.MODIFY ? 2 :
					(editAction == TreeNodeEdit.EditAction.DELETE ? 3 : -1))));
			}
			
			@Override
			public Component getTreeCellRendererComponent(final JTree tree,
					final Object value, final boolean sel, final boolean expanded,
					final boolean leaf, final int row, final boolean hasfocus) {
				super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row,
						hasfocus);
				
				if (value instanceof DefaultMutableTreeNode) {
					DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;
					Object nodeObject = node.getUserObject();

					String text = null;
					Icon icon = null;
					
					if (nodeObject instanceof TreeNodeEdit) {
						TreeNodeEdit edit = (TreeNodeEdit) nodeObject;
						text = edit.getName();
						icon = icons[getItemId(edit)][getStateId(edit)];
						setBackground(edit.isValid() ? Color.WHITE : Color.RED);
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
			
			private List<TreeNodeEdit> m_copyEdits = new ArrayList<>();
			
            public int getSourceActions(JComponent comp) {
                return COPY;
            }
        	
			protected Transferable createTransferable(JComponent comp) {
				m_copyEdits.clear();
                if (comp instanceof JTree) {
                	JTree tree = (JTree) comp;
                	for (TreePath path : tree.getSelectionPaths()) {
        				m_copyEdits.add((TreeNodeEdit) ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject());
                	}
                	return new StringSelection("");
                }
                return null;
            }
			
			public boolean canImport(TransferHandler.TransferSupport info) {
                JTree.DropLocation dl = (JTree.DropLocation) info.getDropLocation();
                TreePath path = dl.getPath();
                
                if (info.isDataFlavorSupported(DataFlavor.stringFlavor) && path != null) {
                	Object nodeObject = ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject();
                	if (nodeObject instanceof TreeNodeEdit) {
                    	TreeNodeEdit edit = (TreeNodeEdit) nodeObject;
        				boolean pasteDataSet = edit instanceof DataSetNodeEdit;
        				boolean pasteGroup = edit instanceof GroupNodeEdit;
        				
        				for (TreeNodeEdit copyEdit : m_copyEdits) {
            				pasteDataSet &= copyEdit instanceof ColumnNodeEdit || copyEdit instanceof AttributeNodeEdit;
            				pasteGroup &= copyEdit instanceof DataSetNodeEdit || copyEdit instanceof GroupNodeEdit || copyEdit instanceof AttributeNodeEdit;
        				}

                        return pasteDataSet || pasteGroup;
                	}
                	
                	return true;
                }
                
                return false;
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
                
                // Get the string that is being dropped.
                Transferable t = info.getTransferable();
                String specListKey = null;
                try {
                	specListKey = (String) t.getTransferData(DataFlavor.stringFlavor);

                } catch (UnsupportedFlavorException | IOException ufioe) {
                	return false;
                }
                
                return !specListKey.isEmpty() ? importFromList(path, specListKey) : importFromTree(path);
            }
			
			private boolean importFromList(TreePath path, String specListKey) {
				List<?> data = SpecInfo.get(specListKey).getSpecList();
            	if (data == null || data.isEmpty()) {
            		return false;
            	}
                
                DefaultMutableTreeNode parent = (DefaultMutableTreeNode) path.getLastPathComponent();
				Object parentObject = parent.getUserObject();
                if (parentObject instanceof DataColumnSpec || parentObject instanceof ColumnNodeEdit
                		|| parentObject instanceof Hdf5Attribute || parentObject instanceof AttributeNodeEdit) {
                	path = path.getParentPath();
                	parent = (DefaultMutableTreeNode) parent.getParent();
                	parentObject = parent.getUserObject();
                }
                
                if (data.get(0) instanceof DataColumnSpec) {
                	if (!(parentObject instanceof Hdf5DataSet<?> || parentObject instanceof DataSetNodeEdit)) {
                		String newName = TreeNodeEdit.getUniqueName(parent, "dataSet");
            			DataSetNodeEdit newEdit = new DataSetNodeEdit((GroupNodeEdit) parentObject, newName);
                    	newEdit.addEditToNode(parent);
                    	
                    	DefaultMutableTreeNode newChild = newEdit.getTreeNode();
                    	path = path.pathByAddingChild(newChild);
                    	parent = newChild;
                    	parentObject = newEdit;
                	}

                	for (int i = 0; i < data.size(); i++) {
                		DataColumnSpec spec = (DataColumnSpec) data.get(i);
                		ColumnNodeEdit newEdit = new ColumnNodeEdit(spec, (DataSetNodeEdit) parentObject, true);
                    	newEdit.addEditToNode(parent);
                	}
                } else if (data.get(0) instanceof FlowVariable) {
                    for (int i = 0; i < data.size(); i++) {
                    	FlowVariable var = (FlowVariable) data.get(i);
                    	
                    	AttributeNodeEdit newEdit = new AttributeNodeEdit(var, (TreeNodeEdit) parentObject);
                    	newEdit.addEditToNode(parent);
                	}
                }
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
                
                return true;
			}
			
			private boolean importFromTree(TreePath path) {
				DefaultMutableTreeNode parent = (DefaultMutableTreeNode) path.getLastPathComponent();
            	TreeNodeEdit edit = (TreeNodeEdit) parent.getUserObject();
            	
				for (TreeNodeEdit copyEdit : m_copyEdits) {
					if (edit instanceof GroupNodeEdit) {
						GroupNodeEdit groupEdit = (GroupNodeEdit) edit;
						if (copyEdit instanceof AttributeNodeEdit) {
							groupEdit.addAttributeNodeEdit((AttributeNodeEdit) copyEdit);
						} else if (copyEdit instanceof DataSetNodeEdit) {
							groupEdit.addDataSetNodeEdit((DataSetNodeEdit) copyEdit);
						} else if (copyEdit instanceof GroupNodeEdit) {
							groupEdit.addGroupNodeEdit((GroupNodeEdit) copyEdit);
						}
            		} else if (edit instanceof DataSetNodeEdit) {
            			DataSetNodeEdit dataSetEdit = (DataSetNodeEdit) edit;
            			if (copyEdit instanceof AttributeNodeEdit) {
							dataSetEdit.addAttributeNodeEdit((AttributeNodeEdit) copyEdit);
						} else if (copyEdit instanceof ColumnNodeEdit) {
							dataSetEdit.addColumnNodeEdit((ColumnNodeEdit) copyEdit);
						};
            		}
    				copyEdit.addEditToNode(parent);
				}
				
				((DefaultTreeModel) (m_tree.getModel())).reload();
				m_tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
				
				return true;
			}
        });
    	m_tree.setDropMode(DropMode.ON_OR_INSERT);

    	m_tree.addMouseListener(new MouseAdapter() {

    		@Override
    		public void mousePressed(MouseEvent e) {
    			if (e.isPopupTrigger()) {
    				createMenu(e);
    			}
    		}
    		
    		@Override
    		public void mouseReleased(MouseEvent e) {
    			if (e.isPopupTrigger()) {
    				createMenu(e);
    			}
    		}
    		
    		// TODO check if this method does not get called twice
    		private void createMenu(MouseEvent e) {
    			TreePath path = m_tree.getPathForLocation(e.getX(), e.getY());
				if (path != null) {
					m_tree.setSelectionPath(path);
    				DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
    				Object userObject = node.getUserObject();
    				if (userObject instanceof ColumnNodeEdit) {
    					ColumnNodeEdit.ColumnNodeMenu menu = ((ColumnNodeEdit) userObject).getColumnEditMenu();
    					menu.initMenu(m_tree, node);
    					menu.show(e.getComponent(), e.getX(), e.getY());
    					
    				} else if (userObject instanceof AttributeNodeEdit) {
    					AttributeNodeEdit.AttributeNodeMenu menu = ((AttributeNodeEdit) userObject).getAttributeEditMenu();
    					menu.initMenu(m_tree, node);
    					menu.show(e.getComponent(), e.getX(), e.getY());
    					
    				} else if (userObject instanceof DataSetNodeEdit) {
    					DataSetNodeEdit.DataSetNodeMenu menu = ((DataSetNodeEdit) userObject).getDataSetEditMenu();
    					menu.initMenu(m_tree, node);
    					menu.show(e.getComponent(), e.getX(), e.getY());
    					
    				} else if (userObject instanceof GroupNodeEdit) {
    					GroupNodeEdit.GroupNodeMenu menu = ((GroupNodeEdit) userObject).getGroupEditMenu();
    					menu.initMenu(m_tree, node);
    					menu.show(e.getComponent(), e.getX(), e.getY());
    				}
				}
    		}
		});
	}
	
	JTree getTree() {
		return m_tree;
	}
	
	void updateTreeWithNewFile(String filePath) {
		FileNodeEdit fileEdit = new FileNodeEdit(filePath);
		((DefaultTreeModel) m_tree.getModel()).setRoot(new DefaultMutableTreeNode(fileEdit));
		m_editTreeConfig.setFileNodeEdit(fileEdit);
	}

	void updateTreeWithExistingFile(Hdf5File file) {
		FileNodeEdit fileEdit = new FileNodeEdit(file);
		m_editTreeConfig.setFileNodeEdit(fileEdit);
		fileEdit.setEditAsRoot((DefaultTreeModel) m_tree.getModel());
		addChildrenToNodeOfEdit(fileEdit);

		DefaultMutableTreeNode root = fileEdit.getTreeNode();
		if (root.getChildCount() > 0) {
			TreePath path = new TreePath(root.getPath());
			m_tree.makeVisible(path.pathByAddingChild(root.getFirstChild()));
		}
	}
    
    private void addChildrenToNodeOfEdit(TreeNodeEdit parentEdit) {
    	Object parentObject = parentEdit.getHdfObject();
    	if (parentEdit instanceof GroupNodeEdit) {
    		Hdf5Group parentGroup = (Hdf5Group) parentObject;
    		
        	try {
        		for (String groupName : parentGroup.loadGroupNames()) {
        			Hdf5Group group = parentGroup.getGroup(groupName);
        			GroupNodeEdit groupEdit = new GroupNodeEdit(group, (GroupNodeEdit) parentEdit);
        			groupEdit.addEditToNode(parentEdit.getTreeNode());
        			addChildrenToNodeOfEdit(groupEdit);
        		}
        		
        		for (String dataSetName : parentGroup.loadDataSetNames()) {
        			Hdf5DataSet<?> dataSet = parentGroup.getDataSet(dataSetName);
        			DataSetNodeEdit dataSetEdit = new DataSetNodeEdit(dataSet, (GroupNodeEdit) parentEdit);
        			dataSetEdit.addEditToNode(parentEdit.getTreeNode());
        			addChildrenToNodeOfEdit(dataSetEdit);
        		}
        	} catch (IOException ioe) {
        		// TODO exception
        	}
    	} else if (parentEdit instanceof DataSetNodeEdit) {
    		Hdf5DataSet<?> parentDataSet = (Hdf5DataSet<?>) parentObject;
    		
    		// TODO what to do with >2 dimensions?
    		if (parentDataSet.getDimensions().length == 2) {
    			for (int i = 0; i < parentDataSet.getDimensions()[1]; i++) {
    				try {
						DataColumnSpec spec = new DataColumnSpecCreator("col" + (i + 1), parentDataSet.getType().getKnimeType().getColumnDataType()).createSpec();
						ColumnNodeEdit columnEdit = new ColumnNodeEdit(spec, (DataSetNodeEdit) parentEdit, false);
						columnEdit.addEditToNode(parentEdit.getTreeNode());
		    			//columnEdit.getTreeNode().setAllowsChildren(false);
	        			
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
					}
    			}
    		} else if (parentDataSet.getDimensions().length < 2) {
    			try {
					DataColumnSpec spec = new DataColumnSpecCreator("col", parentDataSet.getType().getKnimeType().getColumnDataType()).createSpec();
					ColumnNodeEdit columnEdit = new ColumnNodeEdit(spec, (DataSetNodeEdit) parentEdit, false);
					columnEdit.addEditToNode(parentEdit.getTreeNode());
	    			//columnEdit.getTreeNode().setAllowsChildren(false);
        			
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
				}
    		}
    	}

		Hdf5TreeElement parentElement = (Hdf5TreeElement) parentObject;
    	try {
    		for (String attributeName : parentElement.loadAttributeNames()) {
    			Hdf5Attribute<?> attribute = parentElement.getAttribute(attributeName);
    			AttributeNodeEdit attributeEdit = new AttributeNodeEdit(attribute, parentEdit);
    			attributeEdit.addEditToNode(parentEdit.getTreeNode());
    			//attributeEdit.getTreeNode().setAllowsChildren(false);
    		}
    	} catch (IOException ioe) {
    		// TODO exception
    	}
    }

	void saveConfiguration(EditTreeConfiguration editTreeConfig) {
		if (m_editTreeConfig.getFileNodeEdit() != null) {
			editTreeConfig.setFileNodeEdit(m_editTreeConfig.getFileNodeEdit());
		}
	}
	
	void loadConfiguration(EditTreeConfiguration editTreeConfig) {
		editTreeConfig.getFileNodeEdit().setEditAsRoot((DefaultTreeModel) m_tree.getModel());
		
		m_editTreeConfig = editTreeConfig;
	}
}

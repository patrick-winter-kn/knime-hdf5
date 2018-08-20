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
			
			private final String[] stateNames = { "idle", "new", "delete", "invalid" };
			
			private final Icon[][] icons = loadAllIcons();
			
			private Icon[][] loadAllIcons() {
				Icon[][] loadedIcons = new Icon[itemNames.length][stateNames.length];
				
				for (int i = 0; i < itemNames.length; i++) {
					for (int j = 0; j < stateNames.length; j++) {
						String iconName = itemNames[i] + "_" + stateNames[j];
						if (!iconName.equals("file_delete")) {
							loadedIcons[i][j] = loadIcon(EditTreePanel.class, "/icon/" + iconName + ".png");
						}
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
				return !edit.isValid() ? 3 : 
					(editAction == TreeNodeEdit.EditAction.NO_ACTION || editAction == TreeNodeEdit.EditAction.MODIFY ? 0 : 
					(editAction.isCreateOrCopyAction() ? 1 :
					(editAction == TreeNodeEdit.EditAction.DELETE ? 2 : -1)));
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
						text = (edit.getEditAction() == TreeNodeEdit.EditAction.MODIFY ? "*" : "") + edit.getName();
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
                if (comp instanceof JTree) {
                	JTree tree = (JTree) comp;
                	for (TreePath path : tree.getSelectionPaths()) {
        				m_copyEdits.add((TreeNodeEdit) ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject());
                	}
                	return new StringSelection("");
                }
                return null;
            }
			
			@SuppressWarnings("rawtypes")
			public boolean canImport(TransferHandler.TransferSupport info) {
                JTree.DropLocation dl = (JTree.DropLocation) info.getDropLocation();
                TreePath path = dl.getPath();
                
                if (info.isDataFlavorSupported(DataFlavor.stringFlavor) && path != null) {
                	if (!m_copyEdits.isEmpty() ) {
                    	TreeNodeEdit edit = (TreeNodeEdit) ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject();
                    	
                		Class editClass = m_copyEdits.get(0).getClass();
                		boolean allEqual = true;
                		for (int i = 1; i < m_copyEdits.size(); i++) {
                			TreeNodeEdit copyEdit = m_copyEdits.get(i);
                			allEqual &= editClass == copyEdit.getClass();
                		}
                		
                		if (!allEqual) {
            				boolean pasteToDataSet = edit instanceof DataSetNodeEdit;
            				boolean pasteToGroup = edit instanceof GroupNodeEdit;
            				
            				for (TreeNodeEdit copyEdit : m_copyEdits) {
                				pasteToDataSet &= copyEdit instanceof ColumnNodeEdit || copyEdit instanceof AttributeNodeEdit;
                				pasteToGroup &= copyEdit instanceof DataSetNodeEdit || copyEdit instanceof GroupNodeEdit || copyEdit instanceof AttributeNodeEdit;
            				}

                            return pasteToDataSet || pasteToGroup;
                		}
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
                		ColumnNodeEdit newEdit = new ColumnNodeEdit(spec, (DataSetNodeEdit) parentObject);
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
            	TreeNodeEdit parentEdit = (TreeNodeEdit) parent.getUserObject();
            	
            	try {
            		for (TreeNodeEdit copyEdit : m_copyEdits) {
    					TreeNodeEdit newEdit = null;
    					if (parentEdit instanceof GroupNodeEdit) {
    						GroupNodeEdit parentGroupEdit = (GroupNodeEdit) parentEdit;
    						if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo(parentGroupEdit);
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo(parentGroupEdit);
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							String newName = TreeNodeEdit.getUniqueName(parent, "dataSet");
    	            			DataSetNodeEdit newDataSetEdit = new DataSetNodeEdit(parentGroupEdit, newName);
    	            			newDataSetEdit.addEditToNode(parent);
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) newDataSetEdit);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(parentGroupEdit);
    						}
                		} else if (parentEdit instanceof DataSetNodeEdit) {
                			DataSetNodeEdit parentDataSetEdit = (DataSetNodeEdit) parentEdit;
                			if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) parentDataSetEdit.getParent());
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) parentDataSetEdit.getParent());
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) parentDataSetEdit);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(parentDataSetEdit);
    						} 
                		} else if (parentEdit instanceof AttributeNodeEdit) {
                			AttributeNodeEdit parentAttributeEdit = (AttributeNodeEdit) parentEdit;
							TreeNodeEdit grandParentEdit = parentAttributeEdit.getParent();
                			if (copyEdit instanceof GroupNodeEdit) {
                				if (grandParentEdit instanceof GroupNodeEdit) {
        							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) grandParentEdit);
                				} else {
        							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) grandParentEdit.getParent());
                				}
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
	            				if (grandParentEdit instanceof GroupNodeEdit) {
	    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) grandParentEdit);
	            				} else {
	    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) grandParentEdit.getParent());
	            				}
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
	            				if (grandParentEdit instanceof DataSetNodeEdit) {
	    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) grandParentEdit);
	            				} else {
	    							throw new IllegalStateException("There is no dataSet for this column to be added in");
	            				}
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(grandParentEdit);
    						} 
                		} else if (parentEdit instanceof ColumnNodeEdit) {
                			ColumnNodeEdit parentColumnEdit = (ColumnNodeEdit) parentEdit;
							TreeNodeEdit grandParentEdit = parentColumnEdit.getParent();
                			if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) grandParentEdit.getParent());
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) grandParentEdit.getParent());
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) grandParentEdit);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(grandParentEdit);
    						} 
                		}
    					parent = newEdit.getParent().getTreeNode();
    					newEdit.addEditToNode(parent);
    				}
            	} catch (IllegalStateException ise) {
            		NodeLogger.getLogger(getClass()).warn(ise.getMessage());
            		
            	} finally {
    				((DefaultTreeModel) (m_tree.getModel())).reload();
    				for (TreeNodeEdit copyEdit : m_copyEdits) {
    					m_tree.makeVisible(new TreePath(copyEdit.getTreeNode().getPath()));
    				}
    				m_tree.makeVisible(path.pathByAddingChild(parent.getFirstChild()));
            	}
				
				return true;
			}
			
			@Override
			protected void exportDone(JComponent source, Transferable data, int action) {
				super.exportDone(source, data, action);
				m_copyEdits.clear();
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
    		
    		private void createMenu(MouseEvent e) {
    			TreePath path = m_tree.getPathForLocation(e.getX(), e.getY());
				if (path != null) {
					m_tree.setSelectionPath(path);
    				DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
    				Object userObject = node.getUserObject();
    				if (userObject instanceof TreeNodeEdit) {
    					TreeNodeEdit.TreeNodeMenu menu = ((TreeNodeEdit) userObject).getTreeNodeMenu();
    					if (!menu.isVisible()) {
        					menu.show(e.getComponent(), e.getX(), e.getY());
    					}
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
		fileEdit.setEditAsRootOfTree(m_tree);
		addChildrenToNodeOfEdit(fileEdit);
		
		showChildrenOfRoot();
	}
	
	private void showChildrenOfRoot() {
		DefaultMutableTreeNode root = m_editTreeConfig.getFileNodeEdit().getTreeNode();
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
						ColumnNodeEdit columnEdit = new ColumnNodeEdit(spec, (DataSetNodeEdit) parentEdit, i);
						columnEdit.addEditToNode(parentEdit.getTreeNode());
	        			
					} catch (UnsupportedDataTypeException udte) {
						NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
					}
    			}
    		} else if (parentDataSet.getDimensions().length < 2) {
    			try {
					DataColumnSpec spec = new DataColumnSpecCreator("col", parentDataSet.getType().getKnimeType().getColumnDataType()).createSpec();
					ColumnNodeEdit columnEdit = new ColumnNodeEdit(spec, (DataSetNodeEdit) parentEdit, 0);
					columnEdit.addEditToNode(parentEdit.getTreeNode());
        			
				} catch (UnsupportedDataTypeException udte) {
					NodeLogger.getLogger("HDF5 Files").error(udte.getMessage(), udte);
				}
    		}
    	}

		Hdf5TreeElement parentElement = (Hdf5TreeElement) parentObject;
    	try {
    		for (String attributeName : parentElement.loadAttributeNames()) {
    			Hdf5Attribute<?> attribute = parentElement.updateAttribute(attributeName);
    			AttributeNodeEdit attributeEdit = new AttributeNodeEdit(attribute, parentEdit);
    			attributeEdit.addEditToNode(parentEdit.getTreeNode());
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
		m_editTreeConfig.getFileNodeEdit().integrate(editTreeConfig.getFileNodeEdit());
		
		((DefaultTreeModel) (m_tree.getModel())).reload();
		showChildrenOfRoot();
	}
}

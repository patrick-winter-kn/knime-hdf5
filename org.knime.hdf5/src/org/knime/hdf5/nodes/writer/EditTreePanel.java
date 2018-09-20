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

import javax.swing.BorderFactory;
import javax.swing.DropMode;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.TransferHandler;
import javax.swing.border.Border;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;
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
			
			private final String[] stateNames = { "idle", "new", "delete" };
			
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
					edit instanceof AttributeNodeEdit ? 1 :
					edit instanceof DataSetNodeEdit ? 2 :
					edit instanceof FileNodeEdit ? 3 : 
					edit instanceof GroupNodeEdit ? 4 : -1;
			}
			
			private int getStateId(EditAction editAction) {
				return editAction.isCreateOrCopyAction() ? 1 :
					editAction == TreeNodeEdit.EditAction.DELETE ? 2 : 0;
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
					
					if (userObject instanceof TreeNodeEdit) {
						TreeNodeEdit edit = (TreeNodeEdit) userObject;
						setText((edit.getEditAction().isModifyAction() ? "*" : "") + edit.getName());
						tree.setToolTipText(edit.getToolTipText());
						setIcon(icons[getItemId(edit)][getStateId(edit.getEditAction())]);
						
						Border border = null;
						if (edit.isValid()) {
							if (edit instanceof ColumnNodeEdit) {
								ColumnNodeEdit columnEdit = (ColumnNodeEdit) edit;
								if (columnEdit.getEditAction() == EditAction.CREATE && columnEdit.isMaybeInvalid()) {
									border = BorderFactory.createLineBorder(Color.ORANGE);
									tree.setToolTipText(tree.getToolTipText() + " (type is maybe invalid - will be checked directly before execution)");
								}
							}
						} else {
							border = BorderFactory.createLineBorder(Color.RED);
						}
						setBorder(border);
					} else {
						setText(userObject.toString());
					}
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
                
				TreeNodeEdit parentEdit = (TreeNodeEdit) ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject();
                if (parentEdit instanceof ColumnNodeEdit || parentEdit instanceof AttributeNodeEdit) {
                	parentEdit = parentEdit.getParent();
                }
                
                if (data.get(0) instanceof DataColumnSpec) {
                	if (!(parentEdit instanceof DataSetNodeEdit)) {
                		String newName = TreeNodeEdit.getUniqueName(parentEdit.getTreeNode(), "dataSet");
            			DataSetNodeEdit newEdit = new DataSetNodeEdit((GroupNodeEdit) parentEdit, newName);
                    	newEdit.addEditToParentNode();
                    	parentEdit = newEdit;
                	}

                	for (int i = 0; i < data.size(); i++) {
                		DataColumnSpec spec = (DataColumnSpec) data.get(i);
                		ColumnNodeEdit newEdit = new ColumnNodeEdit((DataSetNodeEdit) parentEdit, spec);
                    	newEdit.addEditToParentNode();
                	}
                } else if (data.get(0) instanceof FlowVariable) {
                    for (int i = 0; i < data.size(); i++) {
                    	FlowVariable var = (FlowVariable) data.get(i);
                    	AttributeNodeEdit newEdit = new AttributeNodeEdit((TreeNodeEdit) parentEdit, var);
                    	newEdit.addEditToParentNode();
                	}
                }
				
                parentEdit.reloadTreeWithEditVisible(true);
                
                return true;
			}
			
			private boolean importFromTree(TreePath path) {
				DefaultMutableTreeNode parent = (DefaultMutableTreeNode) path.getLastPathComponent();
            	TreeNodeEdit parentEdit = (TreeNodeEdit) parent.getUserObject();
				TreeNodeEdit newEdit = null;
            	
            	try {
            		for (TreeNodeEdit copyEdit : m_copyEdits) {
    					if (parentEdit instanceof GroupNodeEdit) {
    						GroupNodeEdit parentGroupEdit = (GroupNodeEdit) parentEdit;
    						if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo(parentGroupEdit, false);
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo(parentGroupEdit, false);
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							String newName = TreeNodeEdit.getUniqueName(parent, "dataSet");
    	            			DataSetNodeEdit newDataSetEdit = new DataSetNodeEdit(parentGroupEdit, newName);
    	            			newDataSetEdit.addEditToParentNode();
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) newDataSetEdit, false);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(parentGroupEdit, false);
    						}
                		} else if (parentEdit instanceof DataSetNodeEdit) {
                			DataSetNodeEdit parentDataSetEdit = (DataSetNodeEdit) parentEdit;
                			if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) parentDataSetEdit.getParent(), false);
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) parentDataSetEdit.getParent(), false);
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) parentDataSetEdit, false);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(parentDataSetEdit, false);
    						} 
                		} else if (parentEdit instanceof AttributeNodeEdit) {
                			AttributeNodeEdit parentAttributeEdit = (AttributeNodeEdit) parentEdit;
							TreeNodeEdit grandParentEdit = parentAttributeEdit.getParent();
                			if (copyEdit instanceof GroupNodeEdit) {
                				if (grandParentEdit instanceof GroupNodeEdit) {
        							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) grandParentEdit, false);
                				} else {
        							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) grandParentEdit.getParent(), false);
                				}
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
	            				if (grandParentEdit instanceof GroupNodeEdit) {
	    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) grandParentEdit, false);
	            				} else {
	    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) grandParentEdit.getParent(), false);
	            				}
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
	            				if (grandParentEdit instanceof DataSetNodeEdit) {
	    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) grandParentEdit, false);
	            				} else {
	    							throw new IllegalStateException("There is no dataSet for this column to be added in");
	            				}
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(grandParentEdit, false);
    						} 
                		} else if (parentEdit instanceof ColumnNodeEdit) {
                			ColumnNodeEdit parentColumnEdit = (ColumnNodeEdit) parentEdit;
							TreeNodeEdit grandParentEdit = parentColumnEdit.getParent();
                			if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) grandParentEdit.getParent(), false);
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) grandParentEdit.getParent(), false);
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) grandParentEdit, false);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(grandParentEdit, false);
    						} 
                		}
    					parent = newEdit.getParent().getTreeNode();
    				}
            	} catch (IllegalStateException ise) {
            		NodeLogger.getLogger(getClass()).warn(ise.getMessage());
            		
            	} finally {
            		try {
        				newEdit.reloadTreeWithEditVisible();
            		} catch (Exception e) {
            			e.printStackTrace();
            		}
    				for (TreeNodeEdit copyEdit : m_copyEdits) {
    					m_tree.makeVisible(new TreePath(copyEdit.getTreeNode().getPath()));
    				}
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

	void updateTreeWithResetConfig() throws IOException {
		updateTreeWithFile(m_editTreeConfig.getFileNodeEdit().getFilePath(), false);
	}
	
	void updateTreeWithFile(String filePath, boolean keepConfig) throws IOException {
		Hdf5File file = null;
		
		try {
			FileNodeEdit oldFileEdit = m_editTreeConfig.getFileNodeEdit();
			FileNodeEdit newFileEdit = null;
			if (Hdf5File.existsFile(filePath) && Hdf5File.hasHdf5FileEnding(filePath)) {
				file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
				newFileEdit = new FileNodeEdit(file);
			} else {
				newFileEdit = new FileNodeEdit(filePath);
			}
			m_editTreeConfig.setFileNodeEdit(newFileEdit);
			newFileEdit.setEditAsRootOfTree(m_tree);
			newFileEdit.loadChildrenOfHdfObject();
			if (keepConfig && oldFileEdit != null) {
				newFileEdit.integrate(oldFileEdit, null);
			}
			newFileEdit.reloadTreeWithEditVisible(true);
			
		} finally {
			if (file != null) {
				file.close();
			}
		}
	}

	void saveConfiguration(EditTreeConfiguration editTreeConfig) {
		if (m_editTreeConfig.getFileNodeEdit() != null) {
			editTreeConfig.setFileNodeEdit(m_editTreeConfig.getFileNodeEdit());
		}
	}
	
	void loadConfiguration(EditTreeConfiguration editTreeConfig) {
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		fileEdit.integrate(editTreeConfig.getFileNodeEdit(), null);
		fileEdit.reloadTreeWithEditVisible(true);
	}
}

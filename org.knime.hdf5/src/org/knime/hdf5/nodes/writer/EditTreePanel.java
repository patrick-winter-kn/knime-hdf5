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
import java.util.TreeMap;

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
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;
import org.knime.hdf5.nodes.writer.edit.AttributeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.ColumnNodeEdit;
import org.knime.hdf5.nodes.writer.edit.DataSetNodeEdit;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;
import org.knime.hdf5.nodes.writer.edit.GroupNodeEdit;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.EditAction;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.InvalidCause;
import org.knime.hdf5.nodes.writer.edit.UnsupportedObjectNodeEdit;

/**
 * The {@linkplain JPanel} where the {@linkplain FileNodeEdit} (visualized
 * as {@linkplain JTree}) can be modified.
 */
public class EditTreePanel extends JPanel {

	private static final long serialVersionUID = -9129152385004241047L;

	private EditTreeConfiguration m_editTreeConfig = new EditTreeConfiguration("temp");
	
	private final JTree m_tree = new JTree(new DefaultMutableTreeNode("(null)"));

	public EditTreePanel() {
		setLayout(new BorderLayout());
		setBorder(BorderFactory.createTitledBorder("File:"));
		
		final JScrollPane jsp = new JScrollPane(m_tree);
		add(jsp, BorderLayout.CENTER);
		
		// set the visual representation of all nodes in the tree
    	m_tree.setCellRenderer(new DefaultTreeCellRenderer() {

			private static final long serialVersionUID = -2424225988962935310L;

			private final String[] itemNames = { "column", "attribute", "dataSet", "file", "group" };
			
			private final String[] stateNames = { "idle", "new", "delete", "unsupported" };

			private final Icon[][] icons = loadAllIcons();
			
			private final Icon unsupportedNodeEditIcon = loadIcon(EditTreePanel.class, "/icon/object_unsupported.png");
			
			private Icon[][] loadAllIcons() {
				Icon[][] loadedIcons = new Icon[itemNames.length][stateNames.length];
				
				for (int i = 0; i < itemNames.length; i++) {
					for (int j = 0; j < stateNames.length; j++) {
						String iconName = itemNames[i] + "_" + stateNames[j];
						if (!iconName.equals("file_delete")
								&& (!stateNames[j].equals("unsupported") || itemNames[i].equals("attribute") || itemNames[i].equals("dataSet"))) {
							loadedIcons[i][j] = loadIcon(EditTreePanel.class, "/icon/" + iconName + ".png");
						}
					}
				}
				
				return loadedIcons;
			}
			
			/**
			 * Loads the icon on the relative {@code path} to the path of the
			 * {@code rootClass} (without its name).
			 * 
			 * @param rootClass the class in the root path
			 * @param path the path from the root path
			 * @return the icon in the concatenated path
			 */
			private Icon loadIcon(final Class<?> rootClass, final String path) {
		        ImageIcon icon = null;
		        
		        try {
		            ClassLoader loader = rootClass.getClassLoader();
		            String packagePath = rootClass.getPackage().getName().replace('.', '/');
		            String correctedPath = path;
		            if (!path.startsWith("/")) {
		                correctedPath = "/" + path;
		            }
		            icon = new ImageIcon(loader.getResource(packagePath + correctedPath));
		            
		        } catch (Exception e) {
		            NodeLogger.getLogger(EditTreePanel.class).debug("Unable to load icon at path \""
		            		+ path + "\": " + e.getMessage(), e);
		        }
		        
		        return icon;
		    }
			
			private Icon getEditIcon(TreeNodeEdit edit) {
				int itemId = getItemId(edit);
				return itemId >= 0 ? icons[itemId][getStateId(edit)] : unsupportedNodeEditIcon;
			}
			
			private int getItemId(TreeNodeEdit edit) {
				return edit instanceof ColumnNodeEdit ? 0 : 
					edit instanceof AttributeNodeEdit ? 1 :
					edit instanceof DataSetNodeEdit ? 2 :
					edit instanceof FileNodeEdit ? 3 : 
					edit instanceof GroupNodeEdit ? 4 : -1;
			}
			
			private int getStateId(TreeNodeEdit edit) {
				EditAction editAction = edit.getEditAction();
				return !edit.isSupported() ? 3 :
					editAction.isCreateOrCopyAction() ? 1 :
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
						setIcon(getEditIcon(edit));
						
						Border border = null;
						if (edit.isValid()) {
							if (edit instanceof ColumnNodeEdit) {
								ColumnNodeEdit columnEdit = (ColumnNodeEdit) edit;
								if (columnEdit.getEditAction() == EditAction.CREATE && columnEdit.isMaybeInvalid()) {
									border = BorderFactory.createLineBorder(Color.ORANGE);
									tree.setToolTipText(tree.getToolTipText()
											+ " (type is maybe invalid - will be checked directly before execution)");
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
		
    	// set the drag-and-drop actions of all nodes in the tree
    	m_tree.setDragEnabled(true);
    	m_tree.setTransferHandler(new TransferHandler() {

			private static final long serialVersionUID = -4233815652319877595L;
			
			private List<TreeNodeEdit> m_copyEdits = new ArrayList<>();

			@Override
            public int getSourceActions(JComponent comp) {
                return COPY;
            }

			@Override
			protected Transferable createTransferable(JComponent comp) {
                if (comp instanceof JTree) {
                	JTree tree = (JTree) comp;
                	// store the dragged nodes in the JTree
                	for (TreePath path : tree.getSelectionPaths()) {
        				m_copyEdits.add((TreeNodeEdit) ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject());
                	}
                	return new StringSelection("");
                }
                return null;
            }
			
			@SuppressWarnings("rawtypes")
			@Override
			public boolean canImport(TransferHandler.TransferSupport info) {
				if (!info.isDrop() || !info.isDataFlavorSupported(DataFlavor.stringFlavor)) {
					return false;
				}
				
                JTree.DropLocation dl = (JTree.DropLocation) info.getDropLocation();
                TreePath path = dl.getPath();
                if (path == null) {
					return false;
                }
                
                // an unsupported object cannot be target of a drag-and-drop action
        		DefaultMutableTreeNode parent = (DefaultMutableTreeNode) path.getLastPathComponent();
            	TreeNodeEdit dropLocationEdit = (TreeNodeEdit) parent.getUserObject();
    			if (dropLocationEdit instanceof UnsupportedObjectNodeEdit) {
    				return false;
            	}
    			
    			// load the transfered data
    			String specListKey = null;
            	try {
            		specListKey = (String) info.getTransferable().getTransferData(DataFlavor.stringFlavor);
				} catch (UnsupportedFlavorException | IOException ufioe) {
					return false;
				}
    			
            	/* 
            	 * check if the source of the data is the JTree itself or the
            	 * JLists of the input specs
            	 */
            	if (specListKey.isEmpty()) {
            		for (TreeNodeEdit copyEdit : m_copyEdits) {
            			// check if all edits are supported and if no edits add them to themselves
            			if (!copyEdit.isSupported() || copyEdit instanceof GroupNodeEdit && copyEdit.isEditDescendantOf(dropLocationEdit)) {
            				return false;
            			}
            			
            			// check if the target is supported
            			TreeNodeEdit parentEdit = findEditToAddTo(copyEdit.getClass(), dropLocationEdit, false);
                    	if (parentEdit == null || !parentEdit.isSupported()) {
            				return false;
                    	}
            		}
            		
            		// check if all edits are instances from the same class
            		boolean allEqual = true;
            		if (!m_copyEdits.isEmpty()) {
                		Class editClass = m_copyEdits.get(0).getClass();
                		for (int i = 1; i < m_copyEdits.size(); i++) {
                			allEqual &= editClass == m_copyEdits.get(i).getClass();
                		}
            		}
            		
            		// if not, check if the edits can be pasted anyway
            		if (!allEqual) {
        				boolean pasteToDataSet = dropLocationEdit instanceof DataSetNodeEdit;
        				boolean pasteToGroup = dropLocationEdit instanceof GroupNodeEdit;
        				
        				for (TreeNodeEdit copyEdit : m_copyEdits) {
            				pasteToDataSet &= copyEdit instanceof ColumnNodeEdit || copyEdit instanceof AttributeNodeEdit;
            				pasteToGroup &= copyEdit instanceof DataSetNodeEdit || copyEdit instanceof GroupNodeEdit || copyEdit instanceof AttributeNodeEdit;
        				}

                        return pasteToDataSet || pasteToGroup;
            		}
            	} else {
            		Class<? extends TreeNodeEdit> editClass = specListKey.equals(SpecInfo.COLUMN_SPECS.getSpecName()) ? ColumnNodeEdit.class
            				: (specListKey.equals(SpecInfo.FLOW_VARIABLE_SPECS.getSpecName()) ? AttributeNodeEdit.class : null);

        			// check if the target is supported
            		TreeNodeEdit parentEdit = findEditToAddTo(editClass, dropLocationEdit, false);
            		if (parentEdit == null || !parentEdit.isSupported()) {
						return false;
					}
            		
            		// get the knime input specs to add
            		List<?> data = SpecInfo.get(specListKey).getSpecList();
	            	if (data == null || data.isEmpty()) {
	            		return false;
	            	}
            	}
            	
            	return true;
			}
			
			/**
			 * Finds the next ancestor of {@code dropLocationEdit} (or the
			 * edit itself) where instances of edits with the class copyEditClass
			 * {@code copyEditClass} can be added. Creates a new dataSet edit
			 * (with the name of the first column spec) if
			 * {@code createDataSetForColumns} is {@code true} and the
			 * column cannot be added otherwise.
			 * 
			 * @param copyEditClass the class of edits to add to
			 * @param dropLocationEdit the edit where the user released the
			 *  the mouse
			 * @param createDataSetForColumns if a new dataSet edit may be
			 * 	created 
			 * @return the target edit
			 */
			private TreeNodeEdit findEditToAddTo(Class<? extends TreeNodeEdit> copyEditClass, TreeNodeEdit dropLocationEdit, boolean createDataSetForColumns) {
				if (dropLocationEdit instanceof GroupNodeEdit) {
					if (copyEditClass == GroupNodeEdit.class || copyEditClass == DataSetNodeEdit.class || copyEditClass == AttributeNodeEdit.class) {
						return dropLocationEdit;
						
					} else if (copyEditClass == ColumnNodeEdit.class) {
						if (createDataSetForColumns) {
							// add the new dataSet edit in the group to add the column edit(s) in it
							String newName = TreeNodeEdit.getUniqueName(dropLocationEdit, DataSetNodeEdit.class, "dataSet");
                			DataSetNodeEdit newDataSetEdit = new DataSetNodeEdit((GroupNodeEdit) dropLocationEdit, newName);
                			newDataSetEdit.addEditToParentNodeIfPossible();
                			return newDataSetEdit;
                			
						} else {
							return dropLocationEdit;
						}
					}
        		} else if (dropLocationEdit instanceof DataSetNodeEdit) {
        			// add the dataSet to the parent if it cannot be added here
        			if (copyEditClass == GroupNodeEdit.class || copyEditClass == DataSetNodeEdit.class) {
						return dropLocationEdit.getParent();
						
					} else if (copyEditClass == ColumnNodeEdit.class || copyEditClass == AttributeNodeEdit.class) {
						return dropLocationEdit;
					} 
        		} else if (dropLocationEdit instanceof ColumnNodeEdit || dropLocationEdit instanceof AttributeNodeEdit) {
        			// no edits can be added here, try again for the parent
        			return findEditToAddTo(copyEditClass, dropLocationEdit.getParent(), createDataSetForColumns);
        		}
				
				return null;
			}
			
			@Override
			public boolean importData(TransferHandler.TransferSupport info) {
                JTree.DropLocation dl = (JTree.DropLocation) info.getDropLocation();
                TreePath path = dl.getPath();
                
                Transferable t = info.getTransferable();
                String specListKey = null;
                try {
                	specListKey = (String) t.getTransferData(DataFlavor.stringFlavor);

                } catch (UnsupportedFlavorException | IOException ufioe) {
                	return false;
                }
                
                return specListKey.isEmpty() ? importFromTree(path) : importFromList(path, SpecInfo.get(specListKey));
            }
			
			/**
			 * Imports the knime column specs/flow variables to the target edit.
			 * 
			 * @param path the path to the edit where the user released the mouse
			 * @param specInfo the info if the specs are from columns or flow
			 * 	variables
			 * @return if the import was successful
			 */
			private boolean importFromList(TreePath path, SpecInfo specInfo) {
				TreeNodeEdit dropLocationEdit = (TreeNodeEdit) ((DefaultMutableTreeNode) path.getLastPathComponent()).getUserObject();
				TreeNodeEdit parentEdit = dropLocationEdit;
                if (dropLocationEdit instanceof ColumnNodeEdit || dropLocationEdit instanceof AttributeNodeEdit) {
                	parentEdit = dropLocationEdit.getParent();
                }

        		List<?> data = specInfo.getSpecList();
                if (data.get(0) instanceof DataColumnSpec) {
                	if (!(parentEdit instanceof DataSetNodeEdit)) {
                		String newName = TreeNodeEdit.getUniqueName(parentEdit, DataSetNodeEdit.class, ((DataColumnSpec) data.get(0)).getName());
            			DataSetNodeEdit newEdit = new DataSetNodeEdit((GroupNodeEdit) parentEdit, newName);
                    	newEdit.addEditToParentNodeIfPossible();
                    	parentEdit = newEdit;
                	}

                	for (int i = 0; i < data.size(); i++) {
                		DataColumnSpec spec = (DataColumnSpec) data.get(i);
                		ColumnNodeEdit newEdit = new ColumnNodeEdit((DataSetNodeEdit) parentEdit, spec);
                    	newEdit.addEditToParentNodeIfPossible();
                	}
                } else if (data.get(0) instanceof FlowVariable) {
                    for (int i = 0; i < data.size(); i++) {
                    	FlowVariable var = (FlowVariable) data.get(i);
                    	AttributeNodeEdit newEdit = new AttributeNodeEdit((TreeNodeEdit) parentEdit, var);
                    	newEdit.addEditToParentNodeIfPossible();
                	}
                }
				
                parentEdit.reloadTreeWithEditVisible(true);
                
                return true;
			}

			/**
			 * Imports the source edits to the target edit.
			 * 
			 * @param path the path to the edit where the user released the mouse
			 * @return if the import was successful
			 */
			private boolean importFromTree(TreePath path) {
				DefaultMutableTreeNode parent = (DefaultMutableTreeNode) path.getLastPathComponent();
            	TreeNodeEdit dropLocationEdit = (TreeNodeEdit) parent.getUserObject();
				TreeNodeEdit newEdit = null;
            	
            	try {
            		for (TreeNodeEdit copyEdit : m_copyEdits) {
            			TreeNodeEdit parentEdit = findEditToAddTo(copyEdit.getClass(), dropLocationEdit, true);
            			
            			if (parentEdit != null) {
            				if (copyEdit instanceof GroupNodeEdit) {
    							newEdit = ((GroupNodeEdit) copyEdit).copyGroupEditTo((GroupNodeEdit) parentEdit);
    							
    						} else if (copyEdit instanceof DataSetNodeEdit) {
    							newEdit = ((DataSetNodeEdit) copyEdit).copyDataSetEditTo((GroupNodeEdit) parentEdit);
    							
    						} else if (copyEdit instanceof ColumnNodeEdit) {
    							newEdit = ((ColumnNodeEdit) copyEdit).copyColumnEditTo((DataSetNodeEdit) parentEdit);
    							
    						} else if (copyEdit instanceof AttributeNodeEdit) {
    							newEdit = ((AttributeNodeEdit) copyEdit).copyAttributeEditTo(parentEdit);
    						}
        					parent = newEdit.getParent().getTreeNode();
            			}
    				}
            	} catch (IllegalStateException ise) {
            		NodeLogger.getLogger(getClass()).warn(ise.getMessage());
            		
            	} finally {
            		try {
        				newEdit.reloadTreeWithEditVisible();
            		} catch (Exception e) {
                		NodeLogger.getLogger(getClass()).warn(e.getMessage());
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

    	// add a popup menu for all nodes in the tree
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
    					if (menu != null && !menu.isVisible()) {
        					menu.show(e.getComponent(), e.getX(), e.getY());
    					}
    				}
				}
    		}
		});
	}
	
	/**
	 * @return the {@linkplain JTree} of this panel
	 */
	JTree getTree() {
		return m_tree;
	}
	
	/**
	 * @return the file path of the file edit in the root of the tree
	 */
	String getFilePathOfRoot() {
		return m_editTreeConfig.getFileNodeEdit() != null ? m_editTreeConfig.getFileNodeEdit().getFilePath() : null;
	}

	/**
	 * Resets the file edit in the config and updates the tree.
	 * 
	 * @throws IOException if an hdf object of the hdf file could not be opened/closed
	 */
	void updateTreeWithResetConfig() throws IOException {
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		if (fileEdit != null) {
			updateTreeWithFile(fileEdit.getFilePath(), fileEdit.isOverwriteHdfFile(), false);
		}
	}

	/**
	 * Update this tree with the an initialized config. See
	 * {@linkplain EditTreeConfiguration#initConfigOfFile(String, boolean, boolean, JTree)}
	 * for more information.
	 * 
	 * @param filePath the new file path for the config
	 * @param overwriteFile if the new file edit should be in OVERWRITE policy
	 * @param keepConfig if the old config within the file edit should be kept
	 * @throws IOException if an hdf object of the hdf file could not be
	 * 	opened/closed
	 * @see EditTreeConfiguration#initConfigOfFile(String, boolean, boolean, JTree)
	 */
	void updateTreeWithFile(String filePath, boolean overwriteFile, boolean keepConfig) throws IOException {
		m_editTreeConfig.initConfigOfFile(filePath, overwriteFile, keepConfig, m_tree);
	}
	
	/**
	 * @return all edits in the config that are resettable because they are
	 * 	invalid
	 */
	TreeMap<TreeNodeEdit, InvalidCause> getResettableEdits() {
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		if (fileEdit != null) {
			return fileEdit.getResettableEdits();
		}
		
		return new TreeMap<>();
	}
	
	/**
	 * Resets the {@code resetEdits} of the config back to their initialization
	 * states with their hdf objects.
	 * 
	 * @param resetEdits the edits to be reset
	 */
	void resetEdits(List<TreeNodeEdit> resetEdits) {
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		if (fileEdit != null) {
			fileEdit.resetEdits(resetEdits);
		}
	}
	
	/**
	 * Checks the configuation of this tree
	 * 
	 * @throws InvalidSettingsException if the config is invalid
	 */
	void checkConfiguration() throws InvalidSettingsException {
		m_editTreeConfig.checkConfiguration();
	}

	/**
	 * Saves this config to {@code editTreeConfig}.
	 * 
	 * @param editTreeConfig the config to save this config
	 */
	void saveConfiguration(EditTreeConfiguration editTreeConfig) {
		if (m_editTreeConfig.getFileNodeEdit() != null) {
			editTreeConfig.setFileNodeEdit(m_editTreeConfig.getFileNodeEdit());
		}
	}

	/**
	 * Loads the {@code editTreeConfig} to this config
	 * 
	 * @param editTreeConfig the config to load from
	 */
	void loadConfiguration(EditTreeConfiguration editTreeConfig) {
		FileNodeEdit fileEdit = m_editTreeConfig.getFileNodeEdit();
		fileEdit.integrateAndValidate(editTreeConfig.getFileNodeEdit());
		fileEdit.reloadTreeWithEditVisible(true);
	}
}

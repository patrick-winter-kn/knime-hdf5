package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.WindowConstants;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Attribute;
import org.knime.hdf5.lib.Hdf5TreeElement;

public abstract class TreeNodeEdit {

	public static enum EditAction {
		CREATE("create"),
		COPY("copy"),
		DELETE("delete"),
		MODIFY("modify"),
		NO_ACTION("noAction");
		
		private static final Map<String, EditAction> LOOKUP = new HashMap<>();

		static {
			for (EditAction editAction : EditAction.values()) {
				LOOKUP.put(editAction.getActionName(), editAction);
			}
		}

	    private final String m_actionName;

	    EditAction(final String actionName) {
	    	m_actionName = actionName;
	    }

		public static EditAction get(String actionName) {
			return LOOKUP.get(actionName);
		}

	    public String getActionName() {
	        return m_actionName;
	    }
	    
		public boolean isCreateOrCopyAction() {
			return this == CREATE || this == COPY;
		}
	}
	
	protected static enum SettingsKey {
		NAME("name"),
		INPUT_PATH_FROM_FILE_WITH_NAME("inputPathFromFileWithName"),
		FILE_PATH("filePath"),
		EDIT_ACTION("editAction"),
		KNIME_TYPE("knimeType"),
		HDF_TYPE("hdfType"),
		COMPOUND_AS_ARRAY_POSSIBLE("compoundAsArrayPossible"),
		COMPOUND_AS_ARRAY_USED("compoundAsArrayUsed"),
		LITTLE_ENDIAN("littleEndian"),
		FIXED("fixed"),
		STRING_LENGTH("stringLength"),
		COMPOUND_ITEM_STRING_LENGTH("compoundItemStringLength"),
		COMPRESSION("compression"),
		CHUNK_ROW_SIZE("chunkRowSize"),
		OVERWRITE("overwrite"),
		OVERWRITE_POLICY("overwritePolicy"),
		GROUPS("groups"),
		DATA_SETS("dataSets"),
		ATTRIBUTES("attributes"),
		COLUMNS("columns"),
		COLUMN_SPEC_TYPE("columnSpecType"),
		INPUT_COLUMN_INDEX("inputColumnIndex");

		private String m_key;

		private SettingsKey(String key) {
			m_key = key;
		}
		
		protected String getKey() {
			return m_key;
		}
	}
	
	private String m_inputPathFromFileWithName;
	
	private String m_outputPathFromFile;
	
	private String m_name;

	private TreeNodeMenu m_treeNodeMenu;
	
	protected DefaultMutableTreeNode m_treeNode;
	
	private TreeNodeEdit m_parent;
	
	private Object m_hdfObject;
	
	private EditAction m_editAction = EditAction.NO_ACTION;
	
	protected boolean m_valid;

	TreeNodeEdit(String inputPathFromFileWithName, String outputPathFromFile, String name) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
		m_outputPathFromFile = outputPathFromFile;
		m_name = name;
	}
	
	@SuppressWarnings("unchecked")
	public static String getUniqueName(DefaultMutableTreeNode parent, String name) {
		List<String> usedNames = new ArrayList<>();
		Enumeration<DefaultMutableTreeNode> children = parent.children();
		while (children.hasMoreElements()) {
			usedNames.add(((TreeNodeEdit) children.nextElement().getUserObject()).getName());
		}
		
		return getUniqueName(usedNames, name);
	}
	
	public static String getUniqueName(List<String> usedNames, String name) {
		String newName = name;
		
		if (usedNames.contains(newName)) {
			String oldName = name;
			int i = 1;
			
			if (oldName.matches(".*\\([1-9][0-9]*\\)")) {
				i = Integer.parseInt(oldName.substring(oldName.lastIndexOf("(") + 1, oldName.lastIndexOf(")")));
				oldName = oldName.substring(0, oldName.lastIndexOf("("));
			}
			
			while (usedNames.contains(newName)) {
				newName = oldName + "(" + i + ")";
				i++;
			}
		}
		
		return newName;
	}

	protected static <T extends TreeNodeEdit> boolean doActionsinOrder(T[] edits, BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		List<TreeNodeEdit> deleteEdits = new ArrayList<>();
		List<TreeNodeEdit> modifyEdits = new ArrayList<>();
		List<TreeNodeEdit> otherEdits = new ArrayList<>();
		
		for (TreeNodeEdit edit : edits) {
			if (edit.getEditAction() == EditAction.DELETE) {
				deleteEdits.add(edit);
			} else if (edit.getEditAction() == EditAction.MODIFY) {
				modifyEdits.add(edit);
			} else {
				otherEdits.add(edit);
			}
		}
		
		boolean success = true;
		for (TreeNodeEdit edit : deleteEdits) {
			success &= edit.doAction(inputTable, flowVariables, saveColumnProperties);
		}
		
		List<TreeNodeEdit> secondModifyEdits = new ArrayList<>();
		for (TreeNodeEdit edit : modifyEdits) {
			if (!edit.doAction(inputTable, flowVariables, saveColumnProperties)) {
				secondModifyEdits.add(edit);
			}
		}
		for (TreeNodeEdit edit : secondModifyEdits) {
			success &= edit.doAction(inputTable, flowVariables, saveColumnProperties);
		}
		
		for (TreeNodeEdit edit : otherEdits) {
			success &= edit.doAction(inputTable, flowVariables, saveColumnProperties);
		}
		
		return success;
	}
	
	public String getInputPathFromFileWithName() {
		return m_inputPathFromFileWithName;
	}
	
	void setInputPathFromFileWithName(String inputPathFromFileWithName) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
	}
	
	public String getOutputPathFromFile() {
		return m_outputPathFromFile;
	}

	public String getName() {
		return m_name;
	}
	
	void setName(String name) {
		m_name = name;
	}

	public TreeNodeMenu getTreeNodeMenu() {
		return m_treeNodeMenu;
	}
	
	protected void setTreeNodeMenu(TreeNodeMenu treeNodeMenu) {
		m_treeNodeMenu = treeNodeMenu;
	}
	
	public DefaultMutableTreeNode getTreeNode() {
		return m_treeNode;
	}

	public TreeNodeEdit getParent() {
		return m_parent;
	}
	
	protected void setParent(TreeNodeEdit parent) {
		m_parent = parent;
	}
	
	public Object getHdfObject() {
		return m_hdfObject;
	}

	public void setHdfObject(Hdf5TreeElement hdfObject) {
		m_hdfObject = hdfObject;
	}
	
	protected void setHdfObject(Hdf5Attribute<?> hdfObject) {
		m_hdfObject = hdfObject;
	}

	public EditAction getEditAction() {
		return m_editAction;
	}
	
	public void setEditAction(EditAction editAction) {
		if (editAction != EditAction.MODIFY || m_editAction == EditAction.NO_ACTION) {
			m_editAction = editAction;
			
			if (m_parent != null && m_parent.getEditAction() == EditAction.NO_ACTION && m_editAction != EditAction.NO_ACTION) {
				m_parent.setEditAction(EditAction.MODIFY);
			}
		}
	}

	public boolean isValid() {
		return m_valid;
	}
	
	private void setValid(boolean valid) {
		m_valid = valid;
	}
	
	protected String getOutputPathFromFileWithName() {
		return (m_outputPathFromFile != null ? m_outputPathFromFile + "/" : "") + m_name;
	}
	
	public void reloadTreeWithEditVisible() {
		FileNodeEdit root = getRoot();
		root.reloadTree();
		root.makeTreeNodeVisible(this);
	}
	
	protected FileNodeEdit getRoot() {
		return m_parent != null ? m_parent.getRoot() : (FileNodeEdit) this;
	}
	
	public boolean validate() {
		setValid(getValidation());
		return isValid();
	}
	
	protected void updateParentEditAction() {
		setEditAction(getEditAction());
	}
	
	void setDeletion(boolean isDelete) {
		m_editAction = isDelete ? EditAction.DELETE : EditAction.MODIFY;
		m_treeNodeMenu.setDeleteItemText(isDelete ? "Remove deletion" : "Delete");
	}
	
	protected void saveSettings(NodeSettingsWO settings) {
		settings.addString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey(), m_inputPathFromFileWithName);
		settings.addString(SettingsKey.NAME.getKey(), m_name);
		settings.addString(SettingsKey.EDIT_ACTION.getKey(), m_editAction.getActionName());
	}

	protected void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_editAction = EditAction.get(settings.getString(SettingsKey.EDIT_ACTION.getKey()));
	}
	
	public abstract void addEditToNode(DefaultMutableTreeNode parentNode);
	
	protected boolean getValidation() {
		// TODO improve edit creation before
		/*List<TreeNodeEdit> editsInConflict = new ArrayList<>();
		List<DefaultMutableTreeNode> children = Collections.list(m_treeNode.getParent().children());
		for (DefaultMutableTreeNode child : children) {
			TreeNodeEdit edit = (TreeNodeEdit) child.getUserObject();
			if (isInConflict(edit)) {
				editsInConflict.add(edit);
			}
		}
		if (!editsInConflict.isEmpty()) {
			for (TreeNodeEdit edit : editsInConflict) {
				edit.setValid(false);
			}
			return false;
		}*/
		
		return getEditAction().isCreateOrCopyAction() || getHdfObject() != null;
	}
	
	protected abstract boolean isInConflict(TreeNodeEdit edit);
	
	public boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		switch (m_editAction) {
		case CREATE:
			return createAction(inputTable, flowVariables, saveColumnProperties);
		case COPY:
			return copyAction();
		case DELETE:
			return deleteAction();
		case MODIFY:
			return modifyAction();
		default:
			return true;
		}
	}

	protected abstract boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties);
	protected abstract boolean copyAction();
	protected abstract boolean deleteAction();
	protected abstract boolean modifyAction();
	
	public static abstract class TreeNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = 4973286624577483071L;
		
    	private JMenuItem m_itemDelete;
		
		protected TreeNodeMenu(boolean editable, boolean groupCreatable, boolean deletable) {
			if (editable) {
				JMenuItem itemEdit = new JMenuItem("Edit properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						onEdit();
					}
				});
	    		add(itemEdit);
			}
			
    		if (groupCreatable) {
	    		JMenuItem itemCreateGroup = new JMenuItem("Create new group");
	    		itemCreateGroup.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						onCreateGroup();
					}
				});
	    		add(itemCreateGroup);
    		}

    		if (deletable) {
    			m_itemDelete = new JMenuItem("Delete");
    			m_itemDelete.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						onDelete();
					}
				});
				add(m_itemDelete);
    		}
    	}
		
		private void setDeleteItemText(String text) {
			m_itemDelete.setText(text);
		}

		protected void onEdit() {
			// to implement in subclasses
		}
		
		protected void onCreateGroup() {
			// to implement in subclasses
		}
		
		protected abstract void onDelete();
	}
	
	protected static abstract class PropertiesDialog<Edit extends TreeNodeEdit> extends JDialog {

		private static final long serialVersionUID = -2868431511358917946L;
		
		private final JPanel m_contentPanel = new JPanel();
		
		private final GridBagConstraints m_constraints = new GridBagConstraints();
		
		protected PropertiesDialog(Frame owner, String title) {
			super(owner, title);
			setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
			setLocation(400, 400);
			setModal(true);
			
			JPanel panel = new JPanel(new BorderLayout());
			add(panel, BorderLayout.CENTER);
			panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

			panel.add(m_contentPanel, BorderLayout.CENTER);
			m_contentPanel.setLayout(new GridBagLayout());
			m_constraints.fill = GridBagConstraints.BOTH;
			m_constraints.insets = new Insets(2, 2, 2, 2);
			m_constraints.gridy = 0;
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
			panel.add(buttonPanel, BorderLayout.PAGE_END);
			JButton okButton = new JButton("OK");
			okButton.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					editPropertyItems();
					setVisible(false);
				}
			});
			buttonPanel.add(okButton);
			
			JButton cancelButton = new JButton("Cancel");
			cancelButton.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					setVisible(false);
				}
			});
			buttonPanel.add(cancelButton);

//			m_contentPanel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.BLUE), m_contentPanel.getBorder()));
		}

		protected void addProperty(String description, JComponent component, ChangeListener checkBoxListener, double weighty) {
			PropertyDescriptionPanel propertyPanel = new PropertyDescriptionPanel(description,
					checkBoxListener, Double.compare(weighty, 0.0) != 0);
			m_constraints.gridx = 0;
            m_constraints.weightx = 0.0;
            m_constraints.weighty = weighty;
			m_contentPanel.add(propertyPanel, m_constraints);
            m_constraints.gridx++;
            m_constraints.weightx = 1.0;
            m_contentPanel.add(component, m_constraints);
			m_constraints.gridy++;
		}

		protected void addProperty(String description, JComponent component, ChangeListener checkBoxListener) {
	        addProperty(description, component, checkBoxListener, 0.0);
		}
		
		protected void addProperty(String description, JComponent component, double weighty) {
			addProperty(description, component, null, weighty);
		}
		
		protected void addProperty(String description, JComponent component) {
			addProperty(description, component, null, 0.0);
		}
		
		protected class PropertyDescriptionPanel extends JPanel {

			private static final long serialVersionUID = 3019076429508416644L;
			
			private PropertyDescriptionPanel(String description, ChangeListener checkBoxListener, boolean northwest) {
				setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
				if (checkBoxListener != null) {
					JCheckBox checkBox = new JCheckBox();
					add(checkBox);
					checkBox.addChangeListener(checkBoxListener);
					
					if (northwest) {
						checkBox.setAlignmentY(0.0f);
					}
				}
				JLabel nameLabel = new JLabel(description);
				add(nameLabel);
				if (northwest) {
					nameLabel.setAlignmentY(0.0f);
				}
				
//				setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.GREEN), getBorder()));
//				nameLabel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.RED), nameLabel.getBorder()));
//				component.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.BLUE), component.getBorder()));
			}
		}
		
		protected abstract void initPropertyItems();
		
		protected abstract void editPropertyItems();
	}
}

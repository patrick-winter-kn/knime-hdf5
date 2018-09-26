package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.SwingUtilities;
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

	protected static enum SettingsKey {
		NAME("name"),
		INPUT_PATH_FROM_FILE_WITH_NAME("inputPathFromFileWithName"),
		FILE_PATH("filePath"),
		EDIT_ACTION("editAction"),
		INPUT_TYPE("inputType"),
		OUTPUT_TYPE("outputType"),
		LITTLE_ENDIAN("littleEndian"),
		ROUNDING("rounding"),
		FIXED("fixed"),
		STRING_LENGTH("stringLength"),
		STANDARD_VALUE("standardValue"),
		COMPOUND_AS_ARRAY_POSSIBLE("compoundAsArrayPossible"),
		COMPOUND_AS_ARRAY_USED("compoundAsArrayUsed"),
		COMPOUND_ITEM_STRING_LENGTH("compoundItemStringLength"),
		NUMBER_OF_DIMENSIONS("numberOfDimensions"),
		COMPRESSION("compression"),
		CHUNK_ROW_SIZE("chunkRowSize"),
		OVERWRITE("overwrite"),
		OVERWRITE_POLICY("overwritePolicy"),
		INPUT_ROW_COUNT("inputRowCount"),
		INPUT_COLUMN_INDEX("inputColumnIndex"),
		GROUPS("groups"),
		DATA_SETS("dataSets"),
		ATTRIBUTES("attributes"),
		COLUMNS("columns");

		private String m_key;

		private SettingsKey(String key) {
			m_key = key;
		}
		
		protected String getKey() {
			return m_key;
		}
	}
	
	public static enum EditAction {
		CREATE("create"),
		COPY("copy"),
		DELETE("delete"),
		MODIFY("modify"),
		MODIFY_CHILDREN_ONLY("modifyChildrenOnly"),
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
	    
		public boolean isModifyAction() {
			return this == MODIFY || this == MODIFY_CHILDREN_ONLY;
		}
	}
	
	public static enum InvalidCause {
		FILE_EXTENSION("file extension is not .h5 or .hdf5"),
		FILE_ALREADY_EXISTS("file already exists"),
		NAME_DUPLICATE("name duplicate"),
		NO_HDF_OBJECT("no hdf object available"),
		PARENT_DELETE("parent is getting deleted so this also has to be deleted"),
		NAME_CHARS("name contains invalid characters"),
		ROW_COUNT("dataSet has unequal row sizes"),
		DATA_TYPE("some values do not fit into data type"),
		MISSING_VALUES("there are some missing values");

		private String m_message;

		private InvalidCause(String message) {
			m_message = message;
		}
		
		protected String getMessage() {
			return m_message;
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
	
	private List<TreeNodeEdit> m_incompleteCopies = new ArrayList<>();
	
	private TreeNodeEdit m_copyEdit;
	
	private Map<TreeNodeEdit, InvalidCause> m_invalidEdits = new HashMap<>();

	TreeNodeEdit(String inputPathFromFileWithName, String outputPathFromFile, String name, EditAction editAction) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
		m_outputPathFromFile = outputPathFromFile;
		m_name = name;
		setEditAction(editAction);
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
	
	public String getInputPathFromFileWithName() {
		return m_inputPathFromFileWithName;
	}
	
	void setInputPathFromFileWithName(String inputPathFromFileWithName) {
		m_inputPathFromFileWithName = inputPathFromFileWithName;
		for (TreeNodeEdit copyEdit : m_incompleteCopies) {
			copyEdit.setInputPathFromFileWithName(inputPathFromFileWithName);
		}
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
		m_treeNodeMenu.updateDeleteItemText();
	}
	
	public DefaultMutableTreeNode getTreeNode() {
		return m_treeNode;
	}

	public TreeNodeEdit getParent() {
		return m_parent;
	}
	
	protected void setParent(TreeNodeEdit parent) {
		if (m_parent != parent) {
			if (m_parent != null && parent == null) {
				m_parent.updateInvalidMap(this, null);
				m_parent.removeModifyChildrenProperty();
			}
			m_parent = parent;
			if (m_parent != null) {
				updateParentEditAction();
			}
		}
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
		if (!editAction.isModifyAction() || m_editAction == EditAction.NO_ACTION || m_editAction == EditAction.MODIFY_CHILDREN_ONLY) {
			m_editAction = editAction;
			
			if (m_parent != null && m_parent.getEditAction() == EditAction.NO_ACTION && m_editAction != EditAction.NO_ACTION) {
				m_parent.setEditAction(EditAction.MODIFY_CHILDREN_ONLY);
			}
		}
	}

	private TreeNodeEdit getCopyEdit() {
		return m_copyEdit;
	}
	
	private void setCopyEdit(TreeNodeEdit copyEdit) {
		m_copyEdit = copyEdit;
	}
	
	protected void copyPropertiesFrom(TreeNodeEdit copyEdit) {
		copyCorePropertiesFrom(copyEdit);
		copyAdditionalPropertiesFrom(copyEdit);
	}
	
	protected void copyCorePropertiesFrom(TreeNodeEdit copyEdit) {
		m_name = copyEdit.getName();
		m_inputPathFromFileWithName = copyEdit.getInputPathFromFileWithName();
		m_outputPathFromFile = copyEdit.getOutputPathFromFile();
		setEditAction(copyEdit.getEditAction());
		m_treeNodeMenu.updateDeleteItemText();
		if (getEditAction() == EditAction.COPY) {
			m_parent.addIncompleteCopy(this);
		}
	}
	
	protected abstract void copyAdditionalPropertiesFrom(TreeNodeEdit copyEdit);

	public boolean isValid() {
		return m_invalidEdits.isEmpty();
	}
	
	private void updateInvalidMap(TreeNodeEdit edit, InvalidCause cause) {
		if (cause != null) {
			m_invalidEdits.put(edit, cause);
		} else if (m_invalidEdits.containsKey(edit)) {
			m_invalidEdits.remove(edit);
		}
		if (m_parent != null) {
			m_parent.updateInvalidMap(edit, cause);
		}
	}
	
	protected TreeNodeEdit[] getInvalidsWithCause(InvalidCause cause) {
		List<TreeNodeEdit> edits = new ArrayList<>();
		for (TreeNodeEdit edit : m_invalidEdits.keySet()) {
			if (cause == m_invalidEdits.get(edit)) {
				edits.add(edit);
			}
		}
		return edits.toArray(new TreeNodeEdit[edits.size()]);
	}
	
	public String getInvalidCauseMessages() {
		StringBuilder messageBuilder = new StringBuilder();
		
		TreeNodeEdit[] invalidEdits = m_invalidEdits.keySet().toArray(new TreeNodeEdit[0]);
		Arrays.sort(invalidEdits, new TreeNodeEditComparator());
		int maxMessageCountPerCause = 10;
		for (InvalidCause invalidCause : InvalidCause.values()) {
			int found = 0;
			int count = 0;
			String curInvalidPath = null;
			for (TreeNodeEdit invalidEdit : invalidEdits) {
				if (invalidCause == m_invalidEdits.get(invalidEdit)) {
					if (!invalidEdit.getOutputPathFromFileWithName().equals(curInvalidPath)) {
						found++;
						curInvalidPath = invalidEdit.getOutputPathFromFileWithName();
						if (found <= maxMessageCountPerCause) {
							messageBuilder.append(found == 1 ? invalidCause.getMessage() + ":" : "");
							messageBuilder.append(count > 1 ? " (" + count + ")" : "");
							messageBuilder.append("\n- " + curInvalidPath);
							count = 1;
						}
					} else if (found <= maxMessageCountPerCause) {
						count++;
					}
				}
			}
			messageBuilder.append(count > 1 ? " (" + count + ")" : "");
			messageBuilder.append(found > maxMessageCountPerCause ? "\n- and "
					+ (found - maxMessageCountPerCause) + " more\n" : found > 0 ? "\n" : "");
		}
		
		return messageBuilder.toString();
	}
	
	public String getToolTipText() {
		return getOutputPathFromFileWithName() + (isValid() ? "" : " (invalid"
				+ (m_invalidEdits.containsKey(this) ? " - " + m_invalidEdits.get(this).getMessage() : " children") + ")");
	}
	
	protected void addIncompleteCopy(TreeNodeEdit edit) {
		if (edit.getCopyEdit() != null) {
			edit.getCopyEdit().removeIncompleteCopy(edit);
		}
		m_incompleteCopies.add(edit);
		edit.setCopyEdit(this);
	}
	
	protected void removeIncompleteCopy(TreeNodeEdit edit) {
		m_incompleteCopies.remove(edit);
		edit.setCopyEdit(null);
	}
	
	protected boolean areIncompleteCopiesLeft() {
		return !m_incompleteCopies.isEmpty();
	}
	
	protected void updateIncompleteCopies() {
    	if (m_editAction == EditAction.COPY && m_copyEdit == null) {
    		TreeNodeEdit copyEdit = findCopyEdit();
			if (copyEdit != null) {
				copyEdit.addIncompleteCopy(this);
			}
		}
    	
		for (TreeNodeEdit edit : getAllChildren()) {
			edit.updateIncompleteCopies();
		}
	}
	
	private TreeNodeEdit findCopyEdit() {
		if (this instanceof GroupNodeEdit) {
			return getRoot().getGroupEditByPath(getInputPathFromFileWithName());
		} else if (this instanceof DataSetNodeEdit) {
			return getRoot().getDataSetEditByPath(getInputPathFromFileWithName());
		} else if (this instanceof ColumnNodeEdit) {
			return getRoot().getColumnEditByPath(getInputPathFromFileWithName(), ((ColumnNodeEdit) this).getInputColumnIndex());
		} else if (this instanceof AttributeNodeEdit) {
			return getRoot().getAttributeEditByPath(getInputPathFromFileWithName());
		} 
		return null;
	}
	
	public String getOutputPathFromFileWithName() {
		return (!m_outputPathFromFile.isEmpty() ? m_outputPathFromFile + "/" : "") + m_name;
	}

	public void reloadTreeWithEditVisible() {
		reloadTreeWithEditVisible(false);
	}
	
	public void reloadTreeWithEditVisible(boolean childrenVisible) {
		FileNodeEdit root = getRoot();
		root.reloadTree();
		if (childrenVisible) {
			TreeNodeEdit[] childEdits = getAllChildren();
			root.makeTreeNodeVisible(childEdits.length > 0 ? childEdits[0] : this);
		} else {
			root.makeTreeNodeVisible(this);
		}
	}
	
	protected FileNodeEdit getRoot() {
		return m_parent != null ? m_parent.getRoot() : (FileNodeEdit) this;
	}
	
	private TreeNodeEdit[] getPath() {
		List<TreeNodeEdit> editsInPath = new ArrayList<>();
		editsInPath.add(this);
		while (editsInPath.get(0).getParent() != null) {
			editsInPath.add(0, editsInPath.get(0).getParent());
		}
		return editsInPath.toArray(new TreeNodeEdit[0]);
	}
	
	private void updateParentEditAction() {
		setEditAction(getEditAction());
	}
	
	private void removeModifyChildrenProperty() {
		boolean removeModifyChildrenProperty = m_editAction == EditAction.MODIFY_CHILDREN_ONLY;
		if (removeModifyChildrenProperty) {
			for (TreeNodeEdit child : getAllChildren()) {
				if (child.getEditAction() != EditAction.NO_ACTION) {
					removeModifyChildrenProperty = false;
					break;
				}
			}
		}
		if (removeModifyChildrenProperty) {
			setEditAction(EditAction.NO_ACTION);
			if (m_parent != null) {
				m_parent.removeModifyChildrenProperty();
			}
		}
	}
	
	void setDeletion(boolean isDelete) {
    	if (isDelete && getEditAction().isCreateOrCopyAction() || !isDelete && !isValid() && getEditAction() == EditAction.DELETE) {
    		removeFromParent();
    		
    	} else {
    		m_editAction = isDelete ? EditAction.DELETE : EditAction.MODIFY;
    		updateParentEditAction();
    		m_treeNodeMenu.updateDeleteItemText();
    	}
    	
    	for (TreeNodeEdit edit : getAllChildren()) {
        	edit.setDeletion(isDelete);
    	}
	}

	protected abstract TreeNodeEdit[] getAllChildren();
	
	protected abstract void removeFromParent();
	
	protected void saveSettingsTo(NodeSettingsWO settings) {
		settings.addString(SettingsKey.INPUT_PATH_FROM_FILE_WITH_NAME.getKey(), m_inputPathFromFileWithName);
		settings.addString(SettingsKey.NAME.getKey(), m_name);
		settings.addString(SettingsKey.EDIT_ACTION.getKey(), m_editAction.getActionName());
	}

	protected abstract void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException;

	@SuppressWarnings("unchecked")
	public void addEditToParentNode() {
		DefaultMutableTreeNode parentNode = getParent().getTreeNode();
		if (parentNode != null && !parentNode.isNodeChild(m_treeNode)) {
			if (m_treeNode == null) {
				m_treeNode = new DefaultMutableTreeNode(this);
			}
			
			Enumeration<DefaultMutableTreeNode> enumeration = parentNode.children();
			DefaultMutableTreeNode[] siblings = Collections.list(enumeration).toArray(new DefaultMutableTreeNode[0]);
			int insert;
			for (insert = 0; insert < siblings.length; insert++) {
				TreeNodeEdit edit = (TreeNodeEdit) siblings[insert].getUserObject();
				if (TreeNodeEditComparator.compareProperties(this, edit) < 0) {
					break;
				}
			}
			try {
				parentNode.insert(m_treeNode, insert);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
			
			for (TreeNodeEdit edit : getAllChildren()) {
		        edit.addEditToParentNode();
			}
		}
	}
	
	protected void validate(BufferedDataTable inputTable) {
		InvalidCause cause = validateEditInternal(inputTable);
		
		if (m_parent != null) {
			cause = cause == null && m_parent.getEditAction() == EditAction.DELETE && m_editAction != EditAction.DELETE ? InvalidCause.PARENT_DELETE : cause;
			
			if (cause == null) {
				for (TreeNodeEdit edit : m_parent.getAllChildren()) {
					if (isInConflict(edit)) {
						cause = InvalidCause.NAME_DUPLICATE;
						break;
					}
				}
			}
		}
		
		cause = cause == null && !(this instanceof ColumnNodeEdit) && !m_editAction.isCreateOrCopyAction() && m_hdfObject == null ? InvalidCause.NO_HDF_OBJECT : cause;
		
		// TODO find a way to check if there is a source to copy from, especially when using a file with a different structure than used to save the settings
		// cause = cause == null && m_editAction == EditAction.COPY && m_copyEdit == null ? InvalidCause.NO_COPY_EDIT : cause;
		
		updateInvalidMap(this, cause);

		for (TreeNodeEdit edit : getAllChildren()) {
	        edit.validate(inputTable);
		}
	}
	
	protected abstract InvalidCause validateEditInternal(BufferedDataTable inputTable);
	
	protected abstract boolean isInConflict(TreeNodeEdit edit);
	
	protected boolean doAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		boolean success = false;

		switch (m_editAction) {
		case CREATE:
			success = createAction(inputTable, flowVariables, saveColumnProperties);
			break;
		case COPY:
			success = copyAction();
			if (success && m_copyEdit != null) {
				TreeNodeEdit copyEdit = m_copyEdit;
				m_copyEdit.removeIncompleteCopy(this);
				if (!copyEdit.areIncompleteCopiesLeft() && copyEdit.getEditAction() == EditAction.DELETE) {
					success &= copyEdit.doAction(null, null, false);
				}
			}
			break;
		default:
			success = true;
			break;
		}
		
		doChildActionsInOrder(inputTable, flowVariables, saveColumnProperties);
		
		switch (m_editAction) {
		case DELETE:
			success &= deleteAction();
			break;
		case MODIFY:
			success &= modifyAction();
			break;
		default:
			break;
		}
		
		return success;
	}

	private boolean doChildActionsInOrder(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties) {
		List<TreeNodeEdit> deleteEdits = new ArrayList<>();
		List<TreeNodeEdit> modifyEdits = new ArrayList<>();
		List<TreeNodeEdit> otherEdits = new ArrayList<>();
		
		for (TreeNodeEdit edit : getAllChildren()) {
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
			if (!edit.areIncompleteCopiesLeft()) {
				success &= edit.doAction(null, null, false);
			}
		}
		
		List<TreeNodeEdit> secondModifyEdits = new ArrayList<>();
		for (TreeNodeEdit edit : modifyEdits) {
			if (!edit.doAction(inputTable, flowVariables, saveColumnProperties)) {
				// TODO only add it to here if the action failed due to name duplicate (which is resolved through another modifyAction), not in other fails like "inappropriate type"
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

	protected abstract boolean createAction(BufferedDataTable inputTable, Map<String, FlowVariable> flowVariables, boolean saveColumnProperties);
	protected abstract boolean copyAction();
	protected abstract boolean deleteAction();
	protected abstract boolean modifyAction();
	
	private static class TreeNodeEditComparator implements Comparator<TreeNodeEdit> {
		
		@Override
		public int compare(TreeNodeEdit o1, TreeNodeEdit o2) {
			if (o1.getParent() == o2.getParent()) {
				return compareProperties(o1, o2);
				
			} else {
				TreeNodeEdit[] path1 = o1.getPath();
				TreeNodeEdit[] path2 = o2.getPath();
				int minLength = Math.min(path1.length, path2.length);
				int compare = 0;
				for (int i = 0; compare == 0 && i < minLength; i++) {
					compare = compareProperties(path1[i], path2[i]);
				}
				return compare == 0 ? Integer.compare(path1.length, path2.length) : compare;
			}
		}
		
		private static int compareProperties(TreeNodeEdit o1, TreeNodeEdit o2) {
			int compare = Integer.compare(getClassValue(o1), getClassValue(o2));
			if (o1 instanceof ColumnNodeEdit && o2 instanceof ColumnNodeEdit) {
				return (o1.getEditAction() == EditAction.DELETE) == (o2.getEditAction() == EditAction.DELETE) ? 0
						: (o1.getEditAction() == EditAction.DELETE ? 1 : -1);
			} else {
				return compare == 0 ? o1.getName().compareTo(o2.getName()) : compare;
			}
		}
		
		private static int getClassValue(TreeNodeEdit o) {
			return o.getClass() == GroupNodeEdit.class ? 0 : 
				(o.getClass() == DataSetNodeEdit.class ? 1 :
				(o.getClass() == ColumnNodeEdit.class ? 2 :
				(o.getClass() == AttributeNodeEdit.class ? 3 : 4)));
		}
	}
	
	public abstract class TreeNodeMenu extends JPopupMenu {

		private static final long serialVersionUID = 4973286624577483071L;

    	private PropertiesDialog m_propertiesDialog;
		
    	private JMenuItem m_itemDelete;
		
		protected TreeNodeMenu(boolean editable, boolean groupCreatable, boolean deletable) {
			if (editable) {
				JMenuItem itemEdit = new JMenuItem("Edit properties");
	    		itemEdit.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						if (m_propertiesDialog == null) {
							m_propertiesDialog = getPropertiesDialog();
						}
						
						m_propertiesDialog.loadFromEdit();
						m_propertiesDialog.setVisible(true);
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
    			m_itemDelete = new JMenuItem();
    			updateDeleteItemText();
    			m_itemDelete.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						int dialogResult = JOptionPane.showConfirmDialog(TreeNodeMenu.this,
								getDeleteConfirmMessage(), "Warning", JOptionPane.YES_NO_OPTION);
						if (dialogResult == JOptionPane.YES_OPTION){
							onDelete();
						}
					}
				});
				add(m_itemDelete);
    		}
    	}
		
		private void updateDeleteItemText() {
			if (m_itemDelete != null) {
				m_itemDelete.setText(m_editAction != EditAction.DELETE ? "Delete" : "Remove deletion");
			}
		}
		
		private String getDeleteConfirmMessage() {
			return "Are you sure to "
					+ (m_editAction != EditAction.DELETE ? "delete" : "remove the deletion of")
					+ " this object and all its descendants?";
		}

		protected PropertiesDialog getPropertiesDialog() {
			// to implement in subclasses
			return null;
		}
		
		protected void onCreateGroup() {
			// to implement in subclasses
		}
		
		protected abstract void onDelete();
	}
	
	protected static abstract class PropertiesDialog extends JDialog {

		private static final long serialVersionUID = -2868431511358917946L;
		
		private final JPanel m_contentPanel = new JPanel();
		
		private final GridBagConstraints m_constraints = new GridBagConstraints();
		
		protected PropertiesDialog(Component comp, String title) {
			super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, comp), title, true);
			setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
			setLocation(400, 400);
			
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
					saveToEdit();
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
		}

		protected JCheckBox addProperty(String description, JComponent component, ChangeListener checkBoxListener, double weighty) {
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
			
			return propertyPanel.getCheckBox();
		}

		protected JCheckBox addProperty(String description, JComponent component, ChangeListener checkBoxListener) {
	        return addProperty(description, component, checkBoxListener, 0.0);
		}
		
		protected void addProperty(String description, JComponent component, double weighty) {
			addProperty(description, component, null, weighty);
		}
		
		protected void addProperty(String description, JComponent component) {
			addProperty(description, component, null, 0.0);
		}
		
		protected class PropertyDescriptionPanel extends JPanel {

			private static final long serialVersionUID = 3019076429508416644L;
			
			private JCheckBox m_checkBox;
			
			private PropertyDescriptionPanel(String description, ChangeListener checkBoxListener, boolean northwest) {
				setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
				if (checkBoxListener != null) {
					m_checkBox = new JCheckBox();
					add(m_checkBox);
					m_checkBox.addChangeListener(checkBoxListener);
					
					if (northwest) {
						m_checkBox.setAlignmentY(0.0f);
					}
				}
				JLabel nameLabel = new JLabel(description);
				add(nameLabel);
				if (northwest) {
					nameLabel.setAlignmentY(0.0f);
				}
			}
			
			private JCheckBox getCheckBox() {
				return m_checkBox;
			}
		}
		
		protected abstract void loadFromEdit();
		
		protected abstract void saveToEdit();
	}
}

package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.WindowConstants;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5TreeElement;

public abstract class TreeNodeEdit {

	protected static enum SettingsKey {
		NAME("name"),
		PATH_FROM_FILE("pathFromFile"),
		KNIME_TYPE("knimeType"),
		HDF_TYPE("hdfType"),
		LITTLE_ENDIAN("littleEndian"),
		FIXED("fixed"),
		STRING_LENGTH("stringLength"),
		COMPRESSION("compression"),
		CHUNK_ROW_SIZE("chunkRowSize"),
		OVERWRITE("overwrite"),
		OVERWRITE_POLICY("overwritePolicy"),
		GROUPS("groups"),
		DATA_SETS("dataSets"),
		ATTRIBUTES("attributes"),
		COLUMNS("columns"),
		FLOW_VARIABLE_NAME("flowVariableName"),
		COLUMN_SPEC_TYPE("columnSpecType");

		private String m_key;

		private SettingsKey(String key) {
			m_key = key;
		}
		
		protected String getKey() {
			return m_key;
		}
	}
	
	private final String m_pathFromFile;
	
	private String m_name;
	
	protected DefaultMutableTreeNode m_treeNode;
	
	protected boolean m_valid;

	TreeNodeEdit(DefaultMutableTreeNode parent, String name) {
		this(getPathFromFileFromParent(parent), name);
	}
	
	TreeNodeEdit(String name) {
		m_pathFromFile = null;
		m_name = name;
	}
	
	TreeNodeEdit(String pathFromFile, String name) {
		m_pathFromFile = pathFromFile;
		m_name = name;
	}

	private static String getPathFromFileFromParent(DefaultMutableTreeNode parent) {
		String pathFromFile = "";
		for (TreeNode treeNode : parent.getPath()) {
			Object userObject = ((DefaultMutableTreeNode) treeNode).getUserObject();
			if (userObject instanceof Hdf5TreeElement) {
				Hdf5TreeElement treeElement = (Hdf5TreeElement) userObject;
				if (!treeElement.isFile()) {
					pathFromFile += treeElement.getName() + "/";
				}
			} else if (userObject instanceof TreeNodeEdit) {
				pathFromFile += ((TreeNodeEdit) userObject).getName() + "/";
			}
		}
		return pathFromFile;
	}

	public String getPathFromFile() {
		return m_pathFromFile;
	}

	public String getName() {
		return m_name;
	}
	
	public void setName(String name) {
		m_name = name;
	}
	public DefaultMutableTreeNode getTreeNode() {
		return m_treeNode;
	}

	public boolean isValid() {
		return m_valid;
	}
	
	private void setValid(boolean valid) {
		m_valid = valid;
	}
	
	public boolean validate() {
		setValid(getValidation());
		return isValid();
	}
	
	public String getPathFromFileWithoutEndSlash() {
		return !m_pathFromFile.isEmpty() ? m_pathFromFile.substring(0, m_pathFromFile.length() - 1) : "";
	}
	
	public void saveSettings(NodeSettingsWO settings) {
		settings.addString(SettingsKey.NAME.getKey(), m_name);
	}

	public static TreeNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		throw new InvalidSettingsException("invalid subclass (must override this method)");
	}
	
	public abstract void addEditToNode(DefaultMutableTreeNode parentNode);
	
	@SuppressWarnings("unchecked")
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
		
		return true;
	}
	
	protected abstract boolean isInConflict(TreeNodeEdit edit);
	
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
		
		protected abstract void initPropertyItems(Edit edit);
		
		protected abstract void editPropertyItems();
	}
}

package org.knime.hdf5.nodes.writer.edit;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.WindowConstants;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.lib.Hdf5TreeElement;

public abstract class TreeNodeEdit {
	
	private final String m_pathFromFile;
	
	private String m_name;

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
	
	public String getPathFromFileWithoutEndSlash() {
		return !m_pathFromFile.isEmpty() ? m_pathFromFile.substring(0, m_pathFromFile.length() - 1) : "";
	}
	
	public void saveSettings(NodeSettingsWO settings) {
		settings.addString("name", m_name);
	}

	public static TreeNodeEdit loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		throw new InvalidSettingsException("invalid subclass (must inherit this method)");
	}
	
	public abstract void addEditToNode(DefaultMutableTreeNode parentNode);
	
	protected static abstract class PropertiesDialog<Edit extends TreeNodeEdit> extends JDialog {

		private static final long serialVersionUID = -2868431511358917946L;
		
		private final JPanel m_contentPanel = new JPanel();
		
		private final GridBagConstraints m_constraints = new GridBagConstraints();
		
		protected PropertiesDialog() {}
		
		protected PropertiesDialog(Frame owner, String title) {
			super(owner, title);
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			setLocation(400, 400);
			
			JPanel panel = new JPanel(new BorderLayout());
			add(panel, BorderLayout.CENTER);
			panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

			panel.add(m_contentPanel, BorderLayout.PAGE_START);
			// TODO also use GridbagLayout adding the elements of PropertyPanel instances
			m_contentPanel.setLayout(new GridBagLayout());
			m_constraints.weightx = 1;
			m_constraints.fill = GridBagConstraints.HORIZONTAL;
			m_constraints.gridwidth = GridBagConstraints.REMAINDER;
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
			panel.add(buttonPanel, BorderLayout.PAGE_END);
			JButton okButton = new JButton("OK");
			okButton.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					editProperties();
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
		
		protected void addPropertyPanel(String description, JComponent component, boolean hasCheckBox) {
			m_contentPanel.add(new PropertyPanel(description, component, hasCheckBox), m_constraints);
		}
		
		protected abstract void initProperties(Edit edit);
		
		protected abstract void editProperties();
		
		private class PropertyPanel extends JPanel {

			private static final long serialVersionUID = 3019076429508416644L;
			
			private PropertyPanel(String description, JComponent component, boolean hasCheckBox) {
				setLayout(new BoxLayout(this, BoxLayout.X_AXIS));

				JPanel descriptionPanel = new JPanel();
				add(descriptionPanel);
				if (hasCheckBox) {
					JCheckBox checkBox = new JCheckBox();
					descriptionPanel.add(checkBox, BorderLayout.WEST);
				}
				JLabel nameLabel = new JLabel(description);
				descriptionPanel.add(nameLabel, BorderLayout.CENTER);
				
				add(Box.createHorizontalGlue());
				add(component);
				
				setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.RED), getBorder()));
				nameLabel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.GREEN), nameLabel.getBorder()));
				descriptionPanel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.BLUE), descriptionPanel.getBorder()));
				component.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.BLUE), component.getBorder()));
			}
		}
	}
}

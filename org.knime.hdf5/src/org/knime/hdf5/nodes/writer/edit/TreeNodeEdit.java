package org.knime.hdf5.nodes.writer.edit;

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
}

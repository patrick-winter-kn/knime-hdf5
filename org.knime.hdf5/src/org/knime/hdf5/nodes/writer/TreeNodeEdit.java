package org.knime.hdf5.nodes.writer;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

class TreeNodeEdit {

	public static final int HDF5_GROUP = 1;
	
	public static final int HDF5_DATASET = 2;
	
	public static final int DATA_COLUMN_SPEC = 3;
	
	public static final int FLOW_VARIABLE = 4;
	
	private final String m_pathFromFile;
	
	private final String m_name;
	
	private final DataType m_dataType;
	
	private final int m_nodeValueClassId;
	
	TreeNodeEdit(TreeNode[] parentGroupPath, Hdf5Group childGroup) {
		this(parentGroupPath, childGroup.getName(), null, HDF5_GROUP);
	}
	
	TreeNodeEdit(TreeNode[] dataSetPath, DataColumnSpec spec) {
		this(dataSetPath, spec.getName(), spec.getType(), DATA_COLUMN_SPEC);
	}
	
	TreeNodeEdit(TreeNode[] treeElementPath, FlowVariable var) {
		this(treeElementPath, var.getName(), Hdf5KnimeDataType.getColumnDataType(var.getType()), FLOW_VARIABLE);
	}

	TreeNodeEdit(TreeNode[] pathFromFile, String name, DataType dataType, int nodeValueClassId) {
		this(getPathFromFileFromTreePath(pathFromFile), name, dataType, nodeValueClassId);
	}
	
	TreeNodeEdit(String pathFromFile, String name, DataType dataType, int nodeValueClassId) {
		m_pathFromFile = pathFromFile;
		m_name = name;
		m_dataType = dataType;
		m_nodeValueClassId = nodeValueClassId;
	}

	static String getPathFromFileFromTreePath(TreeNode[] treePath) {
		String pathFromFile = "";
		for (TreeNode treeNode : treePath) {
			Object userObject = ((DefaultMutableTreeNode) treeNode).getUserObject();
			if (userObject instanceof Hdf5TreeElement) {
				Hdf5TreeElement treeElement = (Hdf5TreeElement) userObject;
				if (!treeElement.isFile()) {
					pathFromFile += treeElement.getName() + "/";
				}
			} else {
				pathFromFile += userObject.toString() + "/";
			}
		}
		return pathFromFile;
	}

	String getPathFromFile() {
		return m_pathFromFile;
	}
	
	String getName() {
		return m_name;
	}
	
	DataType getDataType() {
		return m_dataType;
	}

	int getNodeValueClassId() {
		return m_nodeValueClassId;
	}
}

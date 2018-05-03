package org.knime.hdf5.nodes.writer;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5TreeElement;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

class TreeNodeEdit {

	enum EditClass {
		DATA_COLUMN_SPEC(1),
		FLOW_VARIABLE(2),
		HDF5_DATASET(3),
		HDF5_GROUP(4);
		private static final Map<Integer, EditClass> LOOKUP = new HashMap<>();
		
		static {
			for (EditClass s : EnumSet.allOf(EditClass.class)) {
				LOOKUP.put(s.getClassId(), s);
			}
		}
		
		private int m_classId;
		
		private EditClass(int classId) {
			m_classId = classId;
		}
		
		static EditClass get(int classId) {
			return LOOKUP.get(classId);
		}
		
		int getClassId() {
			return m_classId;
		}
	}
	
	private final String m_pathFromFile;
	
	private String m_name;
	
	private final DataType m_dataType;
	
	private final EditClass m_editClass;
	
	TreeNodeEdit(TreeNode[] parentGroupPath, Hdf5Group childGroup) {
		this(parentGroupPath, childGroup.getName(), null, EditClass.HDF5_GROUP);
	}
	
	TreeNodeEdit(TreeNode[] dataSetPath, DataColumnSpec spec) {
		this(dataSetPath, spec.getName(), spec.getType(), EditClass.DATA_COLUMN_SPEC);
	}
	
	TreeNodeEdit(TreeNode[] treeElementPath, FlowVariable var) {
		this(treeElementPath, var.getName(), Hdf5KnimeDataType.getColumnDataType(var.getType()), EditClass.FLOW_VARIABLE);
	}

	TreeNodeEdit(TreeNode[] pathFromFile, String name, DataType dataType, EditClass editClass) {
		this(getPathFromFileFromTreePath(pathFromFile), name, dataType, editClass);
	}
	
	TreeNodeEdit(String pathFromFile, String name, DataType dataType, EditClass editClass) {
		m_pathFromFile = pathFromFile;
		m_name = name;
		m_dataType = dataType;
		m_editClass = editClass;
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
			} else if (userObject instanceof TreeNodeEdit) {
				pathFromFile += ((TreeNodeEdit) userObject).getName() + "/";
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
	
	void setName(String name) {
		m_name = name;
	}
	
	DataType getDataType() {
		return m_dataType;
	}

	EditClass getEditClass() {
		return m_editClass;
	}
}

package org.knime.hdf5.nodes.writer;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.hdf5.nodes.writer.TreeNodeEdit.EditClass;

class EditTreeConfiguration {

	private static final String EDIT_TREE_CONFIG_KEY = "_EditTreeConfig";

	private static final String PATHS_FROM_FILE = "pathsFromFile" + EDIT_TREE_CONFIG_KEY;

	private static final String NAMES = "names" + EDIT_TREE_CONFIG_KEY;
	
	private static final String DATATYPES = "dataTypes" + EDIT_TREE_CONFIG_KEY;
	
	private static final String NODE_VALUE_CLASS_IDS = "nodeValueClassIds" + EDIT_TREE_CONFIG_KEY;
	
	private final String m_configRootName;
	
	private final List<TreeNodeEdit> m_editList = new ArrayList<>();
	
	EditTreeConfiguration(String configRootName) {
		m_configRootName = configRootName;
	}
	
	TreeNodeEdit[] getEdits() {
		return m_editList.toArray(new TreeNodeEdit[] {});
	}
	
	void addTreeNodeEdit(TreeNodeEdit edit) {
		m_editList.add(edit);
	}
	
	boolean removeTreeNodeEdit(TreeNodeEdit edit) {
		return m_editList.remove(edit);
	}
	
	void saveConfiguration(NodeSettingsWO settings) {
        NodeSettingsWO subSettings = settings.addNodeSettings(m_configRootName);

        String[] paths = new String[m_editList.size()];
        String[] names = new String[paths.length];
        DataType[] types = new DataType[paths.length];
        int[] classIds = new int[paths.length];
        
        for (int i = 0; i < paths.length; i++) {
        	TreeNodeEdit edit = m_editList.get(i);
        	paths[i] = edit.getPathFromFile();
        	names[i] = edit.getName();
        	types[i] = edit.getDataType();
        	classIds[i] = edit.getEditClass().getClassId();
        }

        subSettings.addStringArray(PATHS_FROM_FILE, paths);
        subSettings.addStringArray(NAMES, names);
        subSettings.addDataTypeArray(DATATYPES, types);
        subSettings.addIntArray(NODE_VALUE_CLASS_IDS, classIds);
	}
	
	void loadConfigurationInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        NodeSettingsRO subSettings = settings.getNodeSettings(m_configRootName);

        String[] paths = subSettings.getStringArray(PATHS_FROM_FILE);
        String[] names = subSettings.getStringArray(NAMES);
        DataType[] types = subSettings.getDataTypeArray(DATATYPES);
        int[] classIds = subSettings.getIntArray(NODE_VALUE_CLASS_IDS);
        
        m_editList.clear();
        for (int i = 0; i < paths.length; i++) {
        	m_editList.add(new TreeNodeEdit(paths[i], names[i], types[i], EditClass.get(classIds[i])));
        }
    }
	
	void loadConfigurationInDialog(final NodeSettingsRO settings) {
		
	}
}

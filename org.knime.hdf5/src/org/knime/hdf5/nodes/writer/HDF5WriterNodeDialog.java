package org.knime.hdf5.nodes.writer;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.activation.UnsupportedDataTypeException;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTree;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelString m_groupPathSettings;
	
	private SettingsModelString m_groupNameSettings;

	private DefaultListModel<DataColumnSpec> m_dataSetSpecModel = new DefaultListModel<>();
	
	private DefaultListModel<DataColumnSpec> m_attributeSpecModel = new DefaultListModel<>();
	
    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5WriterNodeDialog() {
		createFileChooser();
		createGroupEntryBox();
		
		JPanel panel = new JPanel();
		addTab("Data", panel);
		
		JPanel select = new JPanel();
		panel.add(select);

		JList<DataColumnSpec> colList = new JList<>(m_dataSetSpecModel);
		select.add(colList);
        
		JList<DataColumnSpec> varList = new JList<>(m_attributeSpecModel);
		select.add(varList);
        
		JPanel output = new JPanel();
		panel.add(output);
		
		DefaultMutableTreeNode root = new DefaultMutableTreeNode("group");
		JTree tree = new JTree(root);
		tree.setDragEnabled(true);
		output.add(tree);
        
		JButton createDsButton = new JButton("createDs");
		createDsButton.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				DefaultMutableTreeNode newChild = new DefaultMutableTreeNode("dataSet[" + root.getChildCount() + "]");
				root.add(newChild);
				((DefaultTreeModel) (tree.getModel())).reload();
				tree.makeVisible(new TreePath(newChild.getPath()));
			}
		});
		output.add(createDsButton);
		
		JButton addToDsButton = new JButton("addToDs");
		addToDsButton.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				DefaultMutableTreeNode newChild = new DefaultMutableTreeNode(colList.getSelectedValue());
				newChild.setAllowsChildren(false);
				((DefaultMutableTreeNode) tree.getSelectionPath().getLastPathComponent()).add(newChild);
				((DefaultTreeModel) (tree.getModel())).reload();
				tree.makeVisible(new TreePath(newChild.getPath()));
			}
		});
		output.add(addToDsButton);
	}
    
	private void createFileChooser() {
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "outputFilePathHistory",
				JFileChooser.SAVE_DIALOG, false, filePathFvm, ".h5|.hdf5");
		fileChooser.setBorderTitle("Output file:");
		addDialogComponent(fileChooser);
		fileChooser.getModel().addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				
			}
		});
	}
	
	private void createGroupEntryBox() {
		// Border groupBorder = BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output group:");

		m_groupPathSettings = SettingsFactory.createGroupPathSettings();
		DialogComponentString groupPathSettings = new DialogComponentString(m_groupPathSettings,
				"Path to output group");
		addDialogComponent(groupPathSettings);
		
		m_groupNameSettings = SettingsFactory.createGroupNameSettings();
		DialogComponentString groupNameSettings = new DialogComponentString(m_groupNameSettings,
				"Output group");
		addDialogComponent(groupNameSettings);
	}
    
    /**
     * Calls the update method of the underlying filter panel.
     * @param settings the node settings to read from
     * @param specs the input specs
     * @throws NotConfigurableException if no columns are available for
     *             filtering
     */
    @Override
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
    	for (int i = 0; i < specs[0].getNumColumns(); i++) {
        	m_dataSetSpecModel.addElement(specs[0].getColumnSpec(i));
    	}

    	FlowVariable[] flowVariables = getAvailableFlowVariables().values().toArray(new FlowVariable[] {});
		for (FlowVariable flowVariable : flowVariables) {
			try {
				m_attributeSpecModel.addElement(new DataColumnSpecCreator(flowVariable.getName(), Hdf5KnimeDataType.getColumnDataType(flowVariable.getType())).createSpec());
				
			} catch (UnsupportedDataTypeException udte) {
				NodeLogger.getLogger("HDF5 Files").error("Type of FlowVariable \"" + flowVariable.getName() + "\" is not supported", udte);
			}
		}
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {}
}

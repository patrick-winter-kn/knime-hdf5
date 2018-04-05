package org.knime.hdf5.nodes.writer;

import javax.swing.JFileChooser;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelString m_groupPathSettings;
	
	private SettingsModelString m_groupNameSettings;
	
    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5WriterNodeDialog() {
		createFileChooser();
		createGroupEntryBox();
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
            final DataTableSpec[] specs) throws NotConfigurableException {}
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {}
}

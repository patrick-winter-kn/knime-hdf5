package org.knime.hdf5.nodes.reader;

import java.util.Iterator;

import javax.swing.BorderFactory;
import javax.swing.JFileChooser;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

	private final DataColumnSpecFilterPanel m_dsFilterPanel;

    private DataColumnSpecFilterConfiguration m_dsConf;

    private DataTableSpec m_dsSpec;
    
	private final DataColumnSpecFilterPanel m_attrFilterPanel;

    private DataColumnSpecFilterConfiguration m_attrConf;
    
    private DataTableSpec m_attrSpec;
    
    private SettingsModelString m_fcSource;
    
    private SettingsModelBoolean m_firsdSource;

    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5ReaderNodeDialog() {
    	createFileChooser();
        
        m_dsFilterPanel = new DataColumnSpecFilterPanel();
        addTab("DataSet Selector", m_dsFilterPanel);
        
        m_attrFilterPanel = new DataColumnSpecFilterPanel();
        addTab("Attribute Selector", m_attrFilterPanel);

        m_firsdSource = SettingsFactory.createFirsdSourceSettings();
    	DialogComponentBoolean failIfRowSizeDiffers = new DialogComponentBoolean(m_firsdSource, "Fail if rowSize differs");
    	failIfRowSizeDiffers.getComponentPanel().setBorder(
    			BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Advanced settings:"));
    	addDialogComponent(failIfRowSizeDiffers);
    }
    
    private void createFileChooser() {
    	m_fcSource = SettingsFactory.createFcSourceSettings();
    	FlowVariableModel fcSourceFvm = super.createFlowVariableModel(m_fcSource);
    	DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_fcSource,
    			"fcSourceHistory", JFileChooser.OPEN_DIALOG, false, fcSourceFvm, ".h5");
    	fileChooser.setBorderTitle("Input file:");
    	addDialogComponent(fileChooser);
        fileChooser.getModel().addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
		        String newValue = m_fcSource.getStringValue();
		        
		        if (!newValue.equals("")) {
		        	Hdf5File file = null;
		        	
		        	try {
			        	file = Hdf5File.createFile(newValue);
			        	updateConfigs(file, null);
			        	
		        	} finally {
		        		file.close();
		        	}
		        }
			}
        });
    }
    
    private void updateConfigs(Hdf5File file, final NodeSettingsRO settings) {
    	updateDataSetConfig(file, settings);
		updateAttributeConfig(file, settings);
    }
    
    private void updateDataSetConfig(Hdf5File file, final NodeSettingsRO settings) {
    	/*NodeSettings set = null;
    	if (m_dsConf != null) {
        	set = new NodeSettings(file.getFilePath());
        	m_dsSpec.save(set.addConfig("config"));
    	}*/
    	
    	m_dsSpec = file.createSpecOfDataSets();
    	
        if (m_dsConf == null) {
            m_dsConf = HDF5ReaderNodeModel.createDsFilterPanelConfiguration();
            m_dsConf.loadDefaults(m_dsSpec, false);
            
        } else if (settings == null) {
            m_dsConf.loadDefaults(m_dsSpec, m_dsConf.isEnforceExclusion());
            /*
            try {
            	if (set != null) {
                	m_dsConf.loadDefaults(DataTableSpec.load(set.getConfig("config")), m_dsConf.isEnforceInclusion());
            	}
			} catch (InvalidSettingsException ise) {}*/
            
        } else {
	        m_dsConf.loadConfigurationInDialog(settings, m_dsSpec);
        }
    	
        m_dsFilterPanel.loadConfiguration(m_dsConf, m_dsSpec);
    }
    
    private void updateAttributeConfig(Hdf5File file, final NodeSettingsRO settings) {
    	m_attrSpec = file.createSpecOfAttributes();
    	
        if (m_attrConf == null) {
        	m_attrConf = HDF5ReaderNodeModel.createAttrFilterPanelConfiguration();
            m_attrConf.loadDefaults(m_attrSpec, true);
            
        } else if (settings == null) {
            m_attrConf.loadDefaults(m_attrSpec, m_attrConf.isEnforceExclusion());
        
        } else {
	        m_attrConf.loadConfigurationInDialog(settings, m_attrSpec);
        }
        
        m_attrFilterPanel.loadConfiguration(m_attrConf, m_attrSpec);
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
    	try {
			m_fcSource.loadSettingsFrom(settings);
			m_firsdSource.loadSettingsFrom(settings);

			Hdf5File file = Hdf5File.createFile(m_fcSource.getStringValue());
	    	updateConfigs(file, settings);
	    	
		} catch (InvalidSettingsException ise) {}
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	m_fcSource.saveSettingsTo(settings);
    	m_firsdSource.saveSettingsTo(settings);
    	
        DataColumnSpecFilterConfiguration dsConf = HDF5ReaderNodeModel.createDsFilterPanelConfiguration();
        m_dsFilterPanel.saveConfiguration(dsConf);
        dsConf.saveConfiguration(settings);
        
        DataColumnSpecFilterConfiguration attrConf = HDF5ReaderNodeModel.createAttrFilterPanelConfiguration();
        m_attrFilterPanel.saveConfiguration(attrConf);
        attrConf.saveConfiguration(settings);
        
		Hdf5File file = Hdf5File.createFile(m_fcSource.getStringValue());
		boolean allRowsEqual = true;
		long rows = 0;

		Iterator<String> iter = m_dsFilterPanel.getIncludedNamesAsSet().iterator();
		if (iter.hasNext()) {
			Hdf5DataSet<?> dataSet = file.getDataSetByPath(iter.next());
			rows = dataSet.getDimensions()[0];
		}
		while (allRowsEqual && iter.hasNext()) {
			Hdf5DataSet<?> dataSet = file.getDataSetByPath(iter.next());
			allRowsEqual = rows == dataSet.getDimensions()[0];
		}
		settings.addBoolean("allRowSizesEqual", allRowsEqual);
	}
}

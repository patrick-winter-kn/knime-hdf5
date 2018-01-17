package org.knime.hdf5.nodes.reader;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JFileChooser;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButton;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

    private SettingsModelString m_fcSource;

    private FlowVariableModel m_fcSourceFvm;
    
	private String m_filePath;

	private final DataColumnSpecFilterPanel m_dataSetPanel;

    private DataColumnSpecFilterConfiguration m_dsConf;
    
	private final DataColumnSpecFilterPanel m_attributePanel;

    private DataColumnSpecFilterConfiguration m_attrConf;
    
    private DialogComponentBoolean m_missingValues;
    
    private SettingsModelBoolean m_mvSource;

    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5ReaderNodeDialog() {
    	createFileChooser();
        renameTab("Options", "File Chooser");
        
        m_dataSetPanel = new DataColumnSpecFilterPanel();
        addTab("DataSet Selector", m_dataSetPanel);
        
        m_attributePanel = new DataColumnSpecFilterPanel();
        addTab("Attribute Selector", m_attributePanel);

    	m_mvSource = new SettingsModelBoolean("source", true);
    	m_missingValues = new DialogComponentBoolean(m_mvSource, "allow missing values");
    	addDialogComponent(m_missingValues);
        
    }
    
    private void createFileChooser() {
    	m_fcSource = SettingsFactory.createSourceSettings();
        m_fcSourceFvm = super.createFlowVariableModel(m_fcSource);
    	DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_fcSource,
    			"sourceHistory", JFileChooser.OPEN_DIALOG, false, m_fcSourceFvm);
    	fileChooser.setBorderTitle("Input file:");
    	addDialogComponent(fileChooser);

    	// "Browse File"-Button
        DialogComponentButton fileButton = new DialogComponentButton("Browse File");
        fileButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
		        SettingsModelString model = (SettingsModelString) fileChooser.getModel();
		        String newValue = model.getStringValue();
		        
		        if (!newValue.equals("")) {
		        	Hdf5File file = null;
		        	
		        	try {
			        	file = Hdf5File.createFile(newValue);
			        	m_filePath = file.getFilePath();
			        	updateConfigs(file);
			        	selectTab("DataSet Selector");
			        	// TODO more concrete Exception name
		        	} catch (Exception ex) {
		        		ex.getStackTrace();
		        	} finally {
		        		file.close();
		        	}
		        }
			}
        });
        addDialogComponent(fileButton);
    }
    
    private void updateConfigs(Hdf5File file) throws Exception {
    	updateDataSetConfig(file.createSpecOfDataSets());
		updateAttributeConfig(file.createSpecOfAttributes());
    }
    
    private void updateDataSetConfig(final DataTableSpec dsSpec) {
        if (m_dsConf == null) {
            m_dsConf = createDCSFilterConfiguration();
        }
        
        m_dsConf.loadDefaults(dsSpec, false);
        m_dataSetPanel.loadConfiguration(m_dsConf, dsSpec);
    }
    
    private void updateAttributeConfig(final DataTableSpec attrSpec) {
        if (m_attrConf == null) {
        	m_attrConf = createDCSFilterConfiguration();
        }
        
        m_attrConf.loadDefaults(attrSpec, true);
        m_attributePanel.loadConfiguration(m_attrConf, attrSpec);
    }
	
    /** A new configuration to store the settings. Also enables the type filter.
     * @return ...
     */
    static final DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration("column-filter");
    }
    
    /**
     * Calls the update method of the underlying filter panel.
     * @param settings the node settings to read from
     * @param specs the input specs
     * @throws NotConfigurableException if no columns are available for
     *             filtering
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
        System.out.println("LOAD SETTINGS");
    	final DataTableSpec spec = specs[0];
        if (spec == null || spec.getNumColumns() == 0) {
            throw new NotConfigurableException("No columns available for selection.");
        }

        updateDataSetConfig(spec);
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	settings.addString("filePath", m_filePath);
    	
    	String[] dsPaths = m_dataSetPanel.getIncludedNamesAsSet().toArray(new String[] {});
		settings.addStringArray("dataSets", dsPaths);
		
		SettingsModelBoolean model = (SettingsModelBoolean) m_missingValues.getModel();
        boolean missingValues = model.getBooleanValue();
        
        if (!missingValues) {
			Hdf5File file = Hdf5File.createFile(m_filePath);
			boolean allRowsEqual = true;
			long rows = 0;
			int i = 0;
			while (allRowsEqual && i < dsPaths.length) {
				Hdf5DataSet<?> dataSet = file.getDataSetByPath(dsPaths[i]);
				if (i == 0) {
					rows = dataSet.getDimensions()[0];
				} else {
					allRowsEqual = rows == dataSet.getDimensions()[0];
				}
				i++;
			}
			settings.addBoolean("allRowsEqual", allRowsEqual);
		}
		
    	String[] attrPaths = m_attributePanel.getIncludedNamesAsSet().toArray(new String[] {});
		settings.addStringArray("attributes", attrPaths);
	}
}

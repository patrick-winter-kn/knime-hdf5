package org.knime.hdf5.nodes.reader;

import javax.swing.BorderFactory;
import javax.swing.JFileChooser;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataColumnSpec;
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
import org.knime.core.node.util.filter.NameFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

    private SettingsModelString m_fcSource;

    private FlowVariableModel m_fcSourceFvm;
	
	private Hdf5File m_file;

	private final DataColumnSpecFilterPanel m_dataSetPanel;

    private DataColumnSpecFilterConfiguration m_dsConf;

    private DataTableSpec m_dsSpec;
    
	private final DataColumnSpecFilterPanel m_attributePanel;

    private DataColumnSpecFilterConfiguration m_attrConf;
    
    private DataTableSpec m_attrSpec;
    
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

    	m_mvSource = new SettingsModelBoolean("source", false);
    	m_missingValues = new DialogComponentBoolean(m_mvSource, "allow missing values");
    	m_missingValues.getComponentPanel().setBorder(
    			BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Advanced settings:"));
    	addDialogComponent(m_missingValues);
    }
    
    private void createFileChooser() {
    	m_fcSource = SettingsFactory.createSourceSettings();
        m_fcSourceFvm = super.createFlowVariableModel(m_fcSource);
    	DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_fcSource,
    			"sourceHistory", JFileChooser.OPEN_DIALOG, false, m_fcSourceFvm);
    	fileChooser.setBorderTitle("Input file:");
    	addDialogComponent(fileChooser);
    	
        fileChooser.getModel().addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
		        SettingsModelString model = (SettingsModelString) fileChooser.getModel();
		        String newValue = model.getStringValue();
		        
		        if (!newValue.equals("")) {
		        	m_file = null;
		        	
		        	try {
			        	m_file = Hdf5File.createFile(newValue);
			        	updateConfigs();
			        	// TODO more concrete Exception name
		        	} catch (Exception ex) {
		        		ex.getStackTrace();
		        	} finally {
		        		m_file.close();
		        	}
		        }
			}
        });
    }
    
    private void updateConfigs() throws Exception {
    	updateDataSetConfig();
		updateAttributeConfig();
    	selectTab("DataSet Selector");
    }
    
    private void updateDataSetConfig() {
    	m_dsSpec = m_file.createSpecOfDataSets();
    	
        if (m_dsConf == null) {
            m_dsConf = createDCSFilterConfiguration();
        }

        m_dsConf.loadDefaults(m_dsSpec, false);
        m_dataSetPanel.loadConfiguration(m_dsConf, m_dsSpec);
    }
    
    private void updateAttributeConfig() {
    	m_attrSpec = m_file.createSpecOfAttributes();
    	
        if (m_attrConf == null) {
        	m_attrConf = createDCSFilterConfiguration();
        }
        
        m_attrConf.loadDefaults(m_attrSpec, true);
        m_attributePanel.loadConfiguration(m_attrConf, m_attrSpec);
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
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
    	if (settings.containsKey("filePath") && settings.containsKey("allowMissingValues")) {
        	try {
				m_file = Hdf5File.createFile(settings.getString("filePath"));
				updateConfigs();
				m_mvSource.setBooleanValue(settings.getBoolean("allowMissingValues"));
				
	        	if (settings.containsKey("dataSetsIncluded") && settings.containsKey("dataSetsExcluded")) {
    				DataTableSpec dsInclSpec = DataTableSpec.load(settings.getConfig("dataSetsIncluded"));
    				DataTableSpec dsExclSpec = DataTableSpec.load(settings.getConfig("dataSetsExcluded"));
    		        m_dsConf.loadDefaults(dsInclSpec.getColumnNames(), dsExclSpec.getColumnNames(),
    		        		NameFilterConfiguration.EnforceOption.EnforceInclusion);
    		        m_dataSetPanel.loadConfiguration(m_dsConf, m_dsSpec);
	            }
	        	
	        	if (settings.containsKey("attributesIncluded") && settings.containsKey("attributesExcluded")) {
	                DataTableSpec attrInclSpec = DataTableSpec.load(settings.getConfig("attributesIncluded"));
    				DataTableSpec attrExclSpec = DataTableSpec.load(settings.getConfig("attributesExcluded"));
    		        m_attrConf.loadDefaults(attrInclSpec.getColumnNames(), attrExclSpec.getColumnNames(),
    		        		NameFilterConfiguration.EnforceOption.EnforceInclusion);
    		        m_attributePanel.loadConfiguration(m_attrConf, m_attrSpec);
	            }
			} catch (InvalidSettingsException ise) {
				ise.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	settings.addString("filePath", m_file.getFilePath());

    	DataTableSpec dsInclSpec = new DataTableSpec(m_dataSetPanel.getIncludeList().toArray(new DataColumnSpec[] {}));
		dsInclSpec.save(settings.addConfig("dataSetsIncluded"));
		(new DataTableSpec(m_dataSetPanel.getExcludeList().toArray(new DataColumnSpec[] {}))).save(settings.addConfig("dataSetsExcluded"));
		
		SettingsModelBoolean model = (SettingsModelBoolean) m_missingValues.getModel();
        boolean missingValues = model.getBooleanValue();
        settings.addBoolean("allowMissingValues", missingValues);
        
        String[] dsPaths = dsInclSpec.getColumnNames();
        if (!missingValues) {
			boolean allRowsEqual = true;
			long rows = 0;
			int i = 0;
			while (allRowsEqual && i < dsPaths.length) {
				Hdf5DataSet<?> dataSet = m_file.getDataSetByPath(dsPaths[i]);
				if (i == 0) {
					rows = dataSet.getDimensions()[0];
				} else {
					allRowsEqual = rows == dataSet.getDimensions()[0];
				}
				i++;
			}
			settings.addBoolean("allRowSizesEqual", allRowsEqual);
		}
		
		(new DataTableSpec(m_attributePanel.getIncludeList().toArray(new DataColumnSpec[] {}))).save(settings.addConfig("attributesIncluded"));
		(new DataTableSpec(m_attributePanel.getExcludeList().toArray(new DataColumnSpec[] {}))).save(settings.addConfig("attributesExcluded"));
	}
}

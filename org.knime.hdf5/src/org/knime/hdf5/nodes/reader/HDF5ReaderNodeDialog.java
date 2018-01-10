package org.knime.hdf5.nodes.reader;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JFileChooser;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentButton;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

// TODO it should close all Files before ending KNIME

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

    private SettingsModelString m_source;

    private FlowVariableModel m_sourceFvm;	
    
	private String m_filePath;

	private final DataColumnSpecFilterPanel m_dataSetPanel;

    private DataColumnSpecFilterConfiguration m_dsConf;
    
	private final DataColumnSpecFilterPanel m_attributePanel;

    private DataColumnSpecFilterConfiguration m_attrConf;
    
	

    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5ReaderNodeDialog() {
        m_source = SettingsFactory.createSourceSettings();
        m_sourceFvm = super.createFlowVariableModel(m_source);
        renameTab("Options", "File Chooser");
        
    	DialogComponentFileChooser source = new DialogComponentFileChooser(m_source,
    			"sourceHistory", JFileChooser.OPEN_DIALOG, false, m_sourceFvm);
    	source.setBorderTitle("Input file:");
    	addDialogComponent(source);

    	// "Browse File"-Button
        DialogComponentButton fileButton = new DialogComponentButton("Browse File");
        fileButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
		        final SettingsModelString model = (SettingsModelString) source.getModel();
		        final String newValue = model.getStringValue();
		        
		        if (!newValue.equals("")) {
		        	Hdf5File file = null;
		        	
		        	try {
		        		// TODO there's a NullPointerException if the user quits before the file has been initialized
			        	file = Hdf5File.createFile(newValue);
			        	m_filePath = file.getFilePath();
			        	updateLists(file);
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
        
        m_dataSetPanel = new DataColumnSpecFilterPanel();
        super.addTab("DataSet Selector", m_dataSetPanel);
        
        m_attributePanel = new DataColumnSpecFilterPanel();
        super.addTab("Attribute Selector", m_attributePanel);
        
    }
    
    // TODO make discovery of dataSets and attributes more efficient
    private void updateLists(Hdf5File file) throws Exception {
    	List<String> dsPaths = file.getAllDataSetPaths();
    	List<Hdf5DataSet<?>> dsList = new LinkedList<>();
    	
    	for (String path: dsPaths) {
    		Hdf5DataSet<?> dataSet = file.getDataSetByPath(path);
    		if (dataSet != null) {
    			dsList.add(dataSet);
    		}
		}

    	List<DataColumnSpec> dsColSpecList = new LinkedList<>();
    	List<DataColumnSpec> attrColSpecList = new LinkedList<>();
    	
		Iterator<Hdf5DataSet<?>> iterDS = dsList.iterator();
		while (iterDS.hasNext()) {
			Hdf5DataSet<?> dataSet = iterDS.next();
			Hdf5KnimeDataType dataType = dataSet.getType().getKnimeType();
			DataType type = dataType.getColumnType();
			String pathWithName = dataSet.getPathFromFile() + dataSet.getName();
			
			dsColSpecList.add(new DataColumnSpecCreator(pathWithName, type).createSpec());	
		}
		
		Map<String, Hdf5DataType> attrInfo = file.getAllAttributesInfo();
		Iterator<String> iterAttr = attrInfo.keySet().iterator();
		while (iterAttr.hasNext()) {
			String attrPath = iterAttr.next();
			Hdf5DataType dataType = attrInfo.get(attrPath);
			if (dataType != null) {
				DataType attrType = dataType.getKnimeType().getColumnType();
				
				attrColSpecList.add(new DataColumnSpecCreator(attrPath, attrType).createSpec());
			}
		}
		
		DataColumnSpec[] dsColSpecs = dsColSpecList.toArray(new DataColumnSpec[] {});
		updateDataSetConfig(new DataTableSpec(dsColSpecs));
		
		DataColumnSpec[] attrColSpecs = attrColSpecList.toArray(new DataColumnSpec[] {});
		updateAttributeConfig(new DataTableSpec(attrColSpecs));
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
		
    	String[] attrPaths = m_attributePanel.getIncludedNamesAsSet().toArray(new String[] {});
		settings.addStringArray("attributes", attrPaths);
	}
}

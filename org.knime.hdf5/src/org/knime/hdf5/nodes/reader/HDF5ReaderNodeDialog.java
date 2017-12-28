package org.knime.hdf5.nodes.reader;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

// TODO it should close all Files before ending KNIME

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {
	
	private final DataColumnSpecFilterPanel m_filterPanel;

    private DataColumnSpecFilterConfiguration m_conf;
    
    private SettingsModelString m_source;

    private FlowVariableModel m_sourceFvm;	
    
	private Hdf5File file;
	

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
    	
        m_filterPanel = new DataColumnSpecFilterPanel();
        super.addTab("DataSet Selector", m_filterPanel);
        
    	// "Browse File"-Button
        DialogComponentButton fileButton = new DialogComponentButton("Browse File");
        fileButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				// update the instance of the File which has been used
		        final SettingsModelString model = (SettingsModelString) source.getModel();
		        final String newValue = model.getStringValue();
		        
		        if (!newValue.equals("")) {
		        	file = Hdf5File.createFile(newValue);
		        	updateLists(file);
		        	selectTab("DataSet Selector");
		        }
			}
        });
        addDialogComponent(fileButton);
    }
    
    private void updateLists(Hdf5File file) {
    	List<String> dsPaths = file.getAllDataSetPaths();
    	List<Hdf5DataSet<?>> dsList = new LinkedList<>();
    	
    	for (String path: dsPaths) {
			dsList.add(file.getDataSetByPath(path));
		}
    	
    	List<DataColumnSpec> colSpecList = new LinkedList<>();
		Iterator<Hdf5DataSet<?>> iterDS = dsList.iterator();
		
		while (iterDS.hasNext()) {
			Hdf5DataSet<?> dataSet = iterDS.next();
			Hdf5KnimeDataType dataType = dataSet.getType().getKnimeType();
			DataType type = dataType.getColumnType();
			String pathWithName = dataSet.getPathFromFile() + dataSet.getName();
			
			colSpecList.add(new DataColumnSpecCreator(pathWithName, type).createSpec());
		}
		
		DataColumnSpec[] colSpecs = colSpecList.toArray(new DataColumnSpec[] {});
		updateConfigurations(new DataTableSpec(colSpecs));
    }
    
    void updateConfigurations(final DataTableSpec spec) {
        if (m_conf == null) {
            m_conf = createDCSFilterConfiguration();
        }
        // auto-configure
        m_conf.loadDefaults(spec, false);
        m_filterPanel.loadConfiguration(m_conf, spec);
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
            throw new NotConfigurableException("No columns available for "
                    + "selection.");
        }

        updateConfigurations(spec);
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	settings.addString("filePath", file.getFilePath());
    	String[] dsPaths = m_filterPanel.getIncludedNamesAsSet().toArray(new String[] {});
		settings.addStringArray("dataSets", dsPaths);
	}
}

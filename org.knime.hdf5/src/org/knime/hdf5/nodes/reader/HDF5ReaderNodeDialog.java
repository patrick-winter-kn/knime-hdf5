package org.knime.hdf5.nodes.reader;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
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
import org.knime.hdf5.lib.Hdf5DataType;
import org.knime.hdf5.lib.Hdf5File;

// TODO it should close all Files before ending KNIME

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {
	
	private final DataColumnSpecFilterPanel m_filterPanel;
	
    private SettingsModelString m_source;

    private FlowVariableModel m_sourceFvm;
	
	private List<String> excl = new LinkedList<>();
	
	private List<String> incl = new LinkedList<>();
    
	private String[] names = excl.toArray(new String[] {});
    
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
        super.addTab("Column Selector", m_filterPanel);
        
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
		        	createExcludeList(file);
		        	selectTab("Column Selector");
		        }
			}
        });
        addDialogComponent(fileButton);
    }
    
    private void createExcludeList(Hdf5File file) {
		excl.clear();
    	excl.addAll(file.getAllDataSetPaths());
    	
    	updatePanel();
    }
    
    private void updatePanel() {
    	names = new String[excl.size() + incl.size()];
    	List<String> exclIncl = new LinkedList<>();
    	exclIncl.addAll(excl);
    	exclIncl.addAll(incl);
    	names = exclIncl.toArray(names);
    	
        m_filterPanel.update(incl, excl, names);
        
        
        
        List<DataColumnSpec> colSpecList = new LinkedList<>();
		List<Hdf5DataSet<?>> dsList = new LinkedList<>();
		
		Iterator<String> dsPaths = file.getAllDataSetPaths().iterator();
		while (dsPaths.hasNext()) {
			dsList.add(file.getDataSetByPath(dsPaths.next()));
		}
		
		Iterator<Hdf5DataSet<?>> iterDS = dsList.iterator();
		while (iterDS.hasNext()) {
			Hdf5DataSet<?> dataSet = iterDS.next();
			Hdf5DataType dataType = dataSet.getType();
			DataType type = dataType.getColumnType();
			String path = dataSet.getPathWithoutFileName();
			
			colSpecList.add(new DataColumnSpecCreator(path, type).createSpec());

			System.out.println("-> ");
		}
		
		DataColumnSpec[] colSpecs = colSpecList.toArray(new DataColumnSpec[] {});
		DataTableSpec spec = new DataTableSpec(colSpecs);

        DataColumnSpecFilterConfiguration config = HDF5ReaderNodeModel.createDCSFilterConfiguration();
		System.out.println(m_filterPanel.getExcludeList().addAll(colSpecList));
        //config.loadConfigurationInDialog(settings, spec);
		//m_filterPanel.loadConfiguration(config, spec);

		System.out.println("-> " + Arrays.toString(m_filterPanel.getExcludedNamesAsSet().toArray(new String[] {})));
		System.out.println("-> " + Arrays.toString(m_filterPanel.getExcludeList().toArray()));
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

        DataColumnSpecFilterConfiguration config = HDF5ReaderNodeModel.createDCSFilterConfiguration();
        config.loadConfigurationInDialog(settings, specs[0]);
        m_filterPanel.loadConfiguration(config, specs[0]);
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	settings.addString("filePath", file.getFilePath());
    	String[] dsPaths = m_filterPanel.getIncludedNamesAsSet().toArray(new String[] {});
		settings.addStringArray("dataSets", dsPaths);
		/*
		List<DataColumnSpec> colSpecList = new LinkedList<>();
		List<Hdf5DataSet<?>> dsList = new LinkedList<>();
		
		for (String path: dsPaths) {
			dsList.add(file.getDataSetByPath(path));
		}
		
		Iterator<Hdf5DataSet<?>> iterDS = dsList.iterator();
		while (iterDS.hasNext()) {
			Hdf5DataSet<?> dataSet = iterDS.next();
			Hdf5DataType dataType = dataSet.getType();
			DataType type = dataType.getColumnType();
			String path = dataSet.getPathWithoutFileName();
			
			colSpecList.add(new DataColumnSpecCreator(path, type).createSpec());
		}
		
		DataColumnSpec[] colSpecs = colSpecList.toArray(new DataColumnSpec[] {});
		DataTableSpec spec = new DataTableSpec(colSpecs);
		
		DataColumnSpecFilterConfiguration config = HDF5ReaderNodeModel.createDCSFilterConfiguration();
        config.loadConfigurationInDialog((NodeSettingsRO) settings, spec);
        m_filterPanel.loadConfiguration(config, spec);
		m_filterPanel.saveConfiguration(config);*/
	}
}

package org.knime.hdf5.nodes.reader;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JViewport;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentButton;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.hdf5.lib.Hdf5DataSet;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;

// TODO it should close all Files before ending KNIME

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

	static Map<Hdf5DataSet<?>, List<String>> dsMap = new LinkedHashMap<>();
	
	private final DataColumnSpecFilterPanel m_filterPanel;
	
    private SettingsModelString m_source;

    private FlowVariableModel m_sourceFvm;
    
	private boolean fileLoaded;
	
	private List<String> excl = new LinkedList<>();
	
	private List<String> incl = new LinkedList<>();
    
	private String[] names = excl.toArray(new String[] {});
    
	private Hdf5File file;

	private Hdf5Group group;
	

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
        super.addTab("Column Filter", m_filterPanel);
        JScrollPane jsp = (JScrollPane) m_filterPanel.getComponent(0);
        JViewport vp = (JViewport) jsp.getComponent(0);
        JPanel panel = (JPanel) vp.getComponent(0);
        JPanel filterPanel = (JPanel) panel.getComponent(1);
        JPanel center = (JPanel) filterPanel.getComponent(0);
        System.out.println("center: " + Arrays.toString(center.getComponents()));
        
        JPanel excludePanel = (JPanel) center.getComponent(0);
        JScrollPane exclJSP = (JScrollPane) excludePanel.getComponent(1);
        JViewport exclVP = (JViewport) exclJSP.getComponent(0);
        @SuppressWarnings("rawtypes")
		JList exclList = (JList) exclVP.getComponent(0);
        
        JPanel buttonPan2 = (JPanel) center.getComponent(1);
        JPanel buttonPan = (JPanel) buttonPan2.getComponent(0);
        
        JPanel includePanel = (JPanel) center.getComponent(0);
        System.out.println("includePanel: " + Arrays.toString(includePanel.getComponents()));
        JScrollPane inclJSP = (JScrollPane) includePanel.getComponent(1);
        System.out.println("jsp2: " + Arrays.toString(inclJSP.getComponents()));
        JViewport inclVP = (JViewport) inclJSP.getComponent(0);
        System.out.println("vp2: " + Arrays.toString(inclVP.getComponents()));
        @SuppressWarnings("rawtypes")
		JList inclList = (JList) inclVP.getComponent(0);
        System.out.println("inclList: " + Arrays.toString(inclList.getComponents()));
        
        
        JButton m_openButton = new JButton("open");
        m_openButton.setMaximumSize(new Dimension(125, 25));
        buttonPan.add(m_openButton);
        m_openButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                @SuppressWarnings("unchecked")
				List<Object> selected = exclList.getSelectedValuesList();
                if (selected.size() == 1) {
                	Object dcs = selected.get(0);
                    String oname = dcs.toString();
                    String name = oname.substring(0, oname.lastIndexOf("(") - 1);
                    
                    if (group.existsDataSet(name) && !existsInDsList(name)) {
                		Hdf5DataSet<?> dataSet = group.getDataSet(name);
                		updateIncludeList(dataSet);
                		dataSet.close();
                		
                	} else if (group.existsGroup(name)) {
                		group = group.getGroup(name);
                		updateExcludeList(group);
                	}
                }
            }
        });
        buttonPan.add(Box.createVerticalStrut(25));
        m_openButton.setEnabled(true);

        JButton m_closeButton = new JButton("close");
        m_closeButton.setMaximumSize(new Dimension(125, 25));
        buttonPan.add(m_closeButton);
        m_closeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent ae) {
            	if (!(group instanceof Hdf5File)) {
        			group.close();
	        		group = group.getParent();
	            	updateExcludeList(group);
            	}
            }
        });
        buttonPan.add(Box.createVerticalStrut(25));
        m_closeButton.setEnabled(true);
        
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
		        	group = file;
		        	updateExcludeList(group);

		            System.out.println("Incl Size: " + m_filterPanel.getIncludedNamesAsSet().size());
		            System.out.println("Excl Size: " + m_filterPanel.getExcludedNamesAsSet().size());
		        }
			}
        });
        addDialogComponent(fileButton);
    }
    
    static private boolean existsInDsList(String name) {
    	Iterator<Hdf5DataSet<?>> iter = dsMap.keySet().iterator();
    	while (iter.hasNext()) {
    		if (iter.next().getName().equals(name)) {
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    private void updateExcludeList(Hdf5Group group) {
		// subGroups in Group
    	List<String> groupNames = group.loadGroupNames();
		
        // dataSets in Group
    	List<String> dataSetNames = group.loadDataSetNames();

    	excl.clear();
    	excl.addAll(groupNames);
    	excl.addAll(dataSetNames);
    	
    	updatePanel();
    }
    
    private void updateIncludeList(Hdf5DataSet<?> dataSet) {
		long cols = dataSet.numberOfValuesRange(1, dataSet.getDimensions().length);
		
		String[] names = new String[(int) cols];
		for (int i = 0; i < cols; i++) {
			names[i] = dataSet.getName() + i;
		}
		List<String> nameList = new LinkedList<>();
		nameList.addAll(Arrays.asList(names));
		incl.addAll(nameList);
		dsMap.put(dataSet, nameList);
		
		updatePanel();
    }
    
    private void updatePanel() {
    	names = new String[excl.size() + incl.size()];
    	List<String> exclIncl = new LinkedList<>();
    	exclIncl.addAll(excl);
    	exclIncl.addAll(incl);
    	names = exclIncl.toArray(names);
    	
        m_filterPanel.update(incl, excl, names);
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
    	if (fileLoaded) {
	    	final DataTableSpec spec = specs[0];
	        if (spec == null || spec.getNumColumns() == 0) {
	            throw new NotConfigurableException("No columns available for "
	                    + "selection.");
	        }
	
	        DataColumnSpecFilterConfiguration config = HDF5ReaderNodeModel.createDCSFilterConfiguration();
	        config.loadConfigurationInDialog(settings, specs[0]);
	        m_filterPanel.loadConfiguration(config, specs[0]);
        }
    }
}

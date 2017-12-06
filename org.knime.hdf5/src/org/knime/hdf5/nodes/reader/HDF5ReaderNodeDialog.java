package org.knime.hdf5.nodes.reader;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
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

	// TODO try to avoid static here
	static List<Hdf5DataSet<?>> dsList = new LinkedList<>();
	
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
        super.addTab("Column Selector", m_filterPanel);
        
        @SuppressWarnings("rawtypes")
		JList exclList = (JList) getComponentOfCenter("exclList");
        JPanel buttonPan = (JPanel) getComponentOfCenter("buttonPan");
        @SuppressWarnings("rawtypes")
		JList inclList = (JList) getComponentOfCenter("inclList");
        
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
		        	selectTab("Column Selector");
		        }
			}
        });
        addDialogComponent(fileButton);
        
        
        JButton openButton = new JButton("open");
        openButton.setMaximumSize(new Dimension(125, 25));
        buttonPan.add(openButton);
        openButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                openElement(exclList);
            }
        });
        buttonPan.add(Box.createVerticalStrut(25));
        openButton.setEnabled(true);

        exclList.removeMouseListener(exclList.getMouseListeners()[1]);
        exclList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent me) {
                if (me.getClickCount() == 2) {
                	openElement(exclList);
                }
            }
        });
        
        JButton closeButton = new JButton("close");
        closeButton.setMaximumSize(new Dimension(125, 25));
        buttonPan.add(closeButton);
        closeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent ae) {
            	closeElement();
            }
        });
        buttonPan.add(Box.createVerticalStrut(25));
        closeButton.setEnabled(true);
        
        JButton removeButton = new JButton("remove");
        removeButton.setMaximumSize(new Dimension(125, 25));
        buttonPan.add(removeButton);
        removeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent ae) {
            	removeElement(inclList);
            }
        });
        buttonPan.add(Box.createVerticalStrut(25));
        removeButton.setEnabled(true);
        
        inclList.removeMouseListener(inclList.getMouseListeners()[1]);
        inclList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent me) {
                if (me.getClickCount() == 2) {
                	removeElement(inclList);
                }
            }
        });
        
    }
    
    private static boolean existsInDsList(Hdf5DataSet<?> dataSet) {
    	Iterator<Hdf5DataSet<?>> iter = dsList.iterator();
    	while (iter.hasNext()) {
    		if (iter.next().equals(dataSet)) {
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    private static boolean existsNameInDsList(String name) {
    	Iterator<Hdf5DataSet<?>> iter = dsList.iterator();
    	while (iter.hasNext()) {
    		if (iter.next().getName().equals(name)) {
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    private static String remLastParBlock(String in) {
    	int end = in.lastIndexOf(")");
    	int start = end;
    	int par = end >= 0 ? 1 : 0;
    	char[] toks = in.toCharArray();
    	while (par > 0) {
    		start--;
    		par += toks[start] == ')' ? 1 : (toks[start] == '(' ? -1 : 0);
    	}
    	return in.substring(0, start) + in.substring(Math.min(end + 1, in.length()));
    }
    
    private static String remLastNumBlock(String in) {
    	int start = in.length();
    	char[] toks = in.toCharArray();
    	while (start > 0 && Character.isDigit(toks[start-1])) {
    		start--;
    	}
    	return in.substring(0, start);
    }
    
    private Component getComponentOfCenter(String comp) {
    	JScrollPane jsp = (JScrollPane) m_filterPanel.getComponent(0);
        JViewport vp = (JViewport) jsp.getComponent(0);
        JPanel panel = (JPanel) vp.getComponent(0);
        JPanel filterPanel = (JPanel) panel.getComponent(1);
        JPanel center = (JPanel) filterPanel.getComponent(0);
        
        if (comp.equals("exclList")) {
		    JPanel excludePanel = (JPanel) center.getComponent(0);
		    JScrollPane exclJSP = (JScrollPane) excludePanel.getComponent(1);
		    JViewport exclVP = (JViewport) exclJSP.getComponent(0);
		    @SuppressWarnings("rawtypes")
			JList exclList = (JList) exclVP.getComponent(0);
		    return exclList;
		    
        } else if (comp.equals("buttonPan")) {
            JPanel buttonPan2 = (JPanel) center.getComponent(1);
            JPanel buttonPan = (JPanel) buttonPan2.getComponent(0);
            return buttonPan;
        	
        } else if (comp.equals("inclList")) {
	        JPanel includePanel = (JPanel) center.getComponent(2);
	        JScrollPane inclJSP = (JScrollPane) includePanel.getComponent(1);
	        JViewport inclVP = (JViewport) inclJSP.getComponent(0);
	        @SuppressWarnings("rawtypes")
			JList inclList = (JList) inclVP.getComponent(0);
	        return inclList;
        }
        
        return null;
    }
    
    @SuppressWarnings("rawtypes")
	private void openElement(JList exclList) {
    	@SuppressWarnings("unchecked")
		List<Object> selected = exclList.getSelectedValuesList();
        if (selected.size() == 1) {
        	Object dcs = selected.get(0);
            String oname = dcs.toString();
            String name = oname.substring(0, oname.lastIndexOf("(") - 1);
            
            group.open();
            if (group.existsDataSet(name)) {
        		Hdf5DataSet<?> dataSet = group.getDataSet(name);
        		updateIncludeList(dataSet);
        		dataSet.close();
        		
        	} else if (group.existsGroup(name)) {
        		group = group.getGroup(name);
        		updateExcludeList(group);
        	}
        }
    }
    
	private void closeElement() {
		if (!(group instanceof Hdf5File)) {
			group.close();
    		group = group.getParent();
    		group.open();
        	updateExcludeList(group);
    	}
	}
	
	@SuppressWarnings("rawtypes")
	private void removeElement(JList inclList) {
		List<String> names = new LinkedList<>();
        
		@SuppressWarnings("unchecked")
		Iterator<Object> selected = inclList.getSelectedValuesList().iterator();
        while (selected.hasNext()) {
            String oname = remLastParBlock(selected.next().toString());
            names.add(oname.substring(0, oname.length() - 1));
        }
        
        removeFromIncludeList(names);
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
    	boolean inDsList = existsInDsList(dataSet);
    	
    	if (inDsList || !existsNameInDsList(dataSet.getName())) {
	    	if (inDsList) {
	    		incl.removeAll(dataSet.getActiveCols());
	    	} else {
	    		dsList.add(dataSet);
	    	}
	    	
	    	dataSet.activateAllCols();
			incl.addAll(dataSet.getActiveCols());
			
			updatePanel();
		}
    }
    
    private void removeFromIncludeList(List<String> names) {
    	Map<String, List<String>> remCols = new LinkedHashMap<>();
    	
    	Iterator<String> iter = names.iterator();
    	while (iter.hasNext()) {
    		String colName = iter.next();
    		String dsName = remLastNumBlock(colName);
    		if (!remCols.containsKey(dsName)) {
    			remCols.put(dsName, Arrays.asList(colName));
    		} else {
    			remCols.get(dsName).add(colName);
    		}
    	}
    	
    	Iterator<Hdf5DataSet<?>> iterDS = dsList.iterator();
    	while (iterDS.hasNext()) {
    		Hdf5DataSet<?> dataSet = iterDS.next();
    		
    		if (remCols.containsKey(dataSet.getName())) {
    			dataSet.getActiveCols().removeAll(remCols.get(dataSet.getName()));
    			
    			if (dataSet.getActiveCols().isEmpty()) {
    				dsList.remove(dataSet);
    			}
    		}
    	}
    	
		incl.removeAll(names);
		
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

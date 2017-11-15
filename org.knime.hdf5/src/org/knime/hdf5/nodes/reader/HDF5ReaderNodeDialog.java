package org.knime.hdf5.nodes.reader;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.NominalValue;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.util.filter.NameFilterConfiguration.EnforceOption;
import org.knime.core.node.util.filter.nominal.NominalValueFilterConfiguration;
import org.knime.core.node.util.filter.nominal.NominalValueFilterPanel;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane implements
	ItemListener{

	private String m_selectedColumn;

    private final Map<String, Set<DataCell>> m_colAttributes;

    // models
    private final DefaultComboBoxModel<String> m_columns;

    // gui elements
    private final JComboBox<String> m_columnSelection;

    /** Config key for the selected column. */
    static final String CFG_SELECTED_COL = "selected_column";

    /** Config key for the possible values to be included. */
    static final String CFG_SELECTED_ATTR = "selected attributes";

    /** Config key for filter configuration. */
    static final String CFG_CONFIGROOTNAME = "filter config";

    private NominalValueFilterPanel m_filterPanel;

    
    protected HDF5ReaderNodeDialog(boolean splitter) {
        m_colAttributes = new LinkedHashMap<String, Set<DataCell>>();
        m_columns = new DefaultComboBoxModel<>();

        m_columnSelection = new JComboBox<>(m_columns);
        m_columnSelection.setMaximumSize(new Dimension(Integer.MAX_VALUE, 20));
        // add listener to column selection box to change exclude list
        m_columnSelection.addItemListener(this);

        // create the GUI
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Select column:"));
        JPanel columnSelectionPanel = new JPanel(new BorderLayout());
        columnSelectionPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
        columnSelectionPanel.add(m_columnSelection, BorderLayout.CENTER);
        panel.add(columnSelectionPanel, BorderLayout.NORTH);
        Box attributeSelectionBox = Box.createHorizontalBox();
        m_filterPanel = new NominalValueFilterPanel();
        if (splitter) {
            m_filterPanel.setIncludeTitle("Top");
            m_filterPanel.setExcludeTitle("Bottom");
            m_filterPanel.setAdditionalCheckboxText("<html>Incl. Missing<br/>Values (Top)</html>");
            m_filterPanel.setPatternFilterBorderTitles("Mismatch (Bottom)", "Match (Top)");
            m_filterPanel.setAdditionalPatternCheckboxText("Include Missing Values (Top)");
        }
        attributeSelectionBox.add(m_filterPanel);
        panel.add(attributeSelectionBox, BorderLayout.CENTER);
        addTab("Selection", new JScrollPane(panel));
        removeTab("Options");
        removeTab("Flow Variables");
    }

    /**
     * New pane for configuring the PossibleValueRowFilter node.
     */
    protected HDF5ReaderNodeDialog() {
        this(false);
    }

    /**
     * {@inheritDoc}
     *
     * If the nominal column selection changes, include and exclude lists are
     * cleared and all possible values of that column are put into the exclude
     * list.
     */
    @Override
    public void itemStateChanged(final ItemEvent item) {
        if (item.getStateChange() == ItemEvent.SELECTED) {
            m_selectedColumn = (String)item.getItem();
            ArrayList<String> names = new ArrayList<>();
            if (m_colAttributes.get(m_selectedColumn) != null) {
                for (DataCell dc : m_colAttributes.get(m_selectedColumn)) {
                    names.add(dc.toString());
                }
            }
            String[] namesArray = names.toArray(new String[names.size()]);
            NominalValueFilterConfiguration config = new NominalValueFilterConfiguration(CFG_CONFIGROOTNAME);
            config.loadDefaults(null, namesArray, EnforceOption.EnforceInclusion);
            m_filterPanel.loadConfiguration(config, namesArray);
        }
    }

    /**
     *
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
        if (specs.length == 0 || specs[0] == null) {
            throw new NotConfigurableException("No incoming columns found. "
                    + "Please connect the node with input table!");
        }
        // get selected column
        m_selectedColumn = settings.getString(CFG_SELECTED_COL, "");
        // clear old values
        m_colAttributes.clear();
        m_columns.removeAllElements();
        // disable item state change listener while adding values
        m_columnSelection.removeItemListener(this);
        // fill the models
        for (DataColumnSpec colSpec : specs[0]) {
            if (colSpec.getType().isCompatible(NominalValue.class) && colSpec.getDomain().hasValues()) {
                m_columns.addElement(colSpec.getName());
                // create column - possible values mapping
                m_colAttributes.put(colSpec.getName(), colSpec.getDomain().getValues());
            }
        }
        // set selection
        if (m_selectedColumn != null) {
            m_columnSelection.setSelectedItem(m_selectedColumn);
            // enable item change listener again
            m_columnSelection.addItemListener(this);
        } else {
            m_columnSelection.addItemListener(this);
            m_columnSelection.setSelectedIndex(-1);
            m_columnSelection.setSelectedItem(m_columnSelection.getItemAt(0));
        }

        NominalValueFilterConfiguration config = new NominalValueFilterConfiguration(CFG_CONFIGROOTNAME);
        Set<DataCell> domain = m_colAttributes.get(m_selectedColumn);
        if (settings.containsKey(CFG_CONFIGROOTNAME)) {
            config.loadConfigurationInDialog(settings, domain);
        } else {
            // backwards compatibility
            String[] selectedAttributes = settings.getStringArray(CFG_SELECTED_ATTR, "");
            Set<String> includedAttr = new HashSet<String>();
            for (String s : selectedAttributes) {
                includedAttr.add(s);
            }

            ArrayList<String> m_included = new ArrayList<String>();
            ArrayList<String> m_excluded = new ArrayList<String>();
            if (domain != null) {
                for (DataCell dc : domain) {
                    // if possible value was in the settings...
                    if (includedAttr.contains(dc.toString())) {
                        // ... put it to included ...
                        m_included.add(dc.toString());
                    } else {
                        // ... else to excluded
                        m_excluded.add(dc.toString());
                    }
                }
            }
            config.loadDefaults(m_included.toArray(new String[m_included.size()]),
                m_excluded.toArray(new String[m_excluded.size()]), EnforceOption.EnforceInclusion);
        }
        m_filterPanel.loadConfiguration(config, domain);
        
        
        // oldDialog
        
        /*
        private static Hdf5Group group;

    	// for selection of children of the group group
    	private static DialogComponentStringSelection children;
	    
	    private static DialogComponentStringSelection dataSets;
	    
	    private SettingsModelString m_source;
	
	    private FlowVariableModel m_sourceFvm;
	
	    private SettingsModelString m_children;
	    
	    private SettingsModelString m_dataSets;
	
	    protected HDF5ReaderNodeDialog() {
	        super();
	        
	        m_source = SettingsFactory.createSourceSettings();
	        m_sourceFvm = super.createFlowVariableModel(m_source);
	        m_children = SettingsFactory.createSourceSettings();
	        m_dataSets = SettingsFactory.createSourceSettings();
	        
	        // Source
	        DialogComponentFileChooser source =
	                new DialogComponentFileChooser(m_source, "sourceHistory", JFileChooser.OPEN_DIALOG, false, m_sourceFvm);
	        source.setBorderTitle("Input file: (it may load a few seconds after pressing \"Browse File\")");
	        addDialogComponent(source);
	        
	        // "Browse File"-Button
	        DialogComponentButton fileButton = new DialogComponentButton("Browse File");
	        fileButton.addActionListener(new ActionListener(){
				@Override
				public void actionPerformed(ActionEvent e) {
					// update the instance of the File which has been used
			        final SettingsModelString model = (SettingsModelString) source.getModel();
			        final String newValue = model.getStringValue();
			        
			        if (!newValue.equals("")) {
			        	//findLocation(newValue);
	
			        	HDF5ReaderNodeModel.fname = newValue;
			    		HDF5ReaderNodeModel.dspath = "/";
			        	group = Hdf5File.createFile(newValue);
			        	
			        	// TODO do it like in Column Filter
			        	
			        	
			            // subGroups in File
			        	List<String> groupNames = group.loadGroupNames();
			        	groupNames.add(0, " ");
			        	if (children == null) {
				            children = new DialogComponentStringSelection(m_children,
				            		"Groups: ", groupNames);
			        	} else {
				            children.replaceListItems(groupNames, null);
			        	}
			            addDialogComponent(children);
						children.getModel().addChangeListener(new ChangeListener(){
							@Override
							public void stateChanged(ChangeEvent e) {
						        final SettingsModelString model = (SettingsModelString) children.getModel();
						        final String newValue = model.getStringValue();
	
						        if (!newValue.equals(" ")) {
						    		HDF5ReaderNodeModel.dspath = HDF5ReaderNodeModel.dspath + newValue + "/";
						        	group = group.createGroup(newValue, Hdf5OverwritePolicy.ABORT);
						        	
									// subGroups in Group
						        	List<String> groupNames = group.loadGroupNames();
						        	groupNames.add(0, " ");
						            children.replaceListItems(groupNames, null);
						            
						            // dataSets in Group
						        	List<String> dataSetNames = group.loadDataSetNames();
						        	dataSetNames.add(0, " ");
						            dataSets.replaceListItems(dataSetNames, null);
					            }
							}
						});
						
			            // dataSets in File
			        	List<String> dataSetNames = group.loadDataSetNames();
			        	dataSetNames.add(0, " ");
						if (dataSets == null) {
				            dataSets = new DialogComponentStringSelection(m_dataSets,
				            		"Datasets: ", dataSetNames);
			            } else {
				            dataSets.replaceListItems(dataSetNames, null);
			            }
			            addDialogComponent(dataSets);
			            dataSets.getModel().addChangeListener(new ChangeListener(){
							@Override
							public void stateChanged(ChangeEvent e) {
						        final SettingsModelString model = (SettingsModelString) dataSets.getModel();
						        final String newValue = model.getStringValue();
						        
						        if (!newValue.equals(" ")) {
						        	HDF5ReaderNodeModel.dsname = newValue;
						        }
							}
						});
	
			        }
				}
	        });
	        addDialogComponent(fileButton);
	    }
        */
    }
}

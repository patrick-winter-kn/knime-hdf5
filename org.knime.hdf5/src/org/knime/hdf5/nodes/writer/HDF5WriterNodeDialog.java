package org.knime.hdf5.nodes.writer;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.TransferHandler;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {
	
	private SettingsModelString m_filePathSettings;
	
	private SettingsModelBoolean m_structureMustMatch;
	
	private SettingsModelBoolean m_saveColumnProperties;

	private ListPanel m_columnSpecPanel = new ListPanel();
	
	private ListPanel m_flowVariableSpecPanel = new ListPanel();
	
	private EditTreePanel m_editTreePanel = new EditTreePanel();
	
    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5WriterNodeDialog() {
    	m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "outputFilePathHistory",
				JFileChooser.SAVE_DIALOG, false, filePathFvm, ".h5|.hdf5");
		
		DialogComponentLabel fileInfoLabel = new DialogComponentLabel("");
		fileChooser.getModel().addChangeListener(new ChangeListener() {
			
			@Override
			public void stateChanged(ChangeEvent e) {
				String filePath = m_filePathSettings.getStringValue();
				if (filePath.endsWith(".h5") || filePath.endsWith(".hdf5")) {
					fileInfoLabel.setText("Info: File " + (Hdf5File.existsFile(filePath) ? "exists" : "does not exist"));
					
				} else {
					fileInfoLabel.setText("Error: File ending is not valid");
				}
			}
		});
		
        createNewGroup("Output file:");
		addDialogComponent(fileChooser);
		addDialogComponent(fileInfoLabel);
        closeCurrentGroup();
        
		
		m_structureMustMatch = SettingsFactory.createStructureMustMatchSettings();
		DialogComponentBoolean structureMustMatch = new DialogComponentBoolean(m_structureMustMatch,
				"Structure must match");
		m_saveColumnProperties = SettingsFactory.createStructureMustMatchSettings();
		DialogComponentBoolean saveColumnProperties = new DialogComponentBoolean(m_saveColumnProperties,
				"Save column properties");
		
        createNewGroup("Advanced settings:");
		addDialogComponent(structureMustMatch);
		addDialogComponent(saveColumnProperties);
        closeCurrentGroup();
		
        
		JPanel dataPanel = new JPanel();
		addTab("Data", dataPanel);
		dataPanel.setMinimumSize(new Dimension(800, 500));
		dataPanel.setLayout(new BoxLayout(dataPanel, BoxLayout.X_AXIS));

		JPanel inputPanel = new JPanel();
		dataPanel.add(inputPanel);
		inputPanel.setLayout(new BoxLayout(inputPanel, BoxLayout.Y_AXIS));
		inputPanel.setBorder(BorderFactory.createTitledBorder(" Input "));
		addListToPanel(SpecInfo.COLUMN_SPECS, inputPanel);
		addListToPanel(SpecInfo.FLOW_VARIABLE_SPECS, inputPanel);
    	
    	JPanel outputPanel = new JPanel();
		dataPanel.add(outputPanel);
		outputPanel.setLayout(new BoxLayout(outputPanel, BoxLayout.Y_AXIS));
		outputPanel.setBorder(BorderFactory.createTitledBorder(" Output "));
		JButton updateButton = new JButton("update/reset file config");
		updateButton.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				String filePath = m_filePathSettings.getStringValue();
				//if (filePathChanged(filePath)) {
					if (Hdf5File.existsFile(filePath)) {
						Hdf5File file = null;
						try {
							file = Hdf5File.openFile(filePath, Hdf5File.READ_ONLY_ACCESS);
							m_editTreePanel.updateTreeWithExistingFile(file);
							
						} catch (IOException ioe) {
							// TODO exception
						} finally {
							if (file != null) {
								file.close();
							}
						}
					} else {
						m_editTreePanel.updateTreeWithNewFile(filePath);
					}
				//}
			}
			
			private boolean filePathChanged(String curFilePath) {
				Object rootObject = ((DefaultMutableTreeNode) m_editTreePanel.getTree().getModel().getRoot()).getUserObject();
				if (rootObject instanceof FileNodeEdit) {
					return !curFilePath.equals(((FileNodeEdit) rootObject).getFilePath());
				}
				return true;
			}
		});
		outputPanel.add(updateButton);
		outputPanel.add(m_editTreePanel);
	}
    
    private void addListToPanel(SpecInfo specInfo, JPanel panel) {
    	ListPanel listPanel = specInfo == SpecInfo.COLUMN_SPECS ? m_columnSpecPanel
				: (specInfo == SpecInfo.FLOW_VARIABLE_SPECS ? m_flowVariableSpecPanel : null);
    	panel.add(listPanel);
		listPanel.setBorder(BorderFactory.createTitledBorder(specInfo == SpecInfo.COLUMN_SPECS ? "Columns:"
				: (specInfo == SpecInfo.FLOW_VARIABLE_SPECS ? "Flow Variables:" : "")));

    	JList<Object> list = new JList<>();
    	listPanel.setList(list);

		list.setCellRenderer(new DefaultListCellRenderer() {

			private static final long serialVersionUID = 4119451757237000581L;

			@Override
			public Component getListCellRendererComponent(JList<?> list,
					Object value, int index, boolean isSelected,
					boolean cellHasFocus) {
				super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);

				String text = null;
				Icon icon = null;
				
				if (value instanceof DataColumnSpec) {
					DataColumnSpec spec = (DataColumnSpec) value;

					text = spec.getName();
					icon = spec.getType().getIcon();
					
				} else if (value instanceof FlowVariable) {
					FlowVariable var = (FlowVariable) value;

					text = var.getName();
					switch (var.getType()) {
					case INTEGER:
						icon = FlowVariableListCellRenderer.FLOW_VAR_INT_ICON;
						break;
					case DOUBLE:
						icon = FlowVariableListCellRenderer.FLOW_VAR_DOUBLE_ICON;
						break;
					case STRING:
						icon = FlowVariableListCellRenderer.FLOW_VAR_STRING_ICON;
						break;
					default:
						icon = DataValue.UTILITY.getIcon();
					}
				}
				
				setText(text);
				setIcon(icon);
				
				return this;
			}
		});
		
		list.setDragEnabled(true);
		list.setTransferHandler(new TransferHandler() {

			private static final long serialVersionUID = -4233815652319877595L;

            public int getSourceActions(JComponent comp) {
                return COPY;
            }
             
			@SuppressWarnings("unchecked")
			protected Transferable createTransferable(JComponent comp) {
                if (comp instanceof JList) {
                	JList<?> list = (JList<?>) comp;
                	specInfo.setSpecList((List<SpecInfo>) list.getSelectedValuesList());
                	return new StringSelection(specInfo.getSpecName());
                }
                return null;
            }
		});
    }
    
	private void initListModel(SpecInfo specInfo, Object[] listElements) {
		ListPanel listPanel = specInfo == SpecInfo.COLUMN_SPECS ? m_columnSpecPanel : m_flowVariableSpecPanel;
		DefaultListModel<Object> listModel = new DefaultListModel<>();
		for (Object elem : listElements) {
			listModel.addElement(elem);
		}
		listPanel.getList().setModel(listModel);
		
		JTextField searchField = listPanel.getSearchField();
		JButton searchButton = listPanel.getSearchButton();
		JLabel searchErrorLabel = listPanel.getSearchErrorLabel();

        ActionListener actionListener = new ActionListener() {
        	
            @Override
            public void actionPerformed(final ActionEvent e) {
            	listModel.clear();
				String searchText = searchField.getText();
				try {
	            	Pattern searchRegex = Pattern.compile(!searchText.equals("") ? searchText : ".*");
	            	for (Object elem : listElements) {
	            		String text = elem instanceof DataColumnSpec ? ((DataColumnSpec) elem).getName()
	            				: (elem instanceof FlowVariable ? ((FlowVariable) elem).getName() : "");
	            		
	            		if (searchRegex.matcher(text).matches()) {
	            			listModel.addElement(elem);
	            		}
	            	}
	            	searchErrorLabel.setText("");
	            			
				} catch (PatternSyntaxException pse) {
					for (Object elem : listElements) {
						listModel.addElement(elem);
					}
					searchErrorLabel.setText("Error in regex at index " + pse.getIndex());
				}
            }
        };

        if (searchField.getActionListeners().length > 0) {
            searchField.removeActionListener(searchField.getActionListeners()[0]);
        }
        if (searchButton.getActionListeners().length > 0) {
        	searchButton.removeActionListener(searchButton.getActionListeners()[0]);
        }
        
        searchField.addActionListener(actionListener);
        searchButton.addActionListener(actionListener);
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
    	DataColumnSpec[] colSpecs = new DataColumnSpec[specs[0].getNumColumns()];
    	for (int i = 0; i < colSpecs.length; i++) {
        	colSpecs[i] = specs[0].getColumnSpec(i);
    	}
    	initListModel(SpecInfo.COLUMN_SPECS, colSpecs);
    	
    	List<?> vars = new ArrayList<>(getAvailableFlowVariables().values());
    	Collections.reverse(vars);
    	initListModel(SpecInfo.FLOW_VARIABLE_SPECS, vars.toArray());
    	
    	if (!m_filePathSettings.getStringValue().trim().isEmpty()) {
			Hdf5File file = null;
			try {
				file = Hdf5File.openFile(m_filePathSettings.getStringValue(), Hdf5File.READ_ONLY_ACCESS);
				m_editTreePanel.updateTreeWithExistingFile(file);
				
			} catch (IOException ioe) {
				// TODO exception
			} finally {
				if (file != null) {
					file.close();
				}
			}
		
			EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
			try {
				editTreeConfig.loadConfiguration(settings);
				m_editTreePanel.loadConfiguration(editTreeConfig);
				
			} catch (InvalidSettingsException ise) {
				new NotConfigurableException(ise.getMessage());
			}
		}
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		m_editTreePanel.saveConfiguration(editTreeConfig);
		editTreeConfig.saveConfiguration(settings);
    }
    
    private static class ListPanel extends JPanel {
    	
		private static final long serialVersionUID = -4510553262535977610L;
		
		private JList<Object> m_list;
		
		private final JTextField m_searchField = new JTextField(4);
        
		private final JButton m_searchButton = new JButton("Search");
        
		private final JLabel m_searchErrorLabel = new JLabel();
    	
    	private ListPanel() {
    		super(new BorderLayout());
    		
    		JPanel searchPanel = new JPanel(new BorderLayout());
            add(searchPanel, BorderLayout.NORTH);
            searchPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
            
            searchPanel.add(m_searchField, BorderLayout.CENTER);
            searchPanel.add(m_searchButton, BorderLayout.EAST);
            searchPanel.add(m_searchErrorLabel, BorderLayout.PAGE_END);
            m_searchErrorLabel.setForeground(Color.RED);
    	}

    	private JList<Object> getList() {
    		return m_list;
    	}
    	
    	private void setList(JList<Object> list) {
    		if (m_list != null) {
    			remove(m_list);
    		}
    		m_list = list;
    		final JScrollPane jsp = new JScrollPane(list);
    		add(jsp, BorderLayout.CENTER);
    	}
    	
    	private JTextField getSearchField() {
    		return m_searchField;
    	}
    	
    	private JButton getSearchButton() {
    		return m_searchButton;
    	}
    	
    	private JLabel getSearchErrorLabel() {
    		return m_searchErrorLabel;
    	}
    }
}

package org.knime.hdf5.nodes.writer;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.TransferHandler;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.data.util.ListModelFilterUtils;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {
	
	private SettingsModelString m_filePathSettings;

	private DefaultListModel<DataColumnSpec> m_columnSpecModel = new DefaultListModel<>();
	
	private DefaultListModel<FlowVariable> m_flowVariableSpecModel = new DefaultListModel<>();
	
	private EditTreePanel m_editTreePanel = new EditTreePanel();
	
    /**
     * Creates a new {@link NodeDialogPane} for the column filter in order to
     * set the desired columns.
     */
    public HDF5WriterNodeDialog() {
		createFileChooser();
		
		JPanel dataPanel = new JPanel();
		addTab("Data", dataPanel);
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
		outputPanel.add(m_editTreePanel);
	}
    
    private void createFileChooser() {
		m_filePathSettings = SettingsFactory.createFilePathSettings();
		FlowVariableModel filePathFvm = super.createFlowVariableModel(m_filePathSettings);
		DialogComponentFileChooser fileChooser = new DialogComponentFileChooser(m_filePathSettings, "outputFilePathHistory",
				JFileChooser.SAVE_DIALOG, false, filePathFvm, ".h5|.hdf5");
		fileChooser.setBorderTitle("Output file:");
		addDialogComponent(fileChooser);
		fileChooser.getModel().addChangeListener(new ChangeListener() {

			private boolean m_init = false;
			
			@Override
			public void stateChanged(ChangeEvent e) {
				if (new File(m_filePathSettings.getStringValue()).isFile() || !m_init) {
					Hdf5File file = null;
					try {
						file = getFile(OverwritePolicy.OVERWRITE);
						m_editTreePanel.updateTreeWithFile(file);
						
					} catch (IOException ioe) {
						// TODO exception (should never occur)
					} finally {
						file.close();
					}
			        m_init = true;
				}
			}
		});
	}
    
    private void addListToPanel(SpecInfo specInfo, JPanel panel) {
    	JPanel listPanel = new JPanel(new BorderLayout());
    	panel.add(listPanel);
		listPanel.setBorder(BorderFactory.createTitledBorder(specInfo == SpecInfo.COLUMN_SPECS ? "Columns:" : "Flow Variables:"));

		DefaultListModel<?> listModel = specInfo == SpecInfo.COLUMN_SPECS ? m_columnSpecModel : m_flowVariableSpecModel;
    	JList<?> list = new JList<>(listModel);
		list.setVisibleRowCount(-1);
		final JScrollPane jsp = new JScrollPane(list);
		listPanel.add(jsp, BorderLayout.CENTER);

		JPanel searchPanel = new JPanel(new BorderLayout());
        listPanel.add(searchPanel, BorderLayout.NORTH);
        searchPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        
		JTextField searchField = new JTextField(4);
        JButton searchButton = new JButton("Search");
        JCheckBox markAllHits = new JCheckBox("Select all search hits");
        searchPanel.add(searchField, BorderLayout.CENTER);
        searchPanel.add(searchButton, BorderLayout.EAST);
        searchPanel.add(markAllHits, BorderLayout.PAGE_END);
        
        ActionListener actionListener = new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                ListModelFilterUtils.onSearch(list, listModel, searchField.getText(),
                    markAllHits.isSelected());
            }
        };
        searchField.addActionListener(actionListener);
        searchButton.addActionListener(actionListener);
        
        ActionListener actionListenerAll = new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                list.clearSelection();
                ListModelFilterUtils.onSearch(list, listModel, searchField.getText(),
                    markAllHits.isSelected());
            }
        };
        markAllHits.addActionListener(actionListenerAll);

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
             
			protected Transferable createTransferable(JComponent comp) {
                if (comp instanceof JList) {
                	JList<?> list = (JList<?>) comp;
                	specInfo.setSpecList(list.getSelectedValuesList());
                	return new StringSelection(specInfo.getSpecName());
                }
                return null;
            }
		});
    }
    
    private Hdf5File getFile(OverwritePolicy policy) throws IOException {
		String filePath = m_filePathSettings.getStringValue();
		
		try {
			return Hdf5File.createFile(filePath);
			
		} catch (IOException ioe) {
			if (policy == OverwritePolicy.ABORT) {
				throw new IOException("Abort: " + ioe.getMessage());
			} else {
				return Hdf5File.openFile(filePath, Hdf5File.READ_WRITE_ACCESS);
			}
		}
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
    	m_columnSpecModel.clear();
    	for (int i = 0; i < specs[0].getNumColumns(); i++) {
        	m_columnSpecModel.addElement(specs[0].getColumnSpec(i));
    	}

    	m_flowVariableSpecModel.clear();
    	FlowVariable[] flowVariables = getAvailableFlowVariables().values().toArray(new FlowVariable[] {});
		for (FlowVariable flowVariable : flowVariables) {
			m_flowVariableSpecModel.add(0, flowVariable);
		}
		
		Hdf5File file = null;
		try {
			file = getFile(OverwritePolicy.OVERWRITE);
			m_editTreePanel.updateTreeWithFile(file);
			
		} catch (IOException ioe) {
			// TODO exception (should never occur)
		} finally {
			file.close();
		}

		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		editTreeConfig.loadConfigurationInDialog(settings);
		m_editTreePanel.loadConfiguration(editTreeConfig);
    }
    
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		m_editTreePanel.saveConfiguration(editTreeConfig);
		editTreeConfig.saveConfiguration(settings);
    }
}

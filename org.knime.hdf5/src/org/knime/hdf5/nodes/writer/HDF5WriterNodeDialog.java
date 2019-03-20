package org.knime.hdf5.nodes.writer;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.TransferHandler;
import javax.swing.WindowConstants;
import javax.swing.event.CaretEvent;
import javax.swing.event.CaretListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButton;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.nodes.writer.SettingsFactory.SpecInfo;
import org.knime.hdf5.nodes.writer.edit.EditOverwritePolicy;
import org.knime.hdf5.nodes.writer.edit.FileNodeEdit;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit;
import org.knime.hdf5.nodes.writer.edit.TreeNodeEdit.InvalidCause;

class HDF5WriterNodeDialog extends DefaultNodeSettingsPane {

	private SettingsModelString m_filePathSettings;
	
	private SettingsModelString m_fileOverwritePolicySettings;
	
	private SettingsModelBoolean m_saveColumnPropertiesSettings;
	
	private DialogComponentLabel m_fileInfoLabel = new DialogComponentLabel("");

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
		
		m_fileOverwritePolicySettings = SettingsFactory.createFileOverwritePolicySettings();
		DialogComponentButtonGroup fileOverwritePolicy = new DialogComponentButtonGroup(m_fileOverwritePolicySettings,
				false, "Overwrite file: ", EditOverwritePolicy.getNames(EditOverwritePolicy.getAvailableValuesForFile()));
		
		ChangeListener fileChangeListener = new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				updateFileInfo();
				updateFilePath();
			}
		};
		fileChooser.getModel().addChangeListener(fileChangeListener);
		fileOverwritePolicy.getModel().addChangeListener(fileChangeListener);
		
        createNewGroup("Output file:");
		addDialogComponent(fileChooser);
		addDialogComponent(m_fileInfoLabel);
		addDialogComponent(fileOverwritePolicy);
        closeCurrentGroup();
		
		JPanel inputPanel = new JPanel();
		inputPanel.setLayout(new BoxLayout(inputPanel, BoxLayout.Y_AXIS));
		inputPanel.setBorder(BorderFactory.createTitledBorder(" Input "));
		addListToPanel(SpecInfo.COLUMN_SPECS, inputPanel);
		addListToPanel(SpecInfo.FLOW_VARIABLE_SPECS, inputPanel);
    	
    	JPanel outputPanel = new JPanel();
		outputPanel.setLayout(new BoxLayout(outputPanel, BoxLayout.Y_AXIS));
		outputPanel.setBorder(BorderFactory.createTitledBorder(" Output "));
		
		JPanel buttonPanel = new JPanel();
		outputPanel.add(buttonPanel);
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.X_AXIS));

		JButton resetConfigButton = new JButton("Reset config");
		resetConfigButton.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				try {
					m_editTreePanel.updateTreeWithResetConfig();
				} catch (IOException ioe) {
		    		NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
				}
			}
		});
		buttonPanel.add(resetConfigButton);
		
		JButton removeInvalidsButton = new JButton("Reset invalid edits");
		removeInvalidsButton.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				showRemoveInvalidsDialog();
			}
			
			private void showRemoveInvalidsDialog() {
				TreeMap<TreeNodeEdit, InvalidCause> removableEdits = m_editTreePanel.getResettableEdits();
				RemoveDialog removeDialog = new RemoveDialog(buttonPanel, "Reset invalid edits", removableEdits);
				removeDialog.setLocationRelativeTo((Frame) SwingUtilities.getAncestorOfClass(Frame.class, buttonPanel));
				removeDialog.setVisible(true);
			}

			class RemoveDialog extends JDialog {
				
				private static final long serialVersionUID = -4327493810031381994L;
				
				private int m_selectedCount;

				protected RemoveDialog(Component comp, String title, TreeMap<TreeNodeEdit, InvalidCause> removableEdits) {
					super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, comp), title, true);
					setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
					// setLocation(400, 400);
					
					JPanel panel = new JPanel(new BorderLayout());
					add(panel, BorderLayout.CENTER);
					panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
					
					JPanel listPanel = new JPanel(new BorderLayout());
					panel.add(listPanel, BorderLayout.CENTER);
					
					DefaultListModel<JCheckBox> listModel = new DefaultListModel<>();
					for (TreeNodeEdit edit : removableEdits.keySet()) {
						InvalidCause cause = removableEdits.get(edit);
						JCheckBox checkBox = new JCheckBox(edit.getSummary() + " - " + cause.getMessage());
						listModel.addElement(checkBox);
					}
					
					JList<JCheckBox> checkList = new JList<>(listModel);
					checkList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
					checkList.setCellRenderer(new DefaultListCellRenderer() {

						private static final long serialVersionUID = 8312711495625029011L;

						@Override
						public Component getListCellRendererComponent(JList<?> list,
								Object value, int index, boolean isSelected,
								boolean cellHasFocus) {
							super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
							
							return (JCheckBox) value;
						}
					});

					JCheckBox selectAllBox = new JCheckBox("select all");
					listPanel.add(selectAllBox, BorderLayout.PAGE_START);
					selectAllBox.addMouseListener(new MouseAdapter() {
						
						@Override
						public void mousePressed(MouseEvent e) {
							boolean clickedSelected = !selectAllBox.isSelected();
							for (int i = 0; i < listModel.getSize(); i++) {
								listModel.getElementAt(i).setSelected(clickedSelected);
							}
				        	m_selectedCount = clickedSelected ? listModel.getSize() : 0;
				        	
							checkList.repaint();
						}
					});
					
					checkList.addMouseListener(new MouseAdapter() {
						
						@Override
						public void mousePressed(MouseEvent e) {
							int index = checkList.locationToIndex(e.getPoint());
					        if (index != -1) {
					        	JCheckBox checkBox = listModel.getElementAt(index);
					        	checkBox.setSelected(!checkBox.isSelected());
					        	m_selectedCount += checkBox.isSelected() ? 1 : -1;
					        	selectAllBox.setSelected(m_selectedCount == listModel.getSize());
					        	
								checkList.repaint();
					        }
						}
					});
					
					final JScrollPane jsp = new JScrollPane(checkList);
					jsp.setPreferredSize(new Dimension(500, 300));
					listPanel.add(jsp, BorderLayout.CENTER);
					
					JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
					panel.add(buttonPanel, BorderLayout.PAGE_END);
					JButton okButton = new JButton("OK");
					okButton.addActionListener(new ActionListener() {
						
						@Override
						public void actionPerformed(ActionEvent e) {
							List<TreeNodeEdit> resetEdits = new ArrayList<>();

							List<TreeNodeEdit> listEdits = new ArrayList<>(removableEdits.keySet());
							for (int i = 0; i < listModel.getSize(); i++) {
								JCheckBox checkBox = listModel.getElementAt(i);
								TreeNodeEdit edit = listEdits.get(i);
								if (checkBox.isSelected()) {
									resetEdits.add(edit);
								}
							}
							
							m_editTreePanel.resetEdits(resetEdits);
							setVisible(false);
						}
					});
					buttonPanel.add(okButton);
					
					JButton cancelButton = new JButton("Cancel");
					cancelButton.addActionListener(new ActionListener() {
						
						@Override
						public void actionPerformed(ActionEvent e) {
							setVisible(false);
						}
					});
					buttonPanel.add(cancelButton);
					
					pack();
				}
			}
		});
		buttonPanel.add(removeInvalidsButton);
		
		outputPanel.add(m_editTreePanel);
		

        DialogComponentButton selectDataConfigButton = new DialogComponentButton("Select data configuration");
		
        createNewGroup("Data configuration:");
		addDialogComponent(selectDataConfigButton);
        closeCurrentGroup();
        
        selectDataConfigButton.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				showDataDialog();
			}
			
			private void showDataDialog() {
				DataDialog dataDialog = new DataDialog(getPanel(), "Select data configuration");
				dataDialog.setLocationRelativeTo((Frame) SwingUtilities.getAncestorOfClass(Frame.class, getPanel()));
				dataDialog.setVisible(true);
			}

			class DataDialog extends JDialog {

				private static final long serialVersionUID = 5449228141905592744L;

				protected DataDialog(Component comp, String title) {
					super((Frame) SwingUtilities.getAncestorOfClass(Frame.class, comp), title, true);
					setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
					setMinimumSize(new Dimension(800, 600));
					// setLocation(400, 400);
					
					JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, true, inputPanel, outputPanel);
					add(splitPane, BorderLayout.CENTER);
					splitPane.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
					splitPane.setContinuousLayout(true);
					splitPane.setOneTouchExpandable(true);
					splitPane.setResizeWeight(0.3);

					JPanel selectDataButtonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
					add(selectDataButtonPanel, BorderLayout.PAGE_END);
					
					JButton okButton = new JButton("OK");
					okButton.addActionListener(new ActionListener() {
						
						@Override
						public void actionPerformed(ActionEvent e) {
							try {
								m_editTreePanel.checkConfiguration();
								setVisible(false);
								
							} catch (InvalidSettingsException ise) {
								JOptionPane.showMessageDialog(okButton, ise.getMessage(),
										"Warning: invalid settings", JOptionPane.WARNING_MESSAGE);
							}
						}
					});
					selectDataButtonPanel.add(okButton);
					
					pack();
				}
			}
		});
        

		m_saveColumnPropertiesSettings = SettingsFactory.createSaveColumnPropertiesSettings();
		DialogComponentBoolean saveColumnProperties = new DialogComponentBoolean(m_saveColumnPropertiesSettings,
				"Save column properties");
		
        createNewGroup("Advanced settings:");
		addDialogComponent(saveColumnProperties);
        closeCurrentGroup();
	}
    
    private void updateFilePath() {
    	updateFileInfo();
    	
		try {
			m_editTreePanel.updateTreeWithFile(getFilePathFromSettings(),
					EditOverwritePolicy.get(m_fileOverwritePolicySettings.getStringValue()) == EditOverwritePolicy.OVERWRITE, true);
			
		} catch (IOException ioe) {
			NodeLogger.getLogger(getClass()).error(ioe.getMessage(), ioe);
			
		} catch (InvalidSettingsException ise) {
    		// nothing to do: exception is already handled by updateFileInfo()
		}
    }
    
    private void updateFileInfo() {
    	String errorInfo = null;
    	
		String urlPath = m_filePathSettings.getStringValue();
		try {
			if (!urlPath.trim().isEmpty()) {
				String filePath = HDF5WriterNodeModel.getFilePathFromUrlPath(urlPath, false);
				
				if (Hdf5File.hasHdf5FileEnding(filePath)) {
					m_fileInfoLabel.setText("Info: File " + (Hdf5File.existsHdf5File(filePath) ? "exists" : "does not exist"));
				} else {
					errorInfo = "Error: File ending is not valid";
				}
			} else {
				errorInfo = "Error: No file selected";
			}
		} catch (InvalidSettingsException ise) {
			errorInfo = "Error: " + ise.getMessage();
		}
		
		if (errorInfo != null) {
			m_fileInfoLabel.setText("<html><font color=\"red\">" + errorInfo + "</font></html>");
		}
    }
    
    private String getFilePathFromSettings() throws InvalidSettingsException {
    	String filePath = HDF5WriterNodeModel.getFilePathFromUrlPath(m_filePathSettings.getStringValue(), false);
    	
    	if (EditOverwritePolicy.get(m_fileOverwritePolicySettings.getStringValue()) == EditOverwritePolicy.RENAME) {
    		try {
				filePath = Hdf5File.getUniqueFilePath(filePath);
			} catch (IOException ioe) {
				throw new InvalidSettingsException(ioe);
			}
    	}
    	
    	return filePath;
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
		JLabel searchErrorLabel = listPanel.getSearchErrorLabel();

        if (searchField.getCaretListeners().length > 0) {
            searchField.removeCaretListener(searchField.getCaretListeners()[0]);
        }

		searchField.addCaretListener(new CaretListener() {
			
			@Override
			public void caretUpdate(CaretEvent e) {
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
		});
    }
    
    /**
     * @param settings the node settings to read from
     * @param specs the input specs
     * @throws NotConfigurableException if no columns are available for
     *             filtering
     */
    @Override
	public void loadAdditionalSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
    	updateFileInfo();
    	
    	DataColumnSpec[] colSpecs = new DataColumnSpec[specs[0].getNumColumns()];
    	for (int i = 0; i < colSpecs.length; i++) {
        	colSpecs[i] = specs[0].getColumnSpec(i);
    	}
    	initListModel(SpecInfo.COLUMN_SPECS, colSpecs);
    	
    	List<?> vars = new ArrayList<>(getAvailableFlowVariables().values());
    	Collections.reverse(vars);
    	initListModel(SpecInfo.FLOW_VARIABLE_SPECS, vars.toArray());

		EditTreeConfiguration editTreeConfig = SettingsFactory.createEditTreeConfiguration();
		try {
    		EditOverwritePolicy policy = EditOverwritePolicy.get(m_fileOverwritePolicySettings.getStringValue());
			editTreeConfig.loadConfiguration(settings, m_editTreePanel.getFilePathOfRoot(), policy);
			
			boolean fileEditExists = editTreeConfig.getFileNodeEdit() != null;
	    	if (!m_filePathSettings.getStringValue().trim().isEmpty() || fileEditExists) {
				//m_editTreePanel.updateTreeWithResetConfig(fileEditExists ? editTreeConfig.getFilePathToUpdate(policy == EditOverwritePolicy.RENAME)
				//		: getFilePathFromSettings(), false, policy == EditOverwritePolicy.OVERWRITE);
	    		FileNodeEdit fileEdit = editTreeConfig.getFileNodeEdit();
	    		m_editTreePanel.updateTreeWithFile(fileEdit.getFilePath(), fileEdit.isOverwriteHdfFile(), false);
	    		
				// TODO test it
	    		fileEdit.validateCreateActions(colSpecs, getAvailableFlowVariables());
				m_editTreePanel.loadConfiguration(editTreeConfig);
			}
		} catch (IOException | InvalidSettingsException ioise) {
    		throw new NotConfigurableException(ioise.getMessage(), ioise);
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
		
		private final JLabel m_filterLabel = new JLabel("Filter:");
		
		private final JTextField m_searchField = new JTextField(4);
        
		private final JLabel m_searchErrorLabel = new JLabel();
    	
    	private ListPanel() {
    		super(new BorderLayout());
    		
    		JPanel searchPanel = new JPanel(new BorderLayout());
            add(searchPanel, BorderLayout.NORTH);
            searchPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

            searchPanel.add(m_filterLabel, BorderLayout.WEST);
            searchPanel.add(m_searchField, BorderLayout.CENTER);
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
    	
    	private JLabel getSearchErrorLabel() {
    		return m_searchErrorLabel;
    	}
    }
}

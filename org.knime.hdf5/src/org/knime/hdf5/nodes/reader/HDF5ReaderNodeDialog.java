package org.knime.hdf5.nodes.reader;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.Scanner;

import javax.swing.JFileChooser;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentButton;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

    private static Hdf5Group group;

    // for selection of children of the group group
    private static DialogComponentStringSelection children;
    
    private static DialogComponentStringSelection dataSets;
    
    private SettingsModelString m_source;

    private FlowVariableModel m_sourceFvm;

    private SettingsModelString m_children;
    
    private SettingsModelString m_dataSets;

    /**
     * New pane for configuring the node dialog.
     */
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
					        	group = group.getGroup(newValue);
					        	
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
    
    public void findLocation(String fname) {
    	System.out.println("File path: |" + fname + "|");
    	HDF5ReaderNodeModel.fname = fname;
    	Hdf5File file = Hdf5File.createFile(fname);
		showElements(file);
		Scanner scn = new Scanner(System.in);
		String next = "/";
		String path = "/";
		Hdf5Group group = file;
		while (next.endsWith("/")) {
			System.out.print("Enter name of dataset or (name + '/') of group."
					+ "\nIf it doesn't exist, it will be created.\n> ");
			next = scn.nextLine();
			if (next.endsWith("/")) {
				path = path + next;
				group = group.createGroup(next.substring(0, next.length() - 1));
				showElements(group);
			} else {
				HDF5ReaderNodeModel.dsname = next;
			}
		}
		
		scn.close();
		file.close();
		HDF5ReaderNodeModel.dspath = path;
		System.out.print("\nGo back to KNIME to call execute().");
    }
    
    public static void showElements(Hdf5Group group) {
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("Groups in " + (group instanceof Hdf5File ? "File " : "Group ") + group.getName() + ":\n");
		List<String> groupNames = group.loadGroupNames();
		if (groupNames != null) {
			for (String name: groupNames) {
				System.out.println(name);
			}
		}
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("Datasets in " + (group instanceof Hdf5File ? "File " : "Group ") + group.getName() + ":\n");
		List<String> dataSetNames = group.loadDataSetNames();
		if (dataSetNames != null) {
			for (String name: dataSetNames) {
				System.out.println(name);
			}
		}
		System.out.println("--------------------------------------------------------------------------------");
	}
}

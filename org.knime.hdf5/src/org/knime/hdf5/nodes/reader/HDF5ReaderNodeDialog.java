package org.knime.hdf5.nodes.reader;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Scanner;

import javax.swing.JFileChooser;

import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentButton;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.hdf5.lib.Hdf5File;
import org.knime.hdf5.lib.Hdf5Group;
import org.knime.hdf5.lib.Hdf5DataSet.Hdf5DataType;

class HDF5ReaderNodeDialog extends DefaultNodeSettingsPane {

    private SettingsModelString m_source;

    private FlowVariableModel m_sourceFvm;
    
    private SettingsModelString m_inFile;

    private FlowVariableModel m_inFileFvm;

    /**
     * New pane for configuring the node dialog.
     */
    protected HDF5ReaderNodeDialog() {
        super();
        
        m_source = SettingsFactory.createSourceSettings();
        m_sourceFvm = super.createFlowVariableModel(m_source);
        m_inFile = SettingsFactory.createSourceSettings();
        m_inFileFvm = super.createFlowVariableModel(m_inFile);
        
        // Source
        DialogComponentFileChooser source =
                new DialogComponentFileChooser(m_source, "sourceHistory", JFileChooser.OPEN_DIALOG, false, m_sourceFvm);
        source.setBorderTitle("Input file: (see console in eclipse after pressing \"Update\")");
        addDialogComponent(source);
        
        // "Update"-Button
        DialogComponentButton button = new DialogComponentButton("Update");
        button.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				// update the instance of the File which has been used
		        final SettingsModelString model = (SettingsModelString) source.getModel();
		        final String newValue = model.getStringValue();
		        
		        if (!newValue.equals("")) {
		        	findLocation(newValue);
		        }
			}
        });
        addDialogComponent(button);
        
        /*
        // Elements in File
        DialogComponentFileChooser inFile =
                new DialogComponentFileChooser(m_inFile, "inFileHistory", JFileChooser.OPEN_DIALOG, false, m_inFileFvm);
        inFile.setBorderTitle("Elements in File:");
        addDialogComponent(inFile);
        */
    }
    
    public void findLocation(String fname) {
    	System.out.println("File path: |" + fname + "|");
    	HDF5ReaderNodeModel.fname = fname;
    	Hdf5File file = Hdf5File.createInstance(fname);
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
				group = Hdf5Group.createInstance(group, next.substring(0, next.length() - 1));
				showElements(group);
			} else {
				HDF5ReaderNodeModel.dsname = next;
			}
		}
		
		scn.close();
		file.closeAll();
		HDF5ReaderNodeModel.dspath = path;
		System.out.print("\nGo back to KNIME to call execute().");
    }
    
    public static void showElements(Hdf5Group group) {
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("Groups in " + (group instanceof Hdf5File ? "File " : "Group ") + group.getName() + ":\n");
		group.loadGroups();
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("Datasets in " + (group instanceof Hdf5File ? "File " : "Group ") + group.getName() + ":\n");
		group.loadDataSets(Hdf5DataType.INTEGER);
		group.loadDataSets(Hdf5DataType.LONG);
		group.loadDataSets(Hdf5DataType.DOUBLE);
		group.loadDataSets(Hdf5DataType.STRING);
		System.out.println("--------------------------------------------------------------------------------");
	}
}

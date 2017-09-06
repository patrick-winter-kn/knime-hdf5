package org.knime.hdf5.nodes.reader;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class HDF5ReaderNodeFactory extends NodeFactory<HDF5ReaderNodeModel> {

	@Override
	public HDF5ReaderNodeModel createNodeModel() {
		// TODO Auto-generated method stub
		return new HDF5ReaderNodeModel();
	}

	@Override
	protected int getNrNodeViews() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public NodeView<HDF5ReaderNodeModel> createNodeView(int viewIndex, HDF5ReaderNodeModel nodeModel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean hasDialog() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		// TODO Auto-generated method stub
		return null;
	}

}

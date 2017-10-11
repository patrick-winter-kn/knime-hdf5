package org.knime.hdf5.nodes.reader;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class HDF5ReaderNodeFactory extends NodeFactory<HDF5ReaderNodeModel> {
	/**
     * {@inheritDoc}
     */
	@Override
	public HDF5ReaderNodeModel createNodeModel() {
		return new HDF5ReaderNodeModel();
	}

    /**
     * {@inheritDoc}
     */
	@Override
	protected int getNrNodeViews() {
		return 0;
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public NodeView<HDF5ReaderNodeModel> createNodeView(int viewIndex, HDF5ReaderNodeModel nodeModel) {
		return null;
	}

    /**
     * {@inheritDoc}
     */
	@Override
	protected boolean hasDialog() {
		return true;
	}

    /**
     * {@inheritDoc}
     */
	@Override
	protected NodeDialogPane createNodeDialogPane() {
		return new HDF5ReaderNodeDialog();
	}
}

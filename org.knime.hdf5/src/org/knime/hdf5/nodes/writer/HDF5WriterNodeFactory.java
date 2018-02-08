package org.knime.hdf5.nodes.writer;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class HDF5WriterNodeFactory extends NodeFactory<HDF5WriterNodeModel> {
	/**
     * {@inheritDoc}
     */
	@Override
	public HDF5WriterNodeModel createNodeModel() {
		return new HDF5WriterNodeModel();
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
	public NodeView<HDF5WriterNodeModel> createNodeView(int viewIndex, HDF5WriterNodeModel nodeModel) {
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
		return new HDF5WriterNodeDialog();
	}
}

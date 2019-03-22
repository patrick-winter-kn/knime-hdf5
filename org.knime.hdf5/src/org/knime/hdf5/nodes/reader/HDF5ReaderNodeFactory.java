package org.knime.hdf5.nodes.reader;

import org.knime.core.node.ContextAwareNodeFactory;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 * The Factory Class for the hdf reader.
 */
public class HDF5ReaderNodeFactory extends ContextAwareNodeFactory<HDF5ReaderNodeModel> {
	
	@Override
	public HDF5ReaderNodeModel createNodeModel() {
		return new HDF5ReaderNodeModel();
	}
	
	@Override
	public HDF5ReaderNodeModel createNodeModel(NodeCreationContext context) {
		return new HDF5ReaderNodeModel(context);
	}

	@Override
	protected int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<HDF5ReaderNodeModel> createNodeView(int viewIndex, HDF5ReaderNodeModel nodeModel) {
		return null;
	}

	@Override
	protected boolean hasDialog() {
		return true;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		return new HDF5ReaderNodeDialog();
	}
}

package org.knime.hdf5.nodes.writer;

import org.knime.core.node.ContextAwareNodeFactory;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 * The Factory Class for the hdf writer.
 */
public class HDF5WriterNodeFactory extends ContextAwareNodeFactory<HDF5WriterNodeModel> {

	@Override
	public HDF5WriterNodeModel createNodeModel() {
		return new HDF5WriterNodeModel();
	}
	
	@Override
	public HDF5WriterNodeModel createNodeModel(NodeCreationContext context) {
		return new HDF5WriterNodeModel(context);
	}

	@Override
	protected int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<HDF5WriterNodeModel> createNodeView(int viewIndex, HDF5WriterNodeModel nodeModel) {
		return null;
	}

	@Override
	protected boolean hasDialog() {
		return true;
	}
	
	@Override
	protected NodeDialogPane createNodeDialogPane() {
		return new HDF5WriterNodeDialog();
	}
}

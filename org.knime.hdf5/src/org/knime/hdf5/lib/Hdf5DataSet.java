package org.knime.hdf5.lib;

import java.rmi.AlreadyBoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataCell;
import org.knime.core.data.MissingCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
	private boolean m_dimsAvailable;
	
	private final Hdf5DataType m_type;
	
	private Hdf5DataSet(final Hdf5Group parent, final String name, long[] dimensions,
			long stringLength, final Hdf5DataType type, boolean create) 
					throws NullPointerException, IllegalArgumentException {
		super(name, parent.getFilePath());
		m_type = type;

		if (create) {
			createDimensions(dimensions, stringLength);
	        
	        // Create the dataset.
	        try {
	            if (getDataspaceId() >= 0 && getType().getConstants()[0] >= 0) {
	                setElementId(H5.H5Dcreate(parent.getElementId(), getName(),
	                        getType().getConstants()[0], getDataspaceId(),
	                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT));
	        		parent.addDataSet(this);
	                setOpen(true);
		        }
	        } catch (HDF5LibraryException | NullPointerException hlnpe) {
	            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be created", hlnpe);
	        }
		} else {
			parent.addDataSet(this);
			open();
		}
	}
	
	/**
	 * Creates the new instance of the dataSet.
	 * 
	 * @param parent group
	 * @param name
	 * @param dimensions
	 * @param stringLength only matters for String dataSets
	 * @param type
	 * @param create {@code true} if the dataSet should also be created physically in the .h5 file
	 * @return dataSet
	 */
	static Hdf5DataSet<?> getInstance(final Hdf5Group parent, final String name,
			long[] dimensions, long stringLength, Hdf5DataType type, boolean create) {
		if (parent == null) {
			NodeLogger.getLogger("HDF5 Files").error("parent of dataSet " + name + " cannot be null",
					new NullPointerException());
			return null;
		} else if (!parent.isOpen()) {
			NodeLogger.getLogger("HDF5 Files").error("parent group " + parent.getPathFromFile()
					+ parent.getName() + " is not open!", new IllegalStateException());
			return null;
		} else {
			try {
				switch (type.getKnimeType()) {
					case INTEGER:
						return new Hdf5DataSet<Integer>(parent, name, dimensions, 0, type, create);
					case LONG:
						return new Hdf5DataSet<Long>(parent, name, dimensions, 0, type, create);
					case DOUBLE:
						return new Hdf5DataSet<Double>(parent, name, dimensions, 0, type, create);
					case STRING:
						return new Hdf5DataSet<String>(parent, name, dimensions, stringLength, type, create);
					default:
						return null;
					}
			} catch (NullPointerException | IllegalArgumentException npiae) {
				return null;
			}
		}
	}

	private long getDataspaceId() {
		return m_dataspaceId;
	}

	private void setDataspaceId(long dataspaceId) {
		m_dataspaceId = dataspaceId;
	}

	public long[] getDimensions() {
		return m_dimensions;
	}
	
	private void setDimensions(long[] dimensions) {
		m_dimensions = dimensions;
	}
	
	public Hdf5DataType getType() {
		return m_type;
	}
	
	/**
	 * calculates the number of values/cells which can be saved in the dimensions array
	 * 
	 * @return product of the numbers in the dimensions array 
	 */
	public long numberOfValues() {
		return numberOfValuesRange(0, getDimensions().length);
	}
	
	/**
	 * calculates the number of values/cells which can be saved within the range <br>
	 * { fromIndex - getDimensions().length - 1 } of the dimensions array
	 * 
	 * @param fromIndex inclusive
	 * @return product of the numbers in the dimensions array in this range 
	 */
	public long numberOfValuesFrom(int fromIndex) {
		return numberOfValuesRange(fromIndex, getDimensions().length);
	}

	/**
	 * calculates the number of values/cells which can be saved within the range <br>
	 * { fromIndex, ..., toIndex - 1 } of the dimensions array
	 * 
	 * @param fromIndex inclusive
	 * @param toIndex exclusive
	 * @return product of the numbers in the dimensions array in this range 
	 */
	public long numberOfValuesRange(int fromIndex, int toIndex) {
		if (0 <= fromIndex && fromIndex <= toIndex && toIndex <= getDimensions().length) {
			long values = 1;
			long[] dims = Arrays.copyOfRange(getDimensions(), fromIndex, toIndex);
			
			for (long l: dims) {
				values *= l;
			}
			
			return values;
		}
		NodeLogger.getLogger("HDF5 Files").error("Hdf5DataSet.numberOfValuesRange(" + fromIndex + ", " + toIndex
				+ "): is out of range (0, " + getDimensions().length + ")", new IllegalArgumentException());
		return 0;
	}
	
	/**
	 * increments the colDims array
	 * 
	 * @param colDims the dimensions array of all columns
	 * @return true if colDims incremented successfully and didn't go from max to min
	 */
	public boolean nextColumnDims(long[] colDims) {
		if (colDims.length == getDimensions().length - 1) {
			long[] dims = getDimensions();
			int i = colDims.length - 1;
			boolean transfer = true;
			
			while (transfer && i >= 0) {
				colDims[i] = (colDims[i] + 1) % dims[i+1];
				transfer = colDims[i] == 0;
				i--;
			}
			
			return !transfer;
		} else {
			NodeLogger.getLogger("HDF5 Files").error("Hdf5DataSet.nextColumnDims(colDims): colDims must have the length "
					+ (getDimensions().length - 1) + ", but has the length " + colDims.length + ")",
					new IllegalArgumentException());
			return false;
		}
	}
	
	/**
	 * 
	 * @param dataIn
	 * @return {@code true} if the dataSet was written otherwise {@code false}
	 */
	public boolean write(Type[] dataIn) {
		// Write the data to the dataSet.
        try {
			if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
	        	if (isOpen() && (getType().getConstants()[1] >= 0)) {
					H5.H5Dwrite_string(getElementId(), getType().getConstants()[1],
							HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
							HDF5Constants.H5P_DEFAULT, (String[]) dataIn);
					return true;
				}
        	} else {
	            if (isOpen()) {
	                H5.H5Dwrite(getElementId(), getType().getConstants()[1],
	                        HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
	                        HDF5Constants.H5P_DEFAULT, dataIn);
	    			return true;
	            }
            }
        } catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be written", hnpe);
        }
		return false;
	}
	
	public Type[] readRow(long rowId) {
		return readRows(rowId, rowId + 1);
	}
	
	/**
	 * 
	 * @param fromRow inclusive row index
	 * @param toRow exclusive row index
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Type[] readRows(long fromRow, long toRow) {
		Type[] dataOut = null;
		
		try {
	        if (isOpen()) {
	        	Hdf5DataType dataType = getType();
	        	dataOut = (Type[]) dataType.getKnimeType().createArray((int) numberOfValuesFrom(1));

	            long memSpaceId = H5.H5Screate_simple(m_dimensions.length - 1,
	            		Arrays.copyOfRange(m_dimensions, 1, m_dimensions.length), null);
	            
	            /*
	             * chunking is only possible for dataSets with available (more than 0) dimensions
	             * according to H5.H5Sget_simple_extent_dims()
	             */
	            if (m_dimsAvailable) {
		        	long[] offset = new long[m_dimensions.length];
					offset[0] = fromRow;
					long[] count = m_dimensions.clone();
					count[0] = toRow - fromRow;
					
					H5.H5Sselect_hyperslab(m_dataspaceId, HDF5Constants.H5S_SELECT_SET,
							offset, null, count, null);
	            }
				
				if (dataType.isKnimeType(Hdf5KnimeDataType.STRING)) {
	                if (dataType.isVlen()) {
						long typeId = H5.H5Tget_native_type(dataType.getConstants()[0]);
	                    H5.H5DreadVL(getElementId(), typeId,
	                    		memSpaceId, m_dataspaceId,
								HDF5Constants.H5P_DEFAULT, dataOut);
	    				H5.H5Tclose(typeId);
	                    
	                } else {
						H5.H5Dread_string(getElementId(), dataType.getConstants()[1],
								memSpaceId, m_dataspaceId,
								HDF5Constants.H5P_DEFAULT, (String[]) dataOut);
					}
					
				} else if (dataType.equalTypes()) {
		            H5.H5Dread(getElementId(), dataType.getConstants()[1],
		            		memSpaceId, m_dataspaceId,
		                    HDF5Constants.H5P_DEFAULT, dataOut);
					
				} else {
					Object[] dataRead = (Object[]) dataType.getHdfType().createArray((int) numberOfValuesFrom(1));
		            H5.H5Dread(getElementId(), dataType.getConstants()[1],
		            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataRead);
					for (int i = 0; i < dataRead.length; i++) {
						dataOut[i] = (Type) dataType.hdfToKnime(dataRead[i]);
					}
				}
			}
	    }  catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException | IllegalArgumentException hludtnpiae) {
            NodeLogger.getLogger("HDF5 Files").error(hludtnpiae.getMessage(), hludtnpiae);
        }
		
		return dataOut;
	}
	
	public void extendRow(List<DataCell> row, long rowId) {
		// TODO if rowNum bigger than int_max
		long rowNum = m_dimensions[0];
		long colNum = (int) numberOfValuesFrom(1);
		
		if (rowId < rowNum) {
			Type[] dataRead = readRow(rowId);
			
			for (int c = 0; c < colNum; c++) {
				row.add(getDataCell(dataRead[c]));
			}
			
		} else {
			for (int c = 0; c < colNum; c++) {
				row.add(getDataCell(null));
			}
		}
	}
	
	private DataCell getDataCell(Type value) {
		if (value == null) {
			return new MissingCell("(null) on joining dataSets");
		}
		
		switch (getType().getKnimeType()) {
		case INTEGER:
			return new IntCell((int) value);
		case LONG:
			return new LongCell((long) value);
		case DOUBLE:
			return new DoubleCell((double) value);
		case STRING:
			return new StringCell("" + value);
		default:
			return null;
		}
	}
	
	public void open() {
		try {
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				setElementId(H5.H5Dopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT));
				setOpen(true);
				loadDimensions();
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be opened", hlnpe);
        }
	}
	
	private void createDimensions(long[] dimensions, long stringLength) {
		setDimensions(dimensions);
		
		if (m_type.isHdfType(Hdf5HdfDataType.STRING)) {
			try {
				m_type.getHdfType().createInstanceString(getElementId(), stringLength);
			} catch (AlreadyBoundException e) {
				// TODO Auto-generated catch block
			}
        }
		
    	// Create the data space for the dataset.
        try {
            setDataspaceId(H5.H5Screate_simple(getDimensions().length,
            		getDimensions(), null));
        } catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Data space could not be created", hnpe);
        }
	}
	
	/**
	 * Updates the dimensions array after opening a dataset to ensure that the
	 * dimensions array is correct.
	 */
	private void loadDimensions() {
		// Get dataspace and allocate memory for read buffer.
		try {
			if (isOpen()) {
				setDataspaceId(H5.H5Dget_space(getElementId()));
			}
			
			int ndims = -1;
			if (getDataspaceId() >= 0) {
				ndims = H5.H5Sget_simple_extent_ndims(getDataspaceId());
			}
			
			long[] dims = new long[ndims];
			if (getDataspaceId() >= 0) {
				H5.H5Sget_simple_extent_dims(getDataspaceId(), dims, null);

				m_dimsAvailable = dims.length != 0;
				if (!m_dimsAvailable) {
					NodeLogger.getLogger("HDF5 Files").warn("DataSet \"" + getPathFromFileWithName() + "\" has 0 dimensions. "
							+ "Dimensions are set to dims = new long[]{ 1L } now.");
					dims = new long[]{ 1L };
				}
				
				setDimensions(dims);
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Dimensions could not be loaded", hlnpe);
        }
        
		if (m_type.isHdfType(Hdf5HdfDataType.STRING)) {
			m_type.getHdfType().initInstanceString(getElementId());
    	}
	}
	
	public void close() {
		// End access to the dataset and release resources used by it.
        try {
            if (isOpen()) {
        		Iterator<Hdf5Attribute<?>> iter = getAttributes().iterator();
        		while (iter.hasNext()) {
        			iter.next().close();
        		}

                m_type.getHdfType().closeIfString();
        		
                // Terminate access to the data space.
                if (m_dataspaceId >= 0) {
                	H5.H5Sclose(m_dataspaceId);
                    m_dataspaceId = -1;
                }
                
                H5.H5Dclose(getElementId());
                setOpen(false);
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be closed", hle);
        }
	}
}

package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.MissingCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.JoinedRow;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
	private long m_stringLength;
	
	private final Hdf5DataType m_type;
	
	/**
	 * Creates a dataSet of the type of {@code type}. <br>
	 * Possible types of numbers are Integer, Long, Double and String. <br>
	 * <br>
	 * The last index of the dimensions array represents the maximum length of
	 * the strings plus one more space for the null terminator. <br>
	 * So if you want the stringLength 7, the last index of the array should be 8.
	 * 
	 * @param name
	 * @param dimensions
	 * @param type
	 */
	private Hdf5DataSet(final Hdf5Group parent, final String name, long[] dimensions,
			long stringLength, final Hdf5DataType type, boolean create) {
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
	        } catch (Exception e) {
	            e.printStackTrace();
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
		} else switch (type.getKnimeType()) {
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

	public long getStringLength() {
		return m_stringLength;
	}

	private void setStringLength(long stringLength) {
		m_stringLength = stringLength;
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
        } catch (Exception e) {
            e.printStackTrace();
        }
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public Type[] read() {
		Type[] dataOut = null;
		
		try {
	        if (isOpen()) {
	        	Hdf5DataType dataType = getType();
	        	dataOut = (Type[]) dataType.getKnimeType().createArray((int) numberOfValues());
				
				if (dataType.isKnimeType(Hdf5KnimeDataType.STRING)) {
	                if (dataType.isVlen()) {
						long typeId = H5.H5Tget_native_type(dataType.getConstants()[0]);
	                    H5.H5DreadVL(getElementId(), typeId,
								HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
								HDF5Constants.H5P_DEFAULT, dataOut);
	    				H5.H5Tclose(typeId);
	                    
	                } else {
						H5.H5Dread_string(getElementId(), dataType.getConstants()[1],
								HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
								HDF5Constants.H5P_DEFAULT, (String[]) dataOut);
					}
					
				} else if (dataType.equalTypes()) {
		            H5.H5Dread(getElementId(), dataType.getConstants()[1],
		                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
		                    HDF5Constants.H5P_DEFAULT, dataOut);
					
				} else {
					Object[] dataRead = (Object[]) dataType.getHdfType().createArray((int) numberOfValues());
		            H5.H5Dread(getElementId(), dataType.getConstants()[1],
		                    HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
		                    HDF5Constants.H5P_DEFAULT, dataRead);
					for (int i = 0; i < dataRead.length; i++) {
						dataOut[i] = (Type) dataType.hdfToKnime(dataRead[i]);
					}
				}
			}
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
		
		return dataOut;
	}
	
	public Type readCell(int... indices) {
		int index = 0;
		if (indices.length == getDimensions().length) {
			for (int i = 0; i < indices.length; i++) {
				if (indices[i] >= 0 && indices[i] < getDimensions()[i]) {
					index += indices[i] * (int) numberOfValuesRange(i+1, indices.length);
				} else {
					NodeLogger.getLogger("HDF5 Files").error("Index out of bounds: indices[" + i + "] = "
							+ indices[i] + " and should be from 0 to " + (getDimensions()[i] - 1),
							new IllegalArgumentException());
					return null;
				}
		    }
			return read()[index];
		}
		NodeLogger.getLogger("HDF5 Files").error("Not the correct amount (" + getDimensions().length
				+ ") of indices!: " + indices.length, new IllegalArgumentException());
		return null;
	}
	
	public void extendRows(DataRow[] rows) {
		Type[] dataRead = read();
		
		if (getDimensions().length != 0) {
			int rowNum = (int) getDimensions()[0];
			int rowNumLen = (int) Math.ceil(Math.log10(rows.length));
			int colNum = (int) numberOfValuesFrom(1);
			
			for (int c = 0; c < colNum; c++) {
				for (int r = 0; r < rows.length; r++) {
					DefaultRow row = new DefaultRow("Row" + String.format("%0" + rowNumLen + "d", r),
							getDataCell(r < rowNum ? dataRead[r * colNum + c] : null));
					rows[r] = (rows[r] == null) ? row : new JoinedRow(rows[r], row);
				}
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
				updateDimensions();
			}
		} catch (HDF5LibraryException | NullPointerException e) {
			e.printStackTrace();
		}
	}
	
	private void createDimensions(long[] dimensions, long stringLength) {
		setDimensions(dimensions);
		
		if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
			setStringLength(stringLength);
			getType().createStringTypes(getStringLength());
    	}
		
    	// Create the data space for the dataset.
        try {
            setDataspaceId(H5.H5Screate_simple(getDimensions().length,
            		getDimensions(), null));
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	/**
	 * Updates the dimensions array after opening a dataset to ensure that the
	 * dimensions array is correct.
	 */
	private void updateDimensions() {
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
				
				if (dims.length == 0) {
					NodeLogger.getLogger("HDF5 Files").warn("DataSet \"" + getPathFromFileWithName() + "\" has 0 dimensions. "
							+ "Dimensions are set to dims = new long[]{ 1L } now.");
					dims = new long[]{ 1L };
				}
				
				setDimensions(dims);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
        
		if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
			setStringLength(getType().updateStringTypes(getElementId()));
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

                if (getType().isHdfType(Hdf5HdfDataType.STRING)) {
        			// Terminate access to the file and mem type.
                	getType().closeStringTypes();
        		}
        		
                // Terminate access to the data space.
                if (getDataspaceId() >= 0) {
                	H5.H5Sclose(getDataspaceId());
                    setDataspaceId(-1);
                }
                
                H5.H5Dclose(getElementId());
                setOpen(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

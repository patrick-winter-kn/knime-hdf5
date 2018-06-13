package org.knime.hdf5.lib;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
	private boolean m_dimsAvailable;
	
	private int m_compressionLevel;
	
	private long m_chunkRowSize;
	
	private Hdf5DataType m_type;
	
	private Hdf5DataSet(final Hdf5Group parent, final String name, final Hdf5DataType type) 
			throws NullPointerException, IllegalArgumentException {
		super(name, parent.getFilePath());
		
		m_type = type;
	}
	
	private static Hdf5DataSet<?> getInstance(final Hdf5Group parent, final String name, Hdf5DataType type) throws IllegalStateException {
		if (parent == null) {
			throw new IllegalArgumentException("Parent group of dataSet \"" + name + "\" cannot be null");
			
		} else if (!parent.isOpen()) {
			throw new IllegalStateException("Parent group \"" + parent.getPathFromFileWithName() + "\" is not open");
		
		} else if (type == null) {
			throw new IllegalArgumentException("DataType for dataSet \"" + parent.getPathFromFileWithName() + name + "\" cannot be null");
		}
		
		switch (type.getKnimeType()) {
		case INTEGER:
			return new Hdf5DataSet<Integer>(parent, name, type);
		case LONG:
			return new Hdf5DataSet<Long>(parent, name, type);
		case DOUBLE:
			return new Hdf5DataSet<Double>(parent, name, type);
		case STRING:
			return new Hdf5DataSet<String>(parent, name, type);
		default:
			NodeLogger.getLogger("HDF5 Files").warn("DataSet \""
					+ parent.getPathFromFileWithName() + name + "\" has an unknown dataType");
			return null;
		}
	}
	
	static Hdf5DataSet<?> createDataSet(final Hdf5Group parent, final String name,
			long[] dimensions, int compressionLevel, long chunkRowSize, Hdf5DataType type) {
		Hdf5DataSet<?> dataSet = null;
		
		try {
			dataSet = getInstance(parent, name, type);
			dataSet.createDimensions(dimensions);
			long propertyListId = dataSet.createChunking(compressionLevel, chunkRowSize);

	        dataSet.setElementId(H5.H5Dcreate(parent.getElementId(), dataSet.getName(),
            		dataSet.getType().getConstants()[0], dataSet.getDataspaceId(),
                    HDF5Constants.H5P_DEFAULT, propertyListId, HDF5Constants.H5P_DEFAULT));
	        
            H5.H5Pclose(propertyListId);
	        
    		parent.addDataSet(dataSet);
    		dataSet.setOpen(true);
    		
		} catch (HDF5Exception | NullPointerException | IllegalArgumentException | IllegalStateException hnpiaise) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be created: " + hnpiaise.getMessage(), hnpiaise);
			/* dataSet stays null */
        }
		
		return dataSet;
	}
	
	static Hdf5DataSet<?> openDataSet(final Hdf5Group parent, final String name) {
		Hdf5DataSet<?> dataSet = null;
		
		try {
			Hdf5DataType type = parent.findDataSetType(name);
			dataSet = getInstance(parent, name, type);
			
	    	parent.addDataSet(dataSet);
	    	dataSet.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be opened: " + npiaise.getMessage(), npiaise);
			/* dataSet stays null */
            
        } catch (UnsupportedDataTypeException udte) {
			NodeLogger.getLogger("HDF5 Files").warn(udte.getMessage());
			/* dataSet stays null */
        } 
		
		return dataSet;
	}

	private long getDataspaceId() {
		return m_dataspaceId;
	}
	
	public long[] getDimensions() {
		return m_dimensions;
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
	
	private long selectChunkOfRows(long fromRow, long toRow) throws HDF5Exception, NullPointerException {
		long memSpaceId = H5.H5Screate_simple(m_dimensions.length - 1,
        		Arrays.copyOfRange(m_dimensions, 1, m_dimensions.length), null);
        
        /*
         * using chunks for reading dataSets is only possible with available (more than 0) dimensions
         * according to H5.H5Sget_simple_extent_dims() (method is used when dataSet is being read)
         */
        if (m_dimsAvailable) {
        	long[] offset = new long[m_dimensions.length];
			offset[0] = fromRow;
			long[] count = m_dimensions.clone();
			count[0] = toRow - fromRow;
			
			H5.H5Sselect_hyperslab(m_dataspaceId, HDF5Constants.H5S_SELECT_SET,
					offset, null, count, null);
        }
        
        return memSpaceId;
	}
	
	public boolean writeRow(Type[] dataIn, long rowId) {
		return writeRows(dataIn, rowId, rowId + 1);
	}
	
	/**
	 * 
	 * @param fromRow inclusive row index
	 * @param toRow exclusive row index
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean writeRows(Type[] dataIn, long fromRow, long toRow) {
		try {
	        if (isOpen()) {
	        	long memSpaceId = selectChunkOfRows(fromRow, toRow);
				
	            if (isOpen()) {
	    			if (m_type.isHdfType(HdfDataType.STRING)) {
	    				H5.H5Dwrite_string(getElementId(), m_type.getConstants()[1],
	    						memSpaceId, m_dataspaceId,
	    						HDF5Constants.H5P_DEFAULT, (String[]) dataIn);
	    				
	    			} else if (m_type.hdfTypeEqualsKnimeType()) {
	                    H5.H5Dwrite(getElementId(), m_type.getConstants()[1],
	                    		memSpaceId, m_dataspaceId,
	                            HDF5Constants.H5P_DEFAULT, dataIn);
	                
	    			} else {
						Object[] dataWrite = (Object[]) m_type.getHdfType().createArray((int) numberOfValuesFrom(1));
			            
						Class hdfClass = m_type.getHdfClass();
						Class knimeClass = m_type.getKnimeClass();
						for (int i = 0; i < dataWrite.length; i++) {
							dataWrite[i] = m_type.knimeToHdf(knimeClass, knimeClass.cast(dataIn[i]), hdfClass);
						}
						
			            H5.H5Dwrite(getElementId(), m_type.getConstants()[1],
			            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataWrite);
					}
	    			
					H5.H5Sclose(memSpaceId);
					
	    			return true;
	    			
	        	} else {
	                throw new IllegalStateException("DataSet \"" + getPathFromFileWithName() + "\" is not open: data could not be written into it");
	        	}
			}
	    } catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException | IllegalArgumentException | IllegalStateException hudtnpiaise) {
            NodeLogger.getLogger("HDF5 Files").error(hudtnpiaise.getMessage(), hudtnpiaise);
        }
		
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public boolean writeRowToDataSet(DataRow row, int[] specIndex, long rowId) throws UnsupportedDataTypeException {
		Type[] dataIn = (Type[]) m_type.getKnimeType().createArray((int) numberOfValuesFrom(1));
		
		for (int i = 0; i < dataIn.length; i++) {
			dataIn[i] = getValueFromDataCell(row.getCell(specIndex[i]));
		}
		
		return writeRow(dataIn, rowId);
	}
	
	@SuppressWarnings("unchecked")
	private Type getValueFromDataCell(DataCell dataCell) throws UnsupportedDataTypeException {
		DataType type = dataCell.getType();
		switch (getType().getKnimeType()) {
		case INTEGER:
			if (type.equals(IntCell.TYPE)) {	
				return (Type) (Integer) ((IntCell) dataCell).getIntValue();
			}
			break;
		case LONG:
			if (type.equals(IntCell.TYPE)) {	
				return (Type) (Long) (long) ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {	
				return (Type) (Long) ((LongCell) dataCell).getLongValue();
			} 
			break;
		case DOUBLE:
			if (type.equals(IntCell.TYPE)) {	
				return (Type) (Double) (double) ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {	
				return (Type) (Double) (double) ((LongCell) dataCell).getLongValue();
			} else if (type.equals(DoubleCell.TYPE)) {	
				return (Type) (Double) ((DoubleCell) dataCell).getDoubleValue();
			}
			break;
		case STRING:
			if (type.equals(IntCell.TYPE)) {	
				return (Type) ("" + ((IntCell) dataCell).getIntValue());
			} else if (type.equals(LongCell.TYPE)) {	
				return (Type) ("" + ((LongCell) dataCell).getLongValue());
			} else if (type.equals(DoubleCell.TYPE)) {	
				return (Type) ("" + ((DoubleCell) dataCell).getDoubleValue());
			} else if (type.equals(StringCell.TYPE)) {
				return (Type) ((StringCell) dataCell).getStringValue();
			}
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
		
		throw new UnsupportedDataTypeException("Unsupported combination of dataCellDataType and knimeDataType");
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Type[] readRows(long fromRow, long toRow) throws IllegalStateException {
		Type[] dataOut = null;
		
		try {
	        if (isOpen()) {
	        	dataOut = (Type[]) m_type.getKnimeType().createArray((int) numberOfValuesFrom(1));

	            long memSpaceId = selectChunkOfRows(fromRow, toRow);
				
				if (m_type.isHdfType(HdfDataType.STRING)) {
	                if (m_type.isVlen()) {
						long typeId = H5.H5Tget_native_type(m_type.getConstants()[0]);
	                    H5.H5DreadVL(getElementId(), typeId,
	                    		memSpaceId, m_dataspaceId,
								HDF5Constants.H5P_DEFAULT, dataOut);
	    				H5.H5Tclose(typeId);
	                    
	                } else {
						H5.H5Dread_string(getElementId(), m_type.getConstants()[1],
								memSpaceId, m_dataspaceId,
								HDF5Constants.H5P_DEFAULT, (String[]) dataOut);
					}
					
				} else if (m_type.hdfTypeEqualsKnimeType()) {
		            H5.H5Dread(getElementId(), m_type.getConstants()[1],
		            		memSpaceId, m_dataspaceId,
		                    HDF5Constants.H5P_DEFAULT, dataOut);
					
				} else {
					Object[] dataRead = (Object[]) m_type.getHdfType().createArray((int) numberOfValuesFrom(1));
		            H5.H5Dread(getElementId(), m_type.getConstants()[1],
		            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataRead);
		            
					Class hdfClass = m_type.getHdfClass();
					Class knimeClass = m_type.getKnimeClass();
					for (int i = 0; i < dataRead.length; i++) {
						dataOut[i] = (Type) m_type.hdfToKnime(hdfClass, hdfClass.cast(dataRead[i]), knimeClass);
					}
				}
				
				H5.H5Sclose(memSpaceId);
			} else {
                throw new IllegalStateException("DataSet \"" + getPathFromFileWithName() + "\" is not open: data could not be read");
        	}
	    } catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException | IllegalArgumentException hudtnpiae) {
            NodeLogger.getLogger("HDF5 Files").error(hudtnpiae.getMessage(), hudtnpiae);
        }
		
		return dataOut;
	}
	
	public void extendRow(List<DataCell> row, long rowId) throws UnsupportedDataTypeException {
		long rowNum = m_dimensions[0];
		int colNum = (int) numberOfValuesFrom(1);
		
		if (rowId < rowNum) {
			try {
				Type[] dataRead = readRow(rowId);
				
				for (int c = 0; c < colNum; c++) {
					row.add(getDataCell(dataRead[c]));
				}
			} catch (IllegalStateException ise) {
				NodeLogger.getLogger("HDF5 Files").error(ise.getMessage(), ise);
			}
		} else {
			for (int c = 0; c < colNum; c++) {
				row.add(getDataCell(null));
			}
		}
	}
	
	private DataCell getDataCell(Type value) throws UnsupportedDataTypeException {
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
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
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
				loadChunkInfo();
			}
		} catch (HDF5LibraryException | NullPointerException | IllegalStateException hlnpise) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be opened", hlnpise);
        }
	}
	
	private void createDimensions(long[] dimensions) {
		m_dimensions = dimensions;
		
		// Create the dataSpace for the dataSet.
        try {
            m_dataspaceId = H5.H5Screate_simple(m_dimensions.length,
            		m_dimensions, null);
			m_dimsAvailable = m_dimensions.length != 0;
            
        } catch (HDF5Exception | NullPointerException hnpe) {
            NodeLogger.getLogger("HDF5 Files").error("DataSpace could not be created", hnpe);
        }
	}
	
	/**
	 * 
	 * @param compressionLevel
	 * @param chunkRowSize
	 * @return							id of property list (H5P)
	 * @throws HDF5Exception
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	private long createChunking(int compressionLevel, long chunkRowSize) throws HDF5Exception, NullPointerException, IllegalArgumentException {
		long propertyListId = HDF5Constants.H5P_DEFAULT;
		
		if (compressionLevel > 0) {
			if (chunkRowSize > 0) {
				propertyListId = H5.H5Pcreate(HDF5Constants.H5P_DATASET_CREATE);
                H5.H5Pset_layout(propertyListId, HDF5Constants.H5D_CHUNKED);
                
				long[] chunks = new long[m_dimensions.length];
				Arrays.fill(chunks, 1);
				chunks[0] = chunkRowSize;
                H5.H5Pset_chunk(propertyListId, chunks.length, chunks);
                H5.H5Pset_deflate(propertyListId, compressionLevel);
                
    			m_compressionLevel = compressionLevel;
    			m_chunkRowSize = chunkRowSize;
                
			} else {
				throw new IllegalArgumentException("chunkRowSize must be >0 due to a compressionLevel >0");
			}
		}
		
        return propertyListId;
	}
	
	/**
	 * Updates the dimensions array after opening a dataSet to ensure that the
	 * dimensions array is correct.
	 */
	private void loadDimensions() throws IllegalStateException {
		// Get dataSpace and allocate memory for read buffer.
		try {
			if (isOpen()) {
				m_dataspaceId = H5.H5Dget_space(getElementId());
				
				int ndims = H5.H5Sget_simple_extent_ndims(m_dataspaceId);
				long[] dimensions = new long[ndims];
				H5.H5Sget_simple_extent_dims(m_dataspaceId, dimensions, null);

				m_dimsAvailable = dimensions.length != 0;
				if (!m_dimsAvailable) {
					NodeLogger.getLogger("HDF5 Files").warn("DataSet \"" + getPathFromFileWithName() + "\" has 0 dimensions. "
							+ "Dimensions are set to dims = new long[]{ 1L } now.");
					dimensions = new long[]{ 1L };
				}
				
				m_dimensions = dimensions;
				
			} else {
				throw new IllegalStateException("DataSet is not open");
			}
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            NodeLogger.getLogger("HDF5 Files").error("Dimensions could not be loaded", hlnpe);
        }
	}
	
	private void loadChunkInfo() throws HDF5LibraryException {
		long propertyListId = H5.H5Dget_create_plist(getElementId());
		int layoutType = H5.H5Pget_layout(propertyListId);
        m_compressionLevel = 0;
    	m_chunkRowSize = 0;
		
		if (layoutType == HDF5Constants.H5D_CHUNKED) {
			int[] values = new int[1];
            H5.H5Pget_filter(propertyListId, 0, new int[1], new long[] { 1 }, values, 1, new String[1], new int[1]);
            m_compressionLevel = values[0];
            
            if (m_compressionLevel > 0) {
    			long[] chunks = new long[m_dimensions.length];
    	        H5.H5Pget_chunk(propertyListId, chunks.length, chunks);
    	        m_chunkRowSize = chunks[0];
    	    }
		}
		
        H5.H5Pclose(propertyListId);
	}
	
	public void close() {
		// End access to the dataSet and release resources used by it.
        try {
            if (isOpen()) {
        		Iterator<Hdf5Attribute<?>> iter = getAttributes().iterator();
        		while (iter.hasNext()) {
        			iter.next().close();
        		}

                // Terminate access to the dataSpace.
            	H5.H5Sclose(m_dataspaceId);
                m_dataspaceId = -1;
                
                H5.H5Dclose(getElementId());
                setOpen(false);
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be closed", hle);
        }
	}
}

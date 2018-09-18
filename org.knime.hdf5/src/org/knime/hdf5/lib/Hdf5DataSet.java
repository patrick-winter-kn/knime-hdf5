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
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
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
	
	static Hdf5DataSet<?> openDataSet(final Hdf5Group parent, final String name) throws UnsupportedDataTypeException {
		Hdf5DataSet<?> dataSet = null;
		
		try {
			Hdf5DataType type = parent.findDataSetType(name);
			dataSet = getInstance(parent, name, type);
			
	    	parent.addDataSet(dataSet);
	    	dataSet.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be opened: " + npiaise.getMessage(), npiaise);
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
	
	public int getCompressionLevel() {
		return m_compressionLevel;
	}
	
	public long getChunkRowSize() {
		return m_chunkRowSize;
	}
	
	public Hdf5DataType getType() {
		return m_type;
	}
	
	/**
	 * Calculates the number of rows.
	 * 
	 * @return product of the numbers in the dimensions array 
	 */
	public long numberOfRows() {
		return m_dimensions.length > 0 ? m_dimensions[0] : 1;
	}
	
	/**
	 * Calculates the number of multi-dimensional columns.
	 * 
	 * @return product of the numbers in the dimensions array 
	 */
	public long numberOfColumns() {
		return m_dimensions.length > 1 ? numberOfValuesFrom(1) : 1;
	}
	
	/**
	 * Calculates the number of values/cells which can be saved in the dimensions array.
	 * 
	 * @return product of the numbers in the dimensions array 
	 */
	public long numberOfValues() {
		return m_dimensions.length > 0 ? numberOfValuesFrom(0) : 1;
	}
	
	/**
	 * Calculates the number of values/cells which can be saved within the range <br>
	 * { fromIndex, ..., getDimensions().length - 1 } of the dimensions array.
	 * 
	 * @param fromIndex inclusive
	 * @return product of the numbers in the dimensions array in this range 
	 */
	public long numberOfValuesFrom(int fromIndex) {
		return numberOfValuesRange(fromIndex, getDimensions().length);
	}

	/**
	 * Calculates the number of values/cells which can be saved within the range <br>
	 * { fromIndex, ..., toIndex - 1 } of the dimensions array.
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
	
	private long selectChunk(long[] offset, long[] count) throws HDF5Exception, NullPointerException {
		long memSpaceId = HDF5Constants.H5P_DEFAULT;
		
		// check if dataSet is not scalar
		if (m_dimensions.length > 0) {
			memSpaceId = H5.H5Screate_simple(count.length, count, null);
			
			H5.H5Sselect_hyperslab(m_dataspaceId, HDF5Constants.H5S_SELECT_SET,
					offset, null, count, null);
        }
        
        return memSpaceId;
	}
	
	public boolean writeRow(Type[] dataIn, long rowIndex, Rounding rounding) {
		return writeRows(dataIn, rowIndex, rowIndex + 1, rounding);
	}

	private boolean writeRows(Type[] dataIn, long fromRowIndex, long toRowIndex, Rounding rounding) {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		if (m_dimensions.length > 0) {
			offset[0] = fromRowIndex;
			count[0] = toRowIndex - fromRowIndex;
		}
		
    	return write(dataIn, offset, count, rounding);
	}
	
	public boolean writeColumn(Type[] dataIn, long[] colOffset, Rounding rounding) {
		long[] count = new long[m_dimensions.length > 0 ? m_dimensions.length-1 : 0];
		Arrays.fill(count, 1);
		return writeColumns(dataIn, colOffset, count, rounding);
	}
	
	private boolean writeColumns(Type[] dataIn, long[] colOffset, long[] colCount, Rounding rounding) {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		for (int i = 1; i < offset.length; i++) {
			offset[i] = colOffset[i-1];
			count[i] = colCount[i-1];
		}
		
		return write(dataIn, offset, count, rounding);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean write(Type[] dataIn, long[] offset, long[] count, Rounding rounding) {
		try {
	        if (isOpen()) {
	        	long memSpaceId = selectChunk(offset, count);
				
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
	    				int numberOfValues = 1;
	    				for (int i = 0; i < count.length; i++) {
	    					numberOfValues *= count[i];
	    				}
	    				
						Object[] dataWrite = m_type.getHdfType().createArray(numberOfValues);
			            
						Class hdfClass = m_type.getHdfClass();
						Class knimeClass = m_type.getKnimeClass();
						for (int i = 0; i < dataWrite.length; i++) {
							dataWrite[i] = m_type.knimeToHdf(knimeClass, knimeClass.cast(dataIn[i]), hdfClass, rounding);
						}
						
			            H5.H5Dwrite(getElementId(), m_type.getConstants()[1],
			            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataWrite);
					}

					if (memSpaceId != HDF5Constants.H5P_DEFAULT) {
						H5.H5Sclose(memSpaceId);
					}
					
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
	public boolean writeRowToDataSet(DataRow row, int[] specIndices, long rowIndex, Type[] copyValues, Rounding rounding) throws UnsupportedDataTypeException {
		Type[] dataIn = (Type[]) m_type.getKnimeType().createArray((int) numberOfColumns());
		
		int copyIndex = 0;
		for (int i = 0; i < dataIn.length; i++) {
			dataIn[i] = specIndices[i] >= 0 ? getValueFromDataCell(row.getCell(specIndices[i])) : copyValues[copyIndex++];
		}
		
		return writeRow(dataIn, rowIndex, rounding);
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
			} else {
				return (Type) dataCell.toString();
			}
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
		
		if (dataCell instanceof MissingCell) {
			throw new UnsupportedDataTypeException("Cannot find value from MissingCell");
		}
		
		throw new UnsupportedDataTypeException("Unsupported combination of dataCellDataType and knimeDataType while reading from dataCell");
	}
	
	public Type[] readRow(long rowIndex) {
		return readRows(rowIndex, rowIndex + 1);
	}

	private Type[] readRows(long fromRowIndex, long toRowIndex) {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		if (m_dimensions.length > 0) {
			offset[0] = fromRowIndex;
			count[0] = toRowIndex - fromRowIndex;
		}
		
    	return read(offset, count);
	}
	
	public Type[] readColumn(long[] colOffset) {
		long[] colCount = new long[m_dimensions.length > 0 ? m_dimensions.length-1 : 0];
		Arrays.fill(colCount, 1);
		return readColumns(colOffset, colCount);
	}
	
	private Type[] readColumns(long[] colOffset, long[] colCount) {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		for (int i = 1; i < offset.length; i++) {
			offset[i] = colOffset[i-1];
			count[i] = colCount[i-1];
		}
		
		return read(offset, count);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Type[] read(long[] offset, long[] count) throws IllegalStateException {
		Type[] dataOut = null;
		
		try {
			long numberOfValuesLong = 1;
			for (int i = 0; i < count.length; i++) {
				numberOfValuesLong *= count[i];
			}
			
			if (numberOfValuesLong > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("Number of values to read in dataSet \"" + getPathFromFileWithName()
						+ "\" has overflown the Integer values.");
			}
			int numberOfValues = (int) numberOfValuesLong;
			
	        if (isOpen()) {
	        	dataOut = (Type[]) m_type.getKnimeType().createArray(numberOfValues);

	            long memSpaceId = selectChunk(offset, count);
				
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
					Object[] dataRead = m_type.getHdfType().createArray(numberOfValues);
		            H5.H5Dread(getElementId(), m_type.getConstants()[1],
		            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataRead);
		            
					Class hdfClass = m_type.getHdfClass();
					Class knimeClass = m_type.getKnimeClass();
					for (int i = 0; i < dataRead.length; i++) {
						dataOut[i] = (Type) m_type.hdfToKnime(hdfClass, hdfClass.cast(dataRead[i]), knimeClass);
					}
				}
				
				if (memSpaceId != HDF5Constants.H5P_DEFAULT) {
					H5.H5Sclose(memSpaceId);
				}
			} else {
                throw new IllegalStateException("DataSet \"" + getPathFromFileWithName() + "\" is not open: data could not be read");
        	}
	    } catch (HDF5Exception | UnsupportedDataTypeException | NullPointerException | IllegalArgumentException hudtnpiae) {
            NodeLogger.getLogger("HDF5 Files").error(hudtnpiae.getMessage(), hudtnpiae);
        }
		
		return dataOut;
	}
	
	public void extendRow(List<DataCell> row, long rowIndex) throws UnsupportedDataTypeException {
		long rowNum = numberOfRows();
		int colNum = (int) numberOfColumns();
		
		if (rowIndex < rowNum) {
			try {
				Type[] dataRead = readRow(rowIndex);
				
				for (int c = 0; c < colNum; c++) {
					row.add(getDataCellWithValue(dataRead[c]));
				}
			} catch (IllegalStateException ise) {
				NodeLogger.getLogger("HDF5 Files").error(ise.getMessage(), ise);
			}
		} else {
			for (int c = 0; c < colNum; c++) {
				row.add(getDataCellWithValue(null));
			}
		}
	}
	
	private DataCell getDataCellWithValue(Type value) throws UnsupportedDataTypeException {
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
	
	@Override
	public boolean open() {
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
				
				return true;
			}
		} catch (HDF5LibraryException | NullPointerException | IllegalStateException hlnpise) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be opened", hlnpise);
        }
		
		return false;
	}
	
	private void createDimensions(long[] dimensions) {
		m_dimensions = dimensions;
		
		// Create the dataSpace for the dataSet.
        try {
            m_dataspaceId = m_dimensions.length == 0 ? H5.H5Screate(HDF5Constants.H5S_SCALAR)
        			: H5.H5Screate_simple(m_dimensions.length, m_dimensions, null);
            
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
				throw new IllegalArgumentException("chunkRowSize must be > 0 because of compressionLevel > 0");
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
    	m_chunkRowSize = 1;
		
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

	@Override
	public boolean close() {
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
                return true;
            }
        } catch (HDF5LibraryException hle) {
            NodeLogger.getLogger("HDF5 Files").error("DataSet could not be closed", hle);
        }
        
        return false;
	}
}

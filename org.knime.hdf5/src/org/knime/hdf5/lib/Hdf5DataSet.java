package org.knime.hdf5.lib;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.node.NodeLogger;
import org.knime.hdf5.lib.types.Hdf5DataType;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;
import org.knime.hdf5.lib.types.Hdf5KnimeDataType;
import org.knime.hdf5.nodes.writer.edit.EditDataType.Rounding;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5DataspaceInterfaceException;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Representer of an dataSet for HDF5 (accessed through {@code H5D} in the hdf api)
 * and the respective columns in a data table for KNIME.
 * <br>
 * The generic parameter, which can be {@code Integer}, {@code Long}, {@code Double}
 * or {@code String}, represents the type of the dataSet in KNIME.
 */
public class Hdf5DataSet<Type> extends Hdf5TreeElement {

	private long m_dataspaceId = -1;
	
	private long[] m_dimensions;
	
	private int m_compressionLevel;
	
	private long m_chunkRowSize;
	
	private Hdf5DataType m_type;
	
	private Hdf5DataSet(String name, Hdf5DataType type) 
			throws NullPointerException, IllegalArgumentException {
		super(name);
		
		m_type = type;
	}
	
	private static Hdf5DataSet<?> getInstance(Hdf5Group parent, String name, Hdf5DataType type) throws IllegalStateException {
		if (parent == null) {
			throw new IllegalArgumentException("Parent group of dataSet \"" + name + "\" cannot be null");
			
		} else if (!parent.isOpen()) {
			throw new IllegalStateException("Parent group \"" + parent.getPathFromFileWithName() + "\" is not open");
		
		} else if (type == null) {
			throw new IllegalArgumentException("DataType for dataSet \"" + parent.getPathFromFileWithName() + name + "\" cannot be null");
		}
		
		switch (type.getKnimeType()) {
		case INTEGER:
			return new Hdf5DataSet<Integer>(name, type);
		case LONG:
			return new Hdf5DataSet<Long>(name, type);
		case DOUBLE:
			return new Hdf5DataSet<Double>(name, type);
		case STRING:
			return new Hdf5DataSet<String>(name, type);
		default:
			NodeLogger.getLogger(Hdf5DataSet.class).warn("DataSet \""
					+ parent.getPathFromFileWithName() + name + "\" has an unknown dataType");
			return null;
		}
	}
	
	static Hdf5DataSet<?> createDataSet(Hdf5Group parent, String name, long[] dimensions,
			int compressionLevel, long chunkRowSize, Hdf5DataType type) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		
		try {
			dataSet = getInstance(parent, name, type);
			dataSet.createDataspace(dimensions);
			long propertyListId = dataSet.createCompression(compressionLevel, chunkRowSize);
			
			/*
			 * parent.lockReadOpen() is not needed here because write access
			 * for the whole file is already needed for creating the dataSet,
			 * so no other readers or writers can access parent anyway
			 */
	        dataSet.setElementId(H5.H5Dcreate(parent.getElementId(), dataSet.getName(),
            		dataSet.getType().getConstants()[0], dataSet.getDataspaceId(),
                    HDF5Constants.H5P_DEFAULT, propertyListId, HDF5Constants.H5P_DEFAULT));
	        
            H5.H5Pclose(propertyListId);
	        
    		parent.addDataSet(dataSet);
    		
		} catch (HDF5Exception | NullPointerException | IllegalArgumentException | IllegalStateException hnpiaise) {
            throw new IOException("DataSet could not be created: " + hnpiaise.getMessage(), hnpiaise);
        }
		
		return dataSet;
	}
	
	static Hdf5DataSet<?> openDataSet(Hdf5Group parent, String name) throws IOException {
		Hdf5DataSet<?> dataSet = null;
		
		try {
			Hdf5DataType type = parent.findDataSetType(name);
			dataSet = getInstance(parent, name, type);
			
	    	parent.addDataSet(dataSet);
	    	dataSet.open();
        	
        } catch (NullPointerException | IllegalArgumentException | IllegalStateException npiaise) {
        	throw new IOException("DataSet could not be opened: " + npiaise.getMessage(), npiaise);
        }
		
		return dataSet;
	}
	
	/**
	 * Updates the data type of this dataSet.
	 * 
	 * @throws UnsupportedDataTypeException if the data type is unsupported now
	 * @throws IOException if this dataSet does not exist or an internal error
	 * 	occurred
	 * @see Hdf5Group#findDataSetType(String)
	 * @see Hdf5DataSet#loadDataspace()
	 * @see Hdf5DataSet#loadCompression()
	 */
	void updateDataSet() throws UnsupportedDataTypeException, IOException {
		m_type = getParent().findDataSetType(getName());
	}

	private long getDataspaceId() {
		return m_dataspaceId;
	}
	
	/**
	 * @return an array of the sizes for each dimension of the dataSet
	 */
	public long[] getDimensions() {
		return m_dimensions;
	}
	
	/**
	 * @return 0 if no compression is used, otherwise returns the level of
	 * 	compression (from 1 to 9) 
	 * @see <a href=
	 *	"https://support.hdfgroup.org/HDF5/doc/RM/RM_H5P.html#Property-SetDeflate"
	 *	>H5.H5Pset_deflate(long, int)</a>
	 */
	public int getCompressionLevel() {
		return m_compressionLevel;
	}
	
	/**
	 * @return the size of the row chunks in this dataSet
	 * @see <a href=
	 *	"https://support.hdfgroup.org/HDF5/doc/RM/RM_H5P.html#Property-SetChunk"
	 *	>H5.H5Pset_chunk(long, int, long[])</a>
	 */
	public long getChunkRowSize() {
		return m_chunkRowSize;
	}
	
	public Hdf5DataType getType() {
		return m_type;
	}
	
	/**
	 * Returns the number of rows.
	 * 
	 * @return the number in the dimensions array at index 0 or returns 1 if
	 * 	the dataSet is scalar (where the length of the dimensions array is 0)
	 */
	public long numberOfRows() {
		return m_dimensions.length > 0 ? m_dimensions[0] : 1;
	}
	
	/**
	 * Returns the number of multi-dimensional columns.
	 * 
	 * @return product of the numbers in the dimensions array except index 0
	 */
	public long numberOfColumns() {
		return m_dimensions.length > 1 ? numberOfValuesFrom(1) : 1;
	}
	
	/**
	 * Returns the number of values/cells which can be saved in the dimensions array.
	 * 
	 * @return product of the numbers in the dimensions array 
	 */
	public long numberOfValues() {
		return m_dimensions.length > 0 ? numberOfValuesFrom(0) : 1;
	}
	
	/**
	 * Returns the number of values/cells which can be saved within the range
	 * of indices { fromIndex, ..., getDimensions().length - 1 } of the
	 * dimensions array.
	 * 
	 * @param fromIndex inclusive
	 * @return product of the numbers in the dimensions array in this range 
	 */
	public long numberOfValuesFrom(int fromIndex) {
		return numberOfValuesRange(fromIndex, m_dimensions.length);
	}

	/**
	 * Returns the number of values/cells which can be saved within the range
	 * of indices { fromIndex, ..., toIndex - 1 } of the dimensions array.
	 * 
	 * @param fromIndex inclusive
	 * @param toIndex exclusive
	 * @return product of the numbers in the dimensions array in this range
	 * @throws IllegalArgumentException if the input range is not in the range
	 * 	of the dimensions array
	 */
	public long numberOfValuesRange(int fromIndex, int toIndex) throws IllegalArgumentException {
		if (0 <= fromIndex && fromIndex <= toIndex && toIndex <= m_dimensions.length) {
			long values = 1;
			long[] dims = Arrays.copyOfRange(m_dimensions, fromIndex, toIndex);
			
			for (long l: dims) {
				values *= l;
			}
			
			return values;
		}
		
		throw new IllegalArgumentException("Hdf5DataSet.numberOfValuesRange(" + fromIndex + ", " + toIndex
				+ "): is out of range (0, " + m_dimensions.length + ")");
	}
	
	/**
	 * This method can be used to iterate through all columns of the dataSet.
	 * <br>
	 * <br>
	 * Sets {@code colIndices} to the indices of the next column, i.e. increments
	 * {@code colIndices} by incrementing the last index that does not overflow
	 * the respective size of the column.
	 * 
	 * @param {@code colIndices} the dimensions array of all columns
	 * @return if {@code colIndices} incremented successfully and didn't go from
	 *  max to min, i.e. colIndices does not
	 * @throws IllegalArgumentException
	 */
	public boolean nextColumnIndices(long[] colIndices) throws IllegalArgumentException {
		if (colIndices.length != m_dimensions.length - 1) {
			throw new IllegalArgumentException("Hdf5DataSet.nextColumnIndices(colIndices): colDims must have the length "
					+ (m_dimensions.length - 1) + ", but has the length " + colIndices.length + ")");
		}
		
		int i = colIndices.length - 1;
		boolean transfer = true;
		
		while (transfer && i >= 0) {
			colIndices[i] = (colIndices[i] + 1) % m_dimensions[i+1];
			transfer = colIndices[i] == 0;
			i--;
		}
		
		return !transfer;
	}
	
	/**
	 * Selects a chunk within the dataSet and returns its memory space id.
	 * 
	 * @param offset the indices of the first cell in the dataSet from where
	 * 	the chunk should be selected
	 * @param count the number of values to select for each dimension
	 * @return the memory id of the selected chunk
	 * @throws HDF5Exception if {@code offset} or {@code count} cannot be fit
	 * 	in the dimensions array or an internal error occurred
	 */
	private long selectChunk(long[] offset, long[] count) throws HDF5Exception {
		long memSpaceId = HDF5Constants.H5P_DEFAULT;
		
		// check if dataSet is not scalar
		if (m_dimensions.length > 0) {
			memSpaceId = H5.H5Screate_simple(count.length, count, null);
			
			H5.H5Sselect_hyperslab(m_dataspaceId, HDF5Constants.H5S_SELECT_SET,
					offset, null, count, null);
        }
        
        return memSpaceId;
	}
	
	/**
	 * Closes the memory space used for the chunk.
	 * 
	 * @param memSpaceId the id of the memory space
	 * @return success of closing the memory space
	 */
	private boolean unselectChunk(long memSpaceId) {
		if (memSpaceId >= 0 && memSpaceId != HDF5Constants.H5P_DEFAULT) {
			try {
				H5.H5Sclose(memSpaceId);
				return true;
				
			} catch (HDF5LibraryException hle) {
				NodeLogger.getLogger(getClass()).warn("Memory space of dataSet \"" + getPathFromFileWithName()
						+ "\" could not be closed: " + hle.getMessage(), hle);
			}
		}
		
		return false;
	}
	
	/**
	 * Writes the knime data to the row using the rounding if there is a cast
	 * from float to int when converting from knime to hdf. If this dataSet
	 * has more than 2 dimensions, the one-dimensional data array {@code dataIn}
	 * gets mapped to the dimensions using the principle of
	 * {@linkplain Hdf5DataSet#nextColumnIndices(long[])}.
	 * 
	 * @param dataIn the knime data that should be written in the row
	 * @param rowIndex the index of the row that should be written
	 * @param rounding the rounding for a cast from float to int
	 * @return if the knime data was successfully written
	 * @throws IOException if an error occurred in the hdf library
	 * @throws HDF5DataspaceInterfaceException if the row index is out of range
	 */
	public boolean writeRow(Type[] dataIn, long rowIndex, Rounding rounding) throws IOException, HDF5DataspaceInterfaceException {
		return writeRows(dataIn, rowIndex, rowIndex + 1, rounding);
	}

	private boolean writeRows(Type[] dataIn, long fromRowIndex, long toRowIndex, Rounding rounding) throws IOException, HDF5DataspaceInterfaceException {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		if (m_dimensions.length > 0) {
			offset[0] = fromRowIndex;
			count[0] = toRowIndex - fromRowIndex;
		}
		
    	return write(dataIn, offset, count, rounding);
	}

	/**
	 * Writes the knime data to the column of this dataSet using the rounding
	 * if there is a cast from float to int when converting from knime to hdf.
	 * 
	 * @param dataIn the knime data that should be written in the column
	 * @param colOffset the indices of the column
	 * @param rounding the rounding for a cast from float to int
	 * @return if the knime data was successfully written
	 * @throws IOException if an error occurred in the hdf library
	 * @throws HDF5DataspaceInterfaceException if the column offset is out of range
	 */
	public boolean writeColumn(Type[] dataIn, long[] colOffset, Rounding rounding) throws IOException, HDF5DataspaceInterfaceException {
		long[] count = new long[m_dimensions.length > 0 ? m_dimensions.length-1 : 0];
		Arrays.fill(count, 1);
		return writeColumns(dataIn, colOffset, count, rounding);
	}
	
	private boolean writeColumns(Type[] dataIn, long[] colOffset, long[] colCount, Rounding rounding) throws IOException, HDF5DataspaceInterfaceException {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		for (int i = 1; i < offset.length; i++) {
			offset[i] = colOffset[i-1];
			count[i] = colCount[i-1];
		}
		
		return write(dataIn, offset, count, rounding);
	}
	
	/**
	 * Writes the knime data to this dataSet using the rounding if there is a cast
	 * from float to int when converting from knime to hdf.
	 * 
	 * @param dataIn the knime data that should be written
	 * @param offset the indices of the first cell in the dataSet that should
	 * 	be written
	 * @param count the number of values to select for each dimension
	 * @param rounding the rounding for a cast from float to int
	 * @return if the knime data was successfully written
	 * @throws IOException if an error occurred in the hdf library
	 * @throws HDF5DataspaceInterfaceException if the offset or count is out of range
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean write(Type[] dataIn, long[] offset, long[] count, Rounding rounding) throws IOException, HDF5DataspaceInterfaceException {
		Object[] dataWrite = Arrays.copyOf(dataIn, dataIn.length);
		
		if (!m_type.isHdfType(HdfDataType.STRING) && !m_type.hdfTypeEqualsKnimeType()) {
            int numberOfValues = 1;
			for (int i = 0; i < count.length; i++) {
				numberOfValues *= count[i];
			}
			
			dataWrite = m_type.getHdfType().createArray(numberOfValues);
            
			Class knimeClass = m_type.getKnimeClass();
			Class hdfClass = m_type.getHdfClass();
			for (int i = 0; i < dataWrite.length; i++) {
				dataWrite[i] = m_type.knimeToHdf(knimeClass, knimeClass.cast(dataIn[i]), hdfClass, rounding);
			}
		}
		
		return writeHdf(dataWrite, offset, count);
	}
		
	private boolean writeHdf(Object[] dataWrite, long[] offset, long[] count) throws IOException, HDF5DataspaceInterfaceException {
    	long memSpaceId = -1;
    	try {
    		lockReadOpen();
    		checkOpen();
    		checkChunkSelection(offset, count);
    	
    		memSpaceId = selectChunk(offset, count);
		
			if (m_type.isHdfType(HdfDataType.STRING)) {
				H5.H5Dwrite_string(getElementId(), m_type.getConstants()[1],
						memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, (String[]) dataWrite);
				
			} else {
				H5.H5Dwrite(getElementId(), m_type.getConstants()[1],
	            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataWrite);
			}
			
			return true;
    			
	    } catch (HDF5DataspaceInterfaceException hdie) {
	    	throw hdie;
	    	
	    } catch (HDF5Exception | IOException | NullPointerException hnpe) {
	    	throw new IOException("DataSet \"" + getPathFromFileWithName()
	    			+ "\" could not be written: " + hnpe.getMessage(), hnpe);
	    	
	    } finally {
	    	unlockReadOpen();
			unselectChunk(memSpaceId);
	    }
	}
	
	/**
	 * Writes the values of a knime data row and many hdf dataSets to a row.
	 * {@code dataRowColumnIndices} and {@code dataSetColumnIndices} are used
	 * to specify the input cells those values should be written in this
	 * dataSet.
	 * 
	 * @param rowIndex the index of the row to be written in this dataSet
	 * @param dataRow the knime data row with the values to be written
	 * @param dataRowColumnIndices the indices of the columns in the {@code dataRow}
	 * 	those values should be written in the respective columns of this dataSet
	 * 	or -1 if a dataSet of {@code dataSets} should be used
	 * @param dataSets the dataSets that should be used to write in the respective
	 * 	columns of this dataSet
	 * @param dataSetColumnIndices the indices of the columns in the {@code dataSets}
	 * 	those values should be written in the respective columns of this dataSet
	 * @param standardValue the standard value for missing values in the {@code dataRow}
	 * @param rounding the rounding for a cast from float to int
	 * @return if the data was written successfully to this dataSet
	 * @throws IOException if any dataSet is not open or an internal error occurred
	 * @throws HDF5DataspaceInterfaceException if the row index or any column index
	 * 	is out of range
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean copyValuesToRow(long rowIndex, DataRow dataRow, int[] dataRowColumnIndices, Hdf5DataSet<?>[] dataSets,
			long[] dataSetColumnIndices, Type standardValue, Rounding rounding) throws IOException, HDF5DataspaceInterfaceException {
		Object[] dataWrite = m_type.getHdfType().createArray((int) numberOfColumns());
		Class outputClass = m_type.getHdfClass();
		int dataSetIndex = 0;
		
		for (int i = 0; i < dataWrite.length; i++) {
			if (dataRowColumnIndices[i] >= 0) {
				Type value = (Type) m_type.getKnimeType().getValueFromDataCell(dataRow.getCell(dataRowColumnIndices[i]));
				value = value == null ? standardValue : value;
				
				Class knimeClass = m_type.getKnimeClass();
				dataWrite[i] = m_type.knimeToHdf(knimeClass, knimeClass.cast(value), outputClass, rounding);
				
			} else {
				Hdf5DataSet<?> dataSet = dataSets[dataSetIndex];
				long columnIndex = dataSetColumnIndices[dataSetIndex];
				
				long[] dims = dataSet.getDimensions();
				long[] offset = new long[dims.length];
				long[] count = new long[dims.length];
				if (dims.length > 0) {
					offset[0] = rowIndex;
					for (int dim = offset.length-1; dim >= 2; i--) {
						offset[i] = columnIndex % dims[i];
						columnIndex = columnIndex / dims[i];
					}
					offset[1] = columnIndex;
					Arrays.fill(count, 1);
				}

				Object value = dataSet.readHdf(offset, count)[0];

	            Hdf5DataType type = dataSet.getType();
				Class inputClass = type.getHdfClass();
				dataWrite[i] = type.hdfToHdf(inputClass, inputClass.cast(value), outputClass, m_type, rounding);
				dataSetIndex++;
			}
		}
		
		long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		if (m_dimensions.length > 0) {
			offset[0] = rowIndex;
			count[0] = 1;
		}
		
		return writeHdf(dataWrite, offset, count);
	}
	
	/**
	 * Sets the data of this dataSet back to the standard value which is 0
	 * for numbers and the empty String for Strings.
	 * 
	 * @return if the data was successfully reset
	 * @throws IOException if an error occurred in the hdf library while writing
	 */
	@SuppressWarnings("unchecked")
	public boolean clearData() throws IOException {
		boolean success = false;
		try {
			success = true;
			Type[] dataIn = (Type[]) m_type.getKnimeType().createArray((int) numberOfColumns());
			Arrays.fill(dataIn, (Type) m_type.getKnimeType().getStandardValue());
			for (long i = 0; i < numberOfRows(); i++) {
				success &= writeRow(dataIn, i, Rounding.DOWN);
			}
		} catch (HDF5DataspaceInterfaceException | UnsupportedDataTypeException hdiudte) {
			NodeLogger.getLogger(getClass()).error(hdiudte.getMessage(), hdiudte);
		}
		
		return success;
	}
	
	/**
	 * Reads the data of the row with the input index in this dataSet.
	 * Returns the knime data after converting from hdf to knime. If this dataSet
	 * has more than 2 dimensions, the one-dimensional output data array
	 * gets mapped to the dimensions using the principle of
	 * {@linkplain Hdf5DataSet#nextColumnIndices(long[])}.
	 * 
	 * @param rowIndex the index of the row that should be read
	 * @return the knime output data
	 * @throws IOException if an error occurred in the hdf library
	 * @throws HDF5DataspaceInterfaceException if the row index is out of range
	 */
	public Type[] readRow(long rowIndex) throws IOException, HDF5DataspaceInterfaceException {
		return readRows(rowIndex, rowIndex + 1);
	}

	private Type[] readRows(long fromRowIndex, long toRowIndex) throws IOException, HDF5DataspaceInterfaceException {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		if (m_dimensions.length > 0) {
			offset[0] = fromRowIndex;
			count[0] = toRowIndex - fromRowIndex;
		}
		
    	return read(offset, count);
	}

	/**
	 * Reads the data of the column with the input column offset in this dataSet.
	 * Returns the knime data after converting from hdf to knime.
	 * 
	 * @param colOffset the indices of the column that should be read
	 * @return the knime output data
	 * @throws IOException if an error occurred in the hdf library
	 * @throws HDF5DataspaceInterfaceException if the column offset is out of range
	 */
	public Type[] readColumn(long[] colOffset) throws IOException, HDF5DataspaceInterfaceException {
		long[] colCount = new long[m_dimensions.length > 0 ? m_dimensions.length-1 : 0];
		Arrays.fill(colCount, 1);
		return readColumns(colOffset, colCount);
	}
	
	private Type[] readColumns(long[] colOffset, long[] colCount) throws IOException, HDF5DataspaceInterfaceException {
    	long[] offset = new long[m_dimensions.length];
		long[] count = m_dimensions.clone();
		for (int i = 1; i < offset.length; i++) {
			offset[i] = colOffset[i-1];
			count[i] = colCount[i-1];
		}
		
		return read(offset, count);
	}


	/**
	 * @param offset the indices of the first cell in the dataSet that should
	 * 	be read
	 * @param count the number of values to select for each dimension
	 * @return the knime output data
	 * @throws IOException if an error occurred in the hdf library
	 * @throws HDF5DataspaceInterfaceException if the offset or count is out of range
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Type[] read(long[] offset, long[] count) throws IOException, HDF5DataspaceInterfaceException {
		Type[] dataOut = null;
		
		if (!m_type.isHdfType(HdfDataType.STRING) && !m_type.hdfTypeEqualsKnimeType()) {
			Object[] dataRead = readHdf(offset, count);
			dataOut = (Type[]) m_type.getKnimeType().createArray(dataRead.length);
			
            Class hdfClass = m_type.getHdfClass();
			Class knimeClass = m_type.getKnimeClass();
			for (int i = 0; i < dataRead.length; i++) {
				dataOut[i] = (Type) m_type.hdfToKnime(hdfClass, hdfClass.cast(dataRead[i]), knimeClass);
			}
		} else {
			dataOut = (Type[]) readHdf(offset, count);
		}
		
		return dataOut;
	}
		
	private Object[] readHdf(long[] offset, long[] count) throws IOException, HDF5DataspaceInterfaceException {
        long memSpaceId = -1;
		try {
			lockReadOpen();
			checkOpen();
			int numberOfValues = checkChunkSelection(offset, count);
		
            memSpaceId = selectChunk(offset, count);

			Object[] dataRead = m_type.getHdfType().createArray(numberOfValues);
			if (m_type.isHdfType(HdfDataType.STRING)) {
                if (m_type.isVlen()) {
					long typeId = H5.H5Tget_native_type(m_type.getConstants()[0]);
                    H5.H5DreadVL(getElementId(), typeId,
                    		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataRead);
    				H5.H5Tclose(typeId);
                    
                } else {
					H5.H5Dread_string(getElementId(), m_type.getConstants()[1],
							memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, (String[]) dataRead);
				}
			} else {
	            H5.H5Dread(getElementId(), m_type.getConstants()[1],
	            		memSpaceId, m_dataspaceId, HDF5Constants.H5P_DEFAULT, dataRead);
			}
			
			return dataRead;
			
	    } catch (HDF5DataspaceInterfaceException hdie) {
	    	throw hdie;
	    	
	    } catch (HDF5Exception | IOException | NullPointerException hionpe) {
            throw new IOException("DataSet \"" + getPathFromFileWithName()
					+ "\" could not be read: " + hionpe.getMessage(), hionpe);
            
        } finally {
        	unlockReadOpen();
			unselectChunk(memSpaceId);
        }
	}
	
	/**
	 * @param offset the indices of the first cell in the dataSet from where
	 * 	the chunk should be selected
	 * @param count the number of values to select for each dimension
	 * @return the number of values contained in {@code count}
	 * @throws HDF5DataspaceInterfaceException if the selection of the data
	 * 	space is out of range
	 */
	private int checkChunkSelection(long[] offset, long[] count) throws HDF5DataspaceInterfaceException {
		if (m_dimensions.length != offset.length || offset.length != count.length) {
			throw new HDF5DataspaceInterfaceException("Offset or count has wrong number of dimensions");
		}
		
		long numberOfValues = 1;
		for (int i = 0; i < m_dimensions.length; i++) {
			if (offset[i] < 0) {
				throw new HDF5DataspaceInterfaceException("Cannot select a negative index of dimension " + i);
			} else if (count[i] < 0) {
				throw new HDF5DataspaceInterfaceException("Cannot select a negative number of dimension " + i);
			} else if (offset[i] + count[i] > m_dimensions[i]) {
				throw new HDF5DataspaceInterfaceException("Maximum selected index of dimension " + i + " (size: " + m_dimensions[i] + ") is out of bounds: " + (offset[i] + count[i] - 1));
			}
			numberOfValues *= count[i];
		}

		if (numberOfValues > Integer.MAX_VALUE) {
			throw new HDF5DataspaceInterfaceException("Number of values to read/write in dataSet \"" + getPathFromFileWithName()
					+ "\" has overflown the Integer values.");
		}
		
		return (int) numberOfValues;
	}
	
	/**
	 * Extends the input row by the whole row (with the input row index)
	 * of this dataSet.
	 * 
	 * @param row the row to be extended
	 * @param rowIndex the index of the row in this dataSet as source for the
	 * 	extension
	 * @throws IOException if this dataSet does not exist or an internal error
	 * 	occurred
	 * @throws HDF5DataspaceInterfaceException if the row index is out of range
	 */
	public void extendRow(List<DataCell> row, long rowIndex) throws IOException, HDF5DataspaceInterfaceException {
		Hdf5KnimeDataType knimeType = m_type.getKnimeType();
		long rowNum = numberOfRows();
		int colNum = (int) numberOfColumns();
		
		String missingValueMessage = "(null) on joining hdf dataSets";
		if (rowIndex < rowNum) {
			Type[] dataRead = readRow(rowIndex);
			
			for (int c = 0; c < colNum; c++) {
				// add DataCell with the read value to the row
				row.add(knimeType.getDataCellWithValue(dataRead[c], missingValueMessage));
			}
		} else {
			for (int c = 0; c < colNum; c++) {
				// add MissingCell to the row
				row.add(knimeType.getDataCellWithValue(null, missingValueMessage));
			}
		}
	}
	
	@Override
	public boolean open() throws IOException {
		try {
			lockWriteOpen();
			
			if (!isOpen()) {
				if (!getParent().isOpen()) {
					getParent().open();
				}
				
				checkExists();
				
				setElementId(H5.H5Dopen(getParent().getElementId(), getName(),
						HDF5Constants.H5P_DEFAULT));
				
				loadDataspace();
				loadCompression();
			}
			
			return true;
			
		} catch (HDF5LibraryException | IOException | NullPointerException hlionpe) {
			throw new IOException("DataSet could not be opened: " + hlionpe.getMessage(), hlionpe);
			
        } finally {
        	unlockWriteOpen();
        }
	}
	
	/**
	 * Creates the data space with the size of {@code dimensions} for this dataSet.
	 * 
	 * @param dimensions the dimensions for this dataSet
	 * @throws IOException if an error occurred in the hdf library
	 */
	private void createDataspace(long[] dimensions) throws IOException {
		m_dimensions = dimensions;
		
		// Create the dataSpace for this dataSet.
        try {
            m_dataspaceId = m_dimensions.length == 0 ? H5.H5Screate(HDF5Constants.H5S_SCALAR)
        			: H5.H5Screate_simple(m_dimensions.length, m_dimensions, null);
            
        } catch (HDF5Exception | NullPointerException hnpe) {
            throw new IOException("DataSpace could not be created: " + hnpe.getMessage(), hnpe);
        }
	}
	
	/**
	 * @param compressionLevel the compression level (from 0 to 9) for this dataSet
	 * @param chunkRowSize the size of the row chunks to store this dataSet
	 * 	(may not be larger than the number of rows or 2^32-1,
	 * 	may not be 0 if compression is used)
	 * @return id of the list containing those properties
	 * @throws IOException if an error occurred in the hdf library
	 * @throws IllegalArgumentException if {@code chunkRowSize} is 0 although
	 * 	compression is used
	 * @see <a href=
	 *	"https://support.hdfgroup.org/HDF5/doc/RM/RM_H5P.html#Property-SetDeflate"
	 *	>H5.H5Pset_deflate(long, int)</a>
	 * @see <a href=
	 *	"https://support.hdfgroup.org/HDF5/doc/RM/RM_H5P.html#Property-SetChunk"
	 *	>H5.H5Pset_chunk(long, int, long[])</a>
	 */
	private long createCompression(int compressionLevel, long chunkRowSize) throws IOException, IllegalArgumentException {
		long propertyListId = HDF5Constants.H5P_DEFAULT;
		
		if (compressionLevel > 0) {
			if (chunkRowSize > 0) {
					try {
					propertyListId = H5.H5Pcreate(HDF5Constants.H5P_DATASET_CREATE);
	                H5.H5Pset_layout(propertyListId, HDF5Constants.H5D_CHUNKED);
	                
					long[] chunks = new long[m_dimensions.length];
					Arrays.fill(chunks, 1);
					chunks[0] = chunkRowSize;
	                H5.H5Pset_chunk(propertyListId, chunks.length, chunks);
	                H5.H5Pset_deflate(propertyListId, compressionLevel);
	                
	    			m_compressionLevel = compressionLevel;
	    			m_chunkRowSize = chunkRowSize;
	    			
				} catch (HDF5Exception | NullPointerException hlnpe) {
		            throw new IOException("Chunking could not be created: " + hlnpe.getMessage(), hlnpe);
		        }
                
			} else {
				throw new IllegalArgumentException("chunkRowSize must be > 0 because of compressionLevel > 0");
			}
		}
		
        return propertyListId;
	}
	
	/**
	 * Loads the data space and updates the dimensions array.
	 * 
	 * @throws IOException if an error occurred in the hdf library
	 */
	private void loadDataspace() throws IOException {
		try {
			m_dataspaceId = H5.H5Dget_space(getElementId());
			
			int ndims = H5.H5Sget_simple_extent_ndims(m_dataspaceId);
			long[] dimensions = new long[ndims];
			H5.H5Sget_simple_extent_dims(m_dataspaceId, dimensions, null);
			
			m_dimensions = dimensions;
			
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            throw new IOException("Dimensions could not be loaded: " + hlnpe.getMessage(), hlnpe);
        }
	}
	
	/**
	 * Loads the compression level and the size of the row chunks.
	 * 
	 * @throws IOException if an error occurred in the hdf library
	 */
	private void loadCompression() throws IOException {
		try {
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
	        
		} catch (HDF5LibraryException | NullPointerException hlnpe) {
            throw new IOException("Chunking could not be loaded: " + hlnpe.getMessage(), hlnpe);
        }
	}

	@Override
	public boolean close() throws IOException {
		// End access to the dataSet and release resources used by it.
        try {
        	lockWriteOpen();
        	checkExists();
        	
        	boolean success = true;
        	
            if (isOpen()) {
        		for (Hdf5Attribute<?> attr : getAttributes()) {
        			success &= attr.close();
        		}

        		if (success) {
                    // Terminate access to the dataSpace.
                	success &= H5.H5Sclose(m_dataspaceId) >= 0;
                    m_dataspaceId = -1;
        		}
                
        		success &= H5.H5Dclose(getElementId()) >= 0;
	    		if (success) {
	    			setElementId(-1);
	    		}
            }
            
            return true;
            
        } catch (HDF5LibraryException | IOException hlioe) {
            throw new IOException("DataSet could not be closed: " + hlioe.getMessage(), hlioe);
            
        } finally {
        	unlockWriteOpen();
        }
	}
	
	@Override
	public String toString() {
		return "{ name=" + getName() + ",pathFromFile=" + getPathFromFile() + ",open=" + isOpen()
				+ ",dimensions=" + Arrays.toString(m_dimensions) + ",type=" + m_type
				+ ",compressionLevel=" + m_compressionLevel + ",chunkRowSize=" + m_chunkRowSize + " }";
	}
}

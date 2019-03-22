package org.knime.hdf5.lib.types;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

/**
 * Enum which stores the 4 data types ({@code int, long, double, String})
 * which are supported for data tables or flow variables on knime side.
 * This enum also provides several methods for case distinctions between
 * those data types.
 */
public enum Hdf5KnimeDataType {
	/** equivalent to INT32 in hdf */
	INTEGER,
	/** equivalent to INT64 in hdf */
	LONG,
	/** equivalent to FLOAT64 in hdf */
	DOUBLE,
	/** supports any other dataType through .toString() */
	STRING,
	/** not supported dataTypes */
	UNKNOWN;

	/**
	 * Returns the nearest convertible type from the input type.
	 * Only dataSets can be converted to {@code LONG}.
	 * 
	 * @param type the hdf type
	 * @param fromDS if the {@linkplain HdfDataType} is from a dataSet
	 * @return the equivalent knime type
	 */
	public static Hdf5KnimeDataType getKnimeDataType(HdfDataType type, boolean fromDS) {
		switch (type) {
		case INT8:
		case UINT8:
		case INT16:
		case UINT16:
		case INT32:
			return Hdf5KnimeDataType.INTEGER;
		case UINT32:
		case INT64:
			if (fromDS) {
				return Hdf5KnimeDataType.LONG;
			}
		case UINT64:
		case FLOAT32:
		case FLOAT64:
			return Hdf5KnimeDataType.DOUBLE;
		case STRING:
			return Hdf5KnimeDataType.STRING;
		default:
			return Hdf5KnimeDataType.UNKNOWN;
		}
	}
	
	/**
	 * @param type data type of a column spec
	 * @return the equivalent knime type
	 */
	public static Hdf5KnimeDataType getKnimeDataType(DataType type) {
		if (type != null) {
			if (type.equals(IntCell.TYPE)) {	
				return INTEGER;
			} else if (type.equals(LongCell.TYPE)) {	
				return LONG;
			} else if (type.equals(DoubleCell.TYPE)) {	
				return DOUBLE;
			} else {	
				return STRING;
			}
		}
		return null;
	}
	
	/**
	 * @param type data type of a flow variable
	 * @return the equivalent knime type
	 */
	public static Hdf5KnimeDataType getKnimeDataType(Type type) {
		switch (type) {
		case INTEGER:
			return INTEGER;
		case DOUBLE:
			return DOUBLE;
		default:
			return STRING;
		}
	}
	
	/**
	 * Returns the equivalent knime type for the data type of the
	 * components in the input array.
	 * 
	 * @param values an array of objects
	 * @return the equivalent knime type
	 */
	public static Hdf5KnimeDataType getKnimeDataType(Object[] values) {
		Class<?> type = values.getClass().getComponentType();
		if (type == Integer.class) {
			return INTEGER;
		} else if (type == Long.class) {
			return LONG;
		} else if (type == Double.class) {
			return DOUBLE;
		} else {
			return STRING;
		}
	}
	
	/**
	 * @return the data type of a column spec for this type
	 * @throws UnsupportedDataTypeException	if this type is unknown
	 */
	public DataType getColumnDataType() throws UnsupportedDataTypeException {
		switch (this) {
		case INTEGER:
			return IntCell.TYPE;
		case LONG:
			return LongCell.TYPE;
		case DOUBLE:
			return DoubleCell.TYPE;
		case STRING:
			return StringCell.TYPE;
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}

	/**
	 * Returns the value of the input data cell in the data type of this type.
	 * If the data cell is a {@linkplain MissingCell}, the returned value is {@code null}.
	 * 
	 * @param dataCell the cell of a data table
	 * @return the value of {@code dataCell} in the data type of this type
	 * @throws UnsupportedDataTypeException if the data type of the input data cell
	 * 	is a String and the data type of this type is not a String
	 */
	public Object getValueFromDataCell(DataCell dataCell) throws UnsupportedDataTypeException {
		if (dataCell instanceof MissingCell) {
			return null;
		}
		
		DataType type = dataCell.getType();
		switch (this) {
		case INTEGER:
			if (type.equals(IntCell.TYPE)) {
				return (Integer) ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {
				return (Integer) (int) ((LongCell) dataCell).getLongValue();
			} else if (type.equals(DoubleCell.TYPE)) {
				return (Integer) (int) ((DoubleCell) dataCell).getDoubleValue();
			}
			break;
		case LONG:
			if (type.equals(IntCell.TYPE)) {
				return (Long) (long) ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {
				return (Long) ((LongCell) dataCell).getLongValue();
			} else if (type.equals(DoubleCell.TYPE)) {
				return (Long) (long) ((DoubleCell) dataCell).getDoubleValue();
			}
			break;
		case DOUBLE:
			if (type.equals(IntCell.TYPE)) {
				return (Double) (double) ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {
				return (Double) (double) ((LongCell) dataCell).getLongValue();
			} else if (type.equals(DoubleCell.TYPE)) {
				return (Double) ((DoubleCell) dataCell).getDoubleValue();
			}
			break;
		case STRING:
			if (type.equals(IntCell.TYPE)) {
				return "" + ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {
				return "" + ((LongCell) dataCell).getLongValue();
			} else if (type.equals(DoubleCell.TYPE)) {
				return "" + ((DoubleCell) dataCell).getDoubleValue();
			} else if (type.equals(StringCell.TYPE)) {
				return ((StringCell) dataCell).getStringValue();
			} else {
				return dataCell.toString();
			}
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
		
		throw new UnsupportedDataTypeException("Unsupported combination of dataCellDataType and knimeDataType while reading from dataCell");
	}

	/**
	 * Returns the {@linkplain DataCell} of this type which stores the input value.
	 * If the value is {@code null}, the {@linkplain DataCell} is a {@linkplain MissingCell}.
	 * 
	 * @param value the value with should be stored in the {@linkplain DataCell}
	 * @param missingValueMessage the message in case the value is {@code null}
	 * @return the {@linkplain DataCell} of this type which stores the input value
	 * @throws UnsupportedDataTypeException if this type is unknown
	 */
	public DataCell getDataCellWithValue(Object value, String missingValueMessage) throws UnsupportedDataTypeException {
		if (value == null) {
			return new MissingCell(missingValueMessage);
		}
		
		switch (this) {
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

	/**
	 * @param length the length of the array
	 * @return the array of this type with the input length
	 * @throws UnsupportedDataTypeException if this type is unknown
	 */
	public Object[] createArray(int length) throws UnsupportedDataTypeException {
		switch (this) {
		case INTEGER:
			return new Integer[length];
		case LONG:
			return new Long[length];
		case DOUBLE:
			return new Double[length];
		case STRING:
			return new String[length];
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	/**
	 * @return the standard value of a missing value for this type
	 * @throws UnsupportedDataTypeException if this type is unknown
	 */
	public Object getStandardValue() throws UnsupportedDataTypeException {
		switch (this) {
		case INTEGER:
			return 0;
		case LONG:
			return 0L;
		case DOUBLE:
			return 0.0;
		case STRING:
			return "";
		default:
			throw new UnsupportedDataTypeException("Unknown knimeDataType");
		}
	}
	
	/**
	 * @return the equivalent hdf type for this knime type
	 */
	public HdfDataType getEquivalentHdfType() {
		switch (this) {
		case INTEGER:
			return HdfDataType.INT32;
		case LONG:
			return HdfDataType.INT64;
		case DOUBLE:
			return HdfDataType.FLOAT64;
		case STRING:
			return HdfDataType.STRING;
		default:
			return null;
		}
	}
	
	@Override
	public String toString() {
		switch (this) {
		case INTEGER:
			return "Number (integer)";
		case LONG:
			return "Number (long)";
		case DOUBLE:
			return "Number (double)";
		case STRING:
			return "String";
		default:
			return "Unknown knimeDataType";
		}
	}
}
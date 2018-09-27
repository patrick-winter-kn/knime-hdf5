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

public enum Hdf5KnimeDataType {
	INTEGER,		// equivalent to INT32 in HDF
	LONG,			// equivalent to INT64 in HDF
	DOUBLE,			// equivalent to FLOAT64 in HDF
	STRING,			// supports any other dataType through .toString()
	UNKNOWN;		// unknown dataType for cases it should not be supported

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
			} 
			break;
		case LONG:
			if (type.equals(IntCell.TYPE)) {
				return (Long) (long) ((IntCell) dataCell).getIntValue();
			} else if (type.equals(LongCell.TYPE)) {
				return (Long) ((LongCell) dataCell).getLongValue();
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

	public DataCell getDataCellWithValue(Object value) throws UnsupportedDataTypeException {
		if (value == null) {
			return new MissingCell("(null) on joining hdf dataSets");
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
	
	public Object getMissingValue() throws UnsupportedDataTypeException {
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
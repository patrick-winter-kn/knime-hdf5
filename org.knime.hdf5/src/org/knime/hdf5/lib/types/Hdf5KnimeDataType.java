package org.knime.hdf5.lib.types;

import java.util.ArrayList;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.hdf5.lib.types.Hdf5HdfDataType.HdfDataType;

public enum Hdf5KnimeDataType {
	UNKNOWN,		// data type is unknown
	INTEGER,		// data type is an Integer
	LONG,			// data type is a Long
	DOUBLE,			// data type is a Double
	STRING;			// data type is a String

	public static Hdf5KnimeDataType getKnimeDataType(DataType type) throws UnsupportedDataTypeException {
		if (type != null) {
			if (type.equals(IntCell.TYPE)) {	
				return INTEGER;
			} else if (type.equals(LongCell.TYPE)) {	
				return LONG;
			} else if (type.equals(DoubleCell.TYPE)) {	
				return DOUBLE;
			} else if (type.equals(StringCell.TYPE)) {	
				return STRING;
			}
		}
		throw new UnsupportedDataTypeException("Unknown knimeDataType");
	}
	
	public static Hdf5KnimeDataType getKnimeDataType(Type type)/* throws UnsupportedDataTypeException*/ {
		switch (type) {
		case INTEGER:
			return INTEGER;
		case DOUBLE:
			return DOUBLE;
		case STRING:
			return STRING;
		default:
			// TODO see if useless
			return null;
			//throw new UnsupportedDataTypeException("Unknown dataType of flowVariable");
		}
	}
	
	public static Hdf5KnimeDataType getKnimeDataType(Object[] values)/* throws UnsupportedDataTypeException*/ {
		Class<?> type = values.getClass().getComponentType();
		if (type == Integer.class) {
			return INTEGER;
		} else if (type == Long.class) {
			return LONG;
		} else if (type == Double.class) {
			return DOUBLE;
		} else if (type == String.class) {
			return STRING;
		}
		return null;
		//throw new UnsupportedDataTypeException("Unknown knimeDataType");
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

	public Object createArray(int length) throws UnsupportedDataTypeException {
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
	
	public HdfDataType getEquivalentHdfType() {
		switch (this) {
		case INTEGER:
			return HdfDataType.INTEGER;
		case LONG:
			return HdfDataType.LONG;
		case DOUBLE:
			return HdfDataType.DOUBLE;
		case STRING:
			return HdfDataType.STRING;
		default:
			return null;
		}
	}
	
	public List<HdfDataType> getConvertibleHdfTypes() {
		List<HdfDataType> types = new ArrayList<>();
		
		// TODO add more possible types
		switch (this) {
		case INTEGER:
			types.add(HdfDataType.INTEGER);
		case LONG:
			types.add(HdfDataType.LONG);
		case DOUBLE:
			types.add(HdfDataType.DOUBLE);
		case STRING:
			types.add(HdfDataType.STRING);
		case UNKNOWN:
			break;
		}
		
		return types;
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
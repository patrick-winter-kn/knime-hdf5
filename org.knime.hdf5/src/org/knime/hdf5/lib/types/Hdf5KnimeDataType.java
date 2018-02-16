package org.knime.hdf5.lib.types;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;

public enum Hdf5KnimeDataType {
	UNKNOWN,		// data type is unknown
	INTEGER,		// data type is an Integer
	LONG,			// data type is a Long
	DOUBLE,			// data type is a Double
	STRING;			// data type is a String

	public DataType getColumnType() {
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
			return null;
		}
	}

	public Object createArray(int length) {
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
			return null;
		}
	}
	
	public String getArrayType() {
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
			return null;
		}
	}
}
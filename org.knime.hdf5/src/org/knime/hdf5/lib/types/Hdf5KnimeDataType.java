package org.knime.hdf5.lib.types;

import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;

public enum Hdf5KnimeDataType {
	UNKNOWN(-1),		// data type is unknown
	INTEGER(0),			// data type is an Integer
	LONG(1),			// data type is a Long
	DOUBLE(2),			// data type is a Double
	STRING(3);			// data type is a String

	private final int m_typeId;

	Hdf5KnimeDataType(final int typeId) {
		m_typeId = typeId;
	}

	public DataType getColumnType() {
		switch (m_typeId) {
		case 0: 
			return IntCell.TYPE;
		case 1: 
			return LongCell.TYPE;
		case 2: 
			return DoubleCell.TYPE;
		case 3:
			return StringCell.TYPE;
		}
		return null;
	}
}
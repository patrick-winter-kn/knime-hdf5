package org.knime.hdf5.lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

enum Hdf5Object {
	GROUP(0),			// Object is a group
	DATASET(1),			// Object is a dataset
	TYPE(2),			// Object is a named data type
	LINK(3),			// Object is a symbolic link
	UDLINK(4),			// Object is a user-defined link
	ATTRIBUTE(5), 		// Object is an attribute (but not a real object in .h5)
	RESERVED_6(6),		// Reserved for future use
	RESERVED_7(7);		// Reserved for future use
	private static final Map<Integer, Hdf5Object> lookup = new HashMap<>();

	static {
		for (Hdf5Object o : EnumSet.allOf(Hdf5Object.class)) {
			lookup.put(o.getType(), o);
		}
	}

	private int m_type;

	Hdf5Object(int type) {
		m_type = type;
	}

	public int getType() {
		return m_type;
	}

	public static Hdf5Object get(int type) {
		return lookup.get(type);
	}
}
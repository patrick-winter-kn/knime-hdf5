package org.knime.hdf5.lib;

//TODO
public class Hdf5Attribute<Type> {
	
	private final String name;
	private final String value;
	
	public Hdf5Attribute/*<Type>*/(final String name, final String value) {
		this.name = name;
		this.value = value;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getValue() {
		return this.value;
	}

}

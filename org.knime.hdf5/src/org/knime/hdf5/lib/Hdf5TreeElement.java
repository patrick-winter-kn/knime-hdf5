package org.knime.hdf5.lib;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

//TODO
abstract public class Hdf5TreeElement {
	
	private final List<Hdf5Attribute<?>> attributes = new LinkedList<>();
	
	public Hdf5TreeElement() {
		
	}
	
	// TODO maybe set to private
	public List<Hdf5Attribute<?>> getAttributes() {
		return attributes;
	}

	public Hdf5Attribute<?>[] listAttributes() {
		//Hdf5Attribute<?>[] attributes = (Hdf5Attribute<?>[]) this.getAttributes().toArray();
		Hdf5Attribute<?>[] attributes = new Hdf5Attribute<?>[this.getAttributes().size()];
		Iterator<Hdf5Attribute<?>> iter = this.getAttributes().iterator();
		int i = 0;
		while (iter.hasNext()) {
			attributes[i] = iter.next();
			i++;
		}
		return attributes;
	}

	public Hdf5Attribute<?> getAttribute(final String name) {
		Iterator<Hdf5Attribute<?>> iter = this.getAttributes().iterator();
		while (iter.hasNext()) {
			Hdf5Attribute<?> attr = iter.next();
			if (attr.getName() == name) {
				return attr;
			}
		}
		return null;
	}

	public void addAttribute(Hdf5Attribute<?> attribute) {
		if (this.getAttribute(attribute.getName()) == null) {
			this.getAttributes().add(attribute);
		}
	}
}

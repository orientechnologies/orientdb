package com.orientechnologies.common.collection;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.IdentityHashMap;
import java.util.Iterator;

public class OIdentityHashSet<E> extends AbstractSet<E> implements Serializable {
	private static final long											serialVersionUID	= 1L;

	private static final Object										VALUE							= new Object();

	private transient IdentityHashMap<E, Object>	identityHashMap;

	public OIdentityHashSet() {
		identityHashMap = new IdentityHashMap<E, Object>();
	}

	@Override
	public Iterator<E> iterator() {
		return identityHashMap.keySet().iterator();
	}

	@Override
	public int size() {
		return identityHashMap.size();
	}

	@Override
	public boolean add(E e) {
		return identityHashMap.put(e, VALUE) == null;
	}

	@Override
	public boolean remove(Object o) {
		return identityHashMap.remove(o) == VALUE;
	}

	@Override
	public boolean contains(Object o) {
		return identityHashMap.containsKey(o);
	}

	@Override
	public boolean isEmpty() {
		return identityHashMap.isEmpty();
	}

	@Override
	public void clear() {
		identityHashMap.clear();
	}

	private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
		s.defaultWriteObject();

		s.write(identityHashMap.size());

		for (E e : identityHashMap.keySet())
			s.writeObject(e);
	}

	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		s.defaultReadObject();

		int size = s.readInt();

		identityHashMap = new IdentityHashMap<E, Object>(size);

		for (int i = 0; i < size; i++)
			identityHashMap.put((E) s.readObject(), VALUE);

	}
}

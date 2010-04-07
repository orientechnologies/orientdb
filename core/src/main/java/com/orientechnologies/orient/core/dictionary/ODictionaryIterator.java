package com.orientechnologies.orient.core.dictionary;

import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OTreeMap;

public class ODictionaryIterator<T> implements Iterator<Entry<String, T>> {

	private Iterator<Entry<String, T>>	underltingIterator;

	public ODictionaryIterator(final OTreeMap<String, T> iTree) {
		underltingIterator = iTree.entrySet().iterator();
	}

	public boolean hasNext() {
		return underltingIterator.hasNext();
	}

	public Entry<String, T> next() {
		return underltingIterator.next();
	}

	public void remove() {
		underltingIterator.remove();
	}
}

package com.orientechnologies.orient.core.dictionary;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class ODictionaryIterator<T> implements Iterator<Entry<String, T>> {

	private Iterator<Entry<String, T>>	underltingIterator;

	public ODictionaryIterator(final Map<String, T> iTree) {
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

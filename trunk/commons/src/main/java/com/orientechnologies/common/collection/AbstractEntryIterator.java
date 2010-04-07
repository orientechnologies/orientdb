package com.orientechnologies.common.collection;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Base class for OTreeMap Iterators
 */
abstract class AbstractEntryIterator<K, V, T> implements Iterator<T> {
	OTreeMap<K, V>			tree;
	OTreeMapEntry<K, V>	next;
	OTreeMapEntry<K, V>	lastReturned;
	int									expectedModCount;

	AbstractEntryIterator(OTreeMapEntry<K, V> first) {
		if (first == null)
			// IN CASE OF ABSTRACTMAP.HASHCODE()
			return;

		tree = first.getTree();
		next = first;
		expectedModCount = tree.modCount;
		lastReturned = null;
		tree.pageIndex = -1;
	}

	public final boolean hasNext() {
		return next != null && (OTreeMap.successor(next) != null || tree.pageIndex < next.getSize() - 1);
	}

	final OTreeMapEntry<K, V> nextEntry() {
		if (next == null)
			throw new NoSuchElementException();

		if (tree.pageIndex < next.getSize() - 1) {
			// ITERATE INSIDE THE NODE
			tree.pageIndex++;
		} else {
			// GET THE NEXT NODE
			if (tree.modCount != expectedModCount)
				throw new ConcurrentModificationException();

			tree.pageIndex = 0;
			next = OTreeMap.successor(next);
			lastReturned = next;
		}

		return next;
	}

	final OTreeMapEntry<K, V> prevEntry() {
		OTreeMapEntry<K, V> e = next;
		if (e == null)
			throw new NoSuchElementException();
		if (tree.modCount != expectedModCount)
			throw new ConcurrentModificationException();
		next = OTreeMap.predecessor(e);
		lastReturned = e;
		return e;
	}

	public void remove() {
		if (lastReturned == null)
			throw new IllegalStateException();
		if (tree.modCount != expectedModCount)
			throw new ConcurrentModificationException();
		// deleted entries are replaced by their successors
		if (lastReturned.getLeft() != null && lastReturned.getRight() != null)
			next = lastReturned;
		tree.deleteEntry(lastReturned);
		expectedModCount = tree.modCount;
		lastReturned = null;
	}
}

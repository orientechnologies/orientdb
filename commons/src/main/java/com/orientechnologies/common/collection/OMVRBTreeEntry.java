/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.collection;

import java.util.Map;

@SuppressWarnings("unchecked")
public abstract class OMVRBTreeEntry<K, V> implements Map.Entry<K, V>, Comparable<OMVRBTreeEntry<K, V>> {
	protected OMVRBTree<K, V>	tree;

	private int								pageSplitItems;
	public static final int		BINARY_SEARCH_THRESHOLD	= 10;

	/**
	 * Constructor called on unmarshalling.
	 * 
	 */
	protected OMVRBTreeEntry(final OMVRBTree<K, V> iTree) {
		tree = iTree;
	}

	public abstract void setLeft(OMVRBTreeEntry<K, V> left);

	public abstract OMVRBTreeEntry<K, V> getLeft();

	public abstract void setRight(OMVRBTreeEntry<K, V> right);

	public abstract OMVRBTreeEntry<K, V> getRight();

	public abstract OMVRBTreeEntry<K, V> setParent(OMVRBTreeEntry<K, V> parent);

	public abstract OMVRBTreeEntry<K, V> getParent();

	protected abstract OMVRBTreeEntry<K, V> getLeftInMemory();

	protected abstract OMVRBTreeEntry<K, V> getParentInMemory();

	protected abstract OMVRBTreeEntry<K, V> getRightInMemory();

	protected abstract OMVRBTreeEntry<K, V> getNextInMemory();

	/**
	 * Returns the first Entry only by traversing the memory, or null if no such.
	 */
	public OMVRBTreeEntry<K, V> getFirstInMemory() {
		OMVRBTreeEntry<K, V> node = this;
		OMVRBTreeEntry<K, V> prev = this;

		while (node != null) {
			prev = node;
			node = node.getPreviousInMemory();
		}

		return prev;
	}

	/**
	 * Returns the previous of the current Entry only by traversing the memory, or null if no such.
	 */
	public OMVRBTreeEntry<K, V> getPreviousInMemory() {
		OMVRBTreeEntry<K, V> t = this;
		OMVRBTreeEntry<K, V> p = null;

		if (t.getLeftInMemory() != null) {
			p = t.getLeftInMemory();
			while (p.getRightInMemory() != null)
				p = p.getRightInMemory();
		} else {
			p = t.getParentInMemory();
			while (p != null && t == p.getLeftInMemory()) {
				t = p;
				p = p.getParentInMemory();
			}
		}

		return p;
	}

	protected OMVRBTree<K, V> getTree() {
		return tree;
	}

	public int getDepth() {
		int level = 0;
		OMVRBTreeEntry<K, V> entry = this;
		while (entry.getParent() != null) {
			level++;
			entry = entry.getParent();
		}
		return level;
	}

	/**
	 * Returns the key.
	 * 
	 * @return the key
	 */
	public K getKey() {
		return getKey(tree.pageIndex);
	}

	public K getKey(final int iIndex) {
		if (iIndex >= getSize())
			throw new IndexOutOfBoundsException("Requested index " + iIndex + " when the range is 0-" + getSize());

		tree.pageIndex = iIndex;
		return getKeyAt(iIndex);
	}

	protected abstract K getKeyAt(final int iIndex);

	/**
	 * Returns the value associated with the key.
	 * 
	 * @return the value associated with the key
	 */
	public V getValue() {
		if (tree.pageIndex == -1)
			return getValueAt(0);

		return getValueAt(tree.pageIndex);
	}

	public V getValue(final int iIndex) {
		tree.pageIndex = iIndex;
		return getValueAt(iIndex);
	}

	protected abstract V getValueAt(int iIndex);

	public int getFreeSpace() {
		return getPageSize() - getSize();
	}

	/**
	 * Execute a binary search between the keys of the node. The keys are always kept ordered. It update the pageIndex attribute with
	 * the most closer key found (useful for the next inserting).
	 * 
	 * @param iKey
	 *          Key to find
	 * @return The value found if any, otherwise null
	 */
	protected V search(final Comparable<? super K> iKey) {
		tree.pageItemFound = false;
		int size = getSize();
		if (size == 0)
			return null;

		// CHECK THE LOWER LIMIT
		if (tree.comparator != null)
			tree.pageItemComparator = tree.comparator.compare((K) iKey, getKeyAt(0));
		else
			tree.pageItemComparator = iKey.compareTo(getKeyAt(0));

		if (tree.pageItemComparator == 0) {
			// FOUND: SET THE INDEX AND RETURN THE NODE
			tree.pageItemFound = true;
			tree.pageIndex = 0;
			return getValueAt(tree.pageIndex);

		} else if (tree.pageItemComparator < 0) {
			// KEY OUT OF FIRST ITEM: AVOID SEARCH AND RETURN THE FIRST POSITION
			tree.pageIndex = 0;
			return null;

		} else {
			// CHECK THE UPPER LIMIT
			if (tree.comparator != null)
				tree.pageItemComparator = tree.comparator.compare((K) iKey, getKeyAt(size - 1));
			else
				tree.pageItemComparator = iKey.compareTo(getKeyAt(size - 1));

			if (tree.pageItemComparator > 0) {
				// KEY OUT OF LAST ITEM: AVOID SEARCH AND RETURN THE LAST POSITION
				tree.pageIndex = size;
				return null;
			}
		}

		if (size < BINARY_SEARCH_THRESHOLD)
			return linearSearch(iKey);
		else
			return binarySearch(iKey);
	}

	/**
	 * Linear search inside the node
	 * 
	 * @param iKey
	 *          Key to search
	 * @return Value if found, otherwise null and the tree.pageIndex updated with the closest-after-first position valid for further
	 *         inserts.
	 */
	private V linearSearch(final Comparable<? super K> iKey) {
		V value = null;
		int i = 0;
		tree.pageItemComparator = -1;
		for (int s = getSize(); i < s; ++i) {
			if (tree.comparator != null)
				tree.pageItemComparator = tree.comparator.compare(getKeyAt(i), (K) iKey);
			else
				tree.pageItemComparator = ((Comparable<? super K>) getKeyAt(i)).compareTo((K) iKey);

			if (tree.pageItemComparator == 0) {
				// FOUND: SET THE INDEX AND RETURN THE NODE
				tree.pageItemFound = true;
				value = getValueAt(i);
				break;
			} else if (tree.pageItemComparator > 0)
				break;
		}

		tree.pageIndex = i;

		return value;
	}

	/**
	 * Binary search inside the node
	 * 
	 * @param iKey
	 *          Key to search
	 * @return Value if found, otherwise null and the tree.pageIndex updated with the closest-after-first position valid for further
	 *         inserts.
	 */
	private V binarySearch(final Comparable<? super K> iKey) {
		int low = 0;
		int high = getSize() - 1;
		int mid = 0;

		while (low <= high) {
			mid = (low + high) >>> 1;
			Comparable<Comparable<? super K>> midVal = (Comparable<Comparable<? super K>>) getKeyAt(mid);

			if (tree.comparator != null)
				tree.pageItemComparator = tree.comparator.compare((K) midVal, (K) iKey);
			else
				tree.pageItemComparator = midVal.compareTo((Comparable<? super K>) iKey);

			if (tree.pageItemComparator == 0) {
				// FOUND: SET THE INDEX AND RETURN THE NODE
				tree.pageItemFound = true;
				tree.pageIndex = mid;
				return getValueAt(tree.pageIndex);
			}

			if (low == high)
				break;

			if (tree.pageItemComparator < 0)
				low = mid + 1;
			else
				high = mid;
		}

		tree.pageIndex = mid;
		return null;
	}

	protected abstract void insert(final int iPosition, final K key, final V value);

	protected abstract void remove();

	protected abstract void setColor(boolean iColor);

	public abstract boolean getColor();

	public abstract int getSize();

	public K getLastKey() {
		return getKey(getSize() - 1);
	}

	public K getFirstKey() {
		return getKey(0);
	}

	protected abstract void copyFrom(final OMVRBTreeEntry<K, V> iSource);

	public int getPageSplitItems() {
		return pageSplitItems;
	}

	protected void init() {
		pageSplitItems = (int) (getPageSize() * tree.pageLoadFactor);
	}

	public abstract int getPageSize();

	/**
	 * Compares two nodes by their first keys.
	 */
	public int compareTo(final OMVRBTreeEntry<K, V> o) {
		if (o == null)
			return 1;
		if (o == this)
			return 0;
		if (getSize() == 0)
			return -1;
		if (o.getSize() == 0)
			return 1;
		return ((Comparable<K>) getFirstKey()).compareTo(o.getFirstKey());
	}
}
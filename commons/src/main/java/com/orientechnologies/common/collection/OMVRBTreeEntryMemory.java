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

import java.util.Arrays;

public class OMVRBTreeEntryMemory<K, V> extends OMVRBTreeEntry<K, V> {

	protected int													size	= 1;
	protected int													pageSize;

	protected K[]													keys;
	protected V[]													values;
	protected OMVRBTreeEntryMemory<K, V>	left	= null;
	protected OMVRBTreeEntryMemory<K, V>	right	= null;
	protected OMVRBTreeEntryMemory<K, V>	parent;

	protected boolean											color	= OMVRBTree.RED;

	/**
	 * Constructor called on unmarshalling.
	 * 
	 */
	protected OMVRBTreeEntryMemory(final OMVRBTree<K, V> iTree) {
		super(iTree);
	}

	/**
	 * Make a new cell with given key, value, and parent, and with <tt>null</tt> child links, and BLACK color.
	 */
	protected OMVRBTreeEntryMemory(final OMVRBTree<K, V> iTree, final K iKey, final V iValue, final OMVRBTreeEntryMemory<K, V> iParent) {
		super(iTree);
		setParent(iParent);
		pageSize = tree.getDefaultPageSize();
		keys = (K[]) new Object[pageSize];
		keys[0] = iKey;
		values = (V[]) new Object[pageSize];
		values[0] = iValue;
		init();
	}

	/**
	 * Copy values from the parent node.
	 * 
	 * @param iParent
	 * @param iPosition
	 * @param iLeft
	 */
	protected OMVRBTreeEntryMemory(final OMVRBTreeEntryMemory<K, V> iParent, final int iPosition) {
		super(iParent.getTree());
		pageSize = tree.getDefaultPageSize();
		keys = (K[]) new Object[pageSize];
		values = (V[]) new Object[pageSize];

		size = iParent.size - iPosition;
		System.arraycopy(iParent.keys, iPosition, keys, 0, size);
		System.arraycopy(iParent.values, iPosition, values, 0, size);

		Arrays.fill(iParent.keys, iPosition, iParent.size, null);
		Arrays.fill(iParent.values, iPosition, iParent.size, null);

		iParent.size = iPosition;
		setParent(iParent);

		init();
	}

	@Override
	protected void setColor(final boolean iColor) {
		this.color = iColor;
	}

	@Override
	public boolean getColor() {
		return color;
	}

	@Override
	public void setLeft(final OMVRBTreeEntry<K, V> iLeft) {
		left = (OMVRBTreeEntryMemory<K, V>) iLeft;
		if (iLeft != null && iLeft.getParent() != this)
			iLeft.setParent(this);
	}

	@Override
	public OMVRBTreeEntry<K, V> getLeft() {
		return left;
	}

	@Override
	public void setRight(final OMVRBTreeEntry<K, V> iRight) {
		right = (OMVRBTreeEntryMemory<K, V>) iRight;
		if (iRight != null && iRight.getParent() != this)
			iRight.setParent(this);
	}

	@Override
	public OMVRBTreeEntry<K, V> getRight() {
		return right;
	}

	@Override
	public OMVRBTreeEntry<K, V> setParent(final OMVRBTreeEntry<K, V> iParent) {
		parent = (OMVRBTreeEntryMemory<K, V>) iParent;
		return iParent;
	}

	@Override
	public OMVRBTreeEntry<K, V> getParent() {
		return parent;
	}

	/**
	 * Returns the successor of the current Entry only by traversing the memory, or null if no such.
	 */
	@Override
	public OMVRBTreeEntryMemory<K, V> getNextInMemory() {
		OMVRBTreeEntryMemory<K, V> t = this;
		OMVRBTreeEntryMemory<K, V> p = null;

		if (t.right != null) {
			p = t.right;
			while (p.left != null)
				p = p.left;
		} else {
			p = t.parent;
			while (p != null && t == p.right) {
				t = p;
				p = p.parent;
			}
		}

		return p;
	}

	public int getSize() {
		return size;
	}

	public int getPageSize() {
		return pageSize;
	}

	@Override
	protected OMVRBTreeEntry<K, V> getLeftInMemory() {
		return left;
	}

	@Override
	protected OMVRBTreeEntry<K, V> getParentInMemory() {
		return parent;
	}

	@Override
	protected OMVRBTreeEntry<K, V> getRightInMemory() {
		return right;
	}

	protected K getKeyAt(final int iIndex) {
		return keys[iIndex];
	}

	protected V getValueAt(int iIndex) {
		return values[iIndex];
	}

	/**
	 * Replaces the value currently associated with the key with the given value.
	 * 
	 * @return the value associated with the key before this method was called
	 */
	public V setValue(final V value) {
		V oldValue = this.getValue();
		this.values[tree.pageIndex] = value;
		return oldValue;
	}

	protected void insert(final int iPosition, final K key, final V value) {
		if (iPosition < size) {
			// MOVE RIGHT TO MAKE ROOM FOR THE ITEM
			System.arraycopy(keys, iPosition, keys, iPosition + 1, size - iPosition);
			System.arraycopy(values, iPosition, values, iPosition + 1, size - iPosition);
		}

		keys[iPosition] = key;
		values[iPosition] = value;
		size++;
	}

	protected void remove() {
		if (tree.pageIndex == size - 1) {
			// LAST ONE: JUST REMOVE IT
		} else if (tree.pageIndex > -1) {
			// SHIFT LEFT THE VALUES
			System.arraycopy(keys, tree.pageIndex + 1, keys, tree.pageIndex, size - tree.pageIndex - 1);
			System.arraycopy(values, tree.pageIndex + 1, values, tree.pageIndex, size - tree.pageIndex - 1);
		}

		// FREE RESOURCES
		keys[size - 1] = null;
		values[size - 1] = null;

		size--;
		tree.pageIndex = 0;
	}

	protected void copyFrom(final OMVRBTreeEntry<K, V> iSource) {
		OMVRBTreeEntryMemory<K, V> source = (OMVRBTreeEntryMemory) iSource;
		keys = (K[]) new Object[source.keys.length];
		for (int i = 0; i < source.keys.length; ++i)
			keys[i] = source.keys[i];

		values = (V[]) new Object[source.values.length];
		for (int i = 0; i < source.values.length; ++i)
			values[i] = source.values[i];

		size = source.size;
	}

	@Override
	public String toString() {
		if (keys == null)
			return "?";

		final StringBuilder buffer = new StringBuilder();

		final Object k = tree.pageIndex >= size ? '?' : getKey();

		buffer.append(k);
		buffer.append(" (size=");
		buffer.append(size);
		if (size > 0) {
			buffer.append(" [");
			buffer.append(keys[0] != null ? keys[0] : "{lazy}");
			buffer.append('-');
			buffer.append(keys[size - 1] != null ? keys[size - 1] : "{lazy}");
			buffer.append(']');
		}
		buffer.append(')');

		return buffer.toString();
	}
}
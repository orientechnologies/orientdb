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

public class OMVRBTreeEntryMemory<K, V> extends OMVRBTreeEntry<K, V> {
	protected OMVRBTreeEntryMemory<K, V>	left	= null;
	protected OMVRBTreeEntryMemory<K, V>	right	= null;
	protected OMVRBTreeEntryMemory<K, V>	parent;

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
		super(iTree, iKey, iValue, iParent);
	}

	/**
	 * Copy values from the parent node.
	 * 
	 * @param iParent
	 * @param iPosition
	 * @param iLeft
	 */
	protected OMVRBTreeEntryMemory(final OMVRBTreeEntry<K, V> iParent, final int iPosition) {
		super(iParent, iPosition);
		setParent(iParent);
	}

	@Override
	public void setLeft(final OMVRBTreeEntry<K, V> left) {
		this.left = (OMVRBTreeEntryMemory<K, V>) left;
		if (left != null && left.getParent() != this)
			left.setParent(this);
	}

	@Override
	public OMVRBTreeEntry<K, V> getLeft() {
		return left;
	}

	@Override
	public OMVRBTreeEntry<K, V> setRight(final OMVRBTreeEntry<K, V> right) {
		this.right = (OMVRBTreeEntryMemory<K, V>) right;
		if (right != null && right.getParent() != this)
			right.setParent(this);

		return right;
	}

	@Override
	public OMVRBTreeEntry<K, V> getRight() {
		return right;
	}

	@Override
	public OMVRBTreeEntry<K, V> setParent(final OMVRBTreeEntry<K, V> parent) {
		this.parent = (OMVRBTreeEntryMemory<K, V>) parent;
		return parent;
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
}
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

public class OTreeMapEntryMemory<K, V> extends OTreeMapEntry<K, V> {
	protected OTreeMapEntryMemory<K, V>	left	= null;
	protected OTreeMapEntryMemory<K, V>	right	= null;
	protected OTreeMapEntryMemory<K, V>	parent;

	/**
	 * Constructor called on unmarshalling.
	 * 
	 */
	protected OTreeMapEntryMemory(final OTreeMap<K, V> iTree) {
		super(iTree);
	}

	/**
	 * Make a new cell with given key, value, and parent, and with <tt>null</tt> child links, and BLACK color.
	 */
	protected OTreeMapEntryMemory(final OTreeMap<K, V> iTree, final K iKey, final V iValue, final OTreeMapEntryMemory<K, V> iParent) {
		super(iTree, iKey, iValue, iParent);
	}

	/**
	 * Copy values from the parent node.
	 * 
	 * @param iParent
	 * @param iPosition
	 */
	protected OTreeMapEntryMemory(final OTreeMapEntry<K, V> iParent, final int iPosition) {
		super(iParent, iPosition);
	}

	@Override
	public void setLeft(final OTreeMapEntry<K, V> left) {
		this.left = (OTreeMapEntryMemory<K, V>) left;
		if (left != null && left.getParent() != this)
			left.setParent(this);
	}

	@Override
	public OTreeMapEntry<K, V> getLeft() {
		return left;
	}

	@Override
	public OTreeMapEntry<K, V> setRight(final OTreeMapEntry<K, V> right) {
		this.right = (OTreeMapEntryMemory<K, V>) right;
		if (right != null && right.getParent() != this)
			right.setParent(this);

		return right;
	}

	@Override
	public OTreeMapEntry<K, V> getRight() {
		return right;
	}

	@Override
	public OTreeMapEntry<K, V> setParent(final OTreeMapEntry<K, V> parent) {
		this.parent = (OTreeMapEntryMemory<K, V>) parent;
		return parent;
	}

	@Override
	public OTreeMapEntry<K, V> getParent() {
		return parent;
	}

	/**
	 * Returns the successor of the current Entry only by traversing the memory, or null if no such.
	 */
	public OTreeMapEntryMemory<K, V> nextInMemory() {
		OTreeMapEntryMemory<K, V> t = this;
		OTreeMapEntryMemory<K, V> p = null;

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
}
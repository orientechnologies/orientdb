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

/**
 * Keeps the position of a key/value inside a tree node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OMVRBTreeEntryPosition<K, V> {
	public OMVRBTreeEntry<K, V>	entry;
	public int									position;

	public OMVRBTreeEntryPosition(final OMVRBTreeEntryPosition<K, V> entryPosition) {
		this.entry = entryPosition.entry;
		this.position = entryPosition.position;
	}

	public OMVRBTreeEntryPosition(final OMVRBTreeEntry<K, V> entry) {
		assign(entry);
	}

	public OMVRBTreeEntryPosition(final OMVRBTreeEntry<K, V> entry, final int iPosition) {
		assign(entry, iPosition);
	}

	public void assign(final OMVRBTreeEntry<K, V> entry, final int iPosition) {
		this.entry = entry;
		this.position = iPosition;
	}

	public void assign(final OMVRBTreeEntry<K, V> entry) {
		this.entry = entry;
		this.position = entry != null ? entry.getTree().getPageIndex() : -1;
	}

	public K getKey() {
		return entry != null ? entry.getKey(position) : null;
	}

	public V getValue() {
		return entry != null ? entry.getValue(position) : null;
	}
}
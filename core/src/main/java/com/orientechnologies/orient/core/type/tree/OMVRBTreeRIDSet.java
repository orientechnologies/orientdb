/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Kersion 2.0 (the "License");
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
package com.orientechnologies.orient.core.type.tree;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Persistent Set<ORecordId> implementation that uses the MVRB-Tree to handle entries in persistent way.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OMVRBTreeRIDSet implements Set<ORecordId> {
	private final OMVRBTreeRID	tree;

	public OMVRBTreeRIDSet(final ODatabaseRecord iDatabase, final ORID iRID) {
		this(new OMVRBTreeRID(iDatabase, iRID));
	}

	public OMVRBTreeRIDSet(final ODatabaseRecord iDatabase, String iClusterName) {
		this(new OMVRBTreeRID(iClusterName));
	}

	public OMVRBTreeRIDSet(final OMVRBTreeRID iProvider) {
		tree = iProvider;
	}

	public int size() {
		return tree.size();
	}

	public boolean isEmpty() {
		return tree.isEmpty();
	}

	public boolean contains(final Object o) {
		return tree.containsKey(o);
	}

	public Iterator<ORecordId> iterator() {
		return tree.keySet().iterator();
	}

	public Object[] toArray() {
		return tree.keySet().toArray();
	}

	public <T> T[] toArray(final T[] a) {
		return tree.keySet().toArray(a);
	}

	public boolean add(final ORecordId e) {
		return tree.put(e, e) != null;
	}

	public boolean remove(final Object o) {
		return tree.remove(o) != null;
	}

	public boolean containsAll(final Collection<?> c) {
		for (Object o : c)
			if (!tree.containsKey(o))
				return false;
		return true;
	}

	public boolean addAll(final Collection<? extends ORecordId> c) {
		for (ORecordId o : c)
			tree.put(o, o);
		return true;
	}

	public boolean retainAll(final Collection<?> c) {
		boolean modified = false;
		final Iterator<?> e = iterator();
		while (e.hasNext()) {
			if (!c.contains(e.next())) {
				e.remove();
				modified = true;
			}
		}
		return modified;
	}

	public boolean removeAll(final Collection<?> c) {
		boolean modified = false;
		for (Object o : c)
			if (tree.remove(o) != null)
				modified = true;
		return modified;
	}

	public void clear() {
		tree.clear();
	}
}

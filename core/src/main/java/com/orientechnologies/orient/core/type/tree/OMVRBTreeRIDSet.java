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
import java.util.Set;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.orient.core.db.record.ODetachable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Persistent Set<OIdentifiable> implementation that uses the MVRB-Tree to handle entries in persistent way.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OMVRBTreeRIDSet implements Set<OIdentifiable>, OStringBuilderSerializable, OSerializableStream, ODetachable {
	private static final long		serialVersionUID	= 1L;

	private final OMVRBTreeRID	tree;

	public OMVRBTreeRIDSet() {
		this(new OMVRBTreeRID());
	}

	public OMVRBTreeRIDSet(final ORID iRID) {
		this(new OMVRBTreeRID(iRID));
	}

	public OMVRBTreeRIDSet(final String iClusterName) {
		this(new OMVRBTreeRID(iClusterName));
	}

	public OMVRBTreeRIDSet(final ORecord<?> iOwner) {
		this((OMVRBTreeRID) new OMVRBTreeRID().setOwner(iOwner));
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

	public OLazyIterator<OIdentifiable> iterator(final boolean iAutoConvertToRecord) {
		return tree.iterator(iAutoConvertToRecord);
	}

	public OLazyIterator<OIdentifiable> iterator() {
		return tree.iterator();
	}

	public Object[] toArray() {
		return tree.toArray();
	}

	public <T> T[] toArray(final T[] a) {
		return tree.toArray(a);
	}

	public boolean add(final OIdentifiable e) {
		return tree.put(e, null) != null;
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

	public boolean addAll(final Collection<? extends OIdentifiable> c) {
		boolean changed = false;
		for (OIdentifiable o : c)
			if (add(o) && !changed)
				changed = true;
		return changed;
	}

	public boolean retainAll(final Collection<?> c) {
		return tree.retainAll(c);
	}

	public boolean removeAll(final Collection<?> c) {
		return tree.removeAll(c);
	}

	public boolean detach() {
		return tree.detach();
	}

	public void clear() {
		tree.clear();
	}

	public OMVRBTreeRIDSet fromDocument(final ODocument iDocument) {
		fromStream(iDocument.toStream());
		return this;
	}

	public ODocument toDocument() {
		tree.save();
		return ((OMVRBTreeRIDProvider) tree.getProvider()).toDocument();
	}

	public OMVRBTreeRIDSet copy() {
		final OMVRBTreeRIDSet clone = new OMVRBTreeRIDSet(new OMVRBTreeRID(new OMVRBTreeRIDProvider(null, tree.getProvider()
				.getClusterId())));
		clone.addAll(this);
		return clone;
	}

	public OStringBuilderSerializable fromStream(final StringBuilder iSource) {
		((OMVRBTreeRIDProvider) tree.getProvider()).fromStream(iSource);
		return this;
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		fromStream(new StringBuilder(OBinaryProtocol.bytes2string(iStream)));
		return this;
	}

	public OStringBuilderSerializable toStream(StringBuilder iOutput) throws OSerializationException {
		tree.save();
		((OMVRBTreeRIDProvider) tree.getProvider()).toStream(iOutput);
		return this;
	}

	public byte[] toStream() throws OSerializationException {
		final StringBuilder buffer = new StringBuilder();
		toStream(buffer);
		return buffer.toString().getBytes();
	}

	@Override
	public String toString() {
		return tree.toString();
	}

	public OMVRBTreeRIDSet setAutoConvert(final boolean b) {
		tree.setAutoConvert(b);
		return this;
	}
}

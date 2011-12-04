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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OLazyRecordIterator;
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
public class OMVRBTreeRIDSet implements Set<OIdentifiable>, OStringBuilderSerializable, OSerializableStream {
	private static final long		serialVersionUID		= 1L;

	private final OMVRBTreeRID	tree;
	private boolean							autoConvertToRecord	= true;

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

	public Iterator<OIdentifiable> iterator() {
		return new OLazyRecordIterator(tree.keySet().iterator(), autoConvertToRecord);
	}

	public Object[] toArray() {
		return tree.keySet().toArray();
	}

	public <T> T[] toArray(final T[] a) {
		return tree.keySet().toArray(a);
	}

	public boolean add(final OIdentifiable e) {
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

	public boolean addAll(final Collection<? extends OIdentifiable> c) {
		for (OIdentifiable o : c)
			tree.put(o, null);
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

	public void save() throws IOException {
		tree.save();
	}

	public ODocument toDocument() {
		tree.lazySave();
		return ((OMVRBTreeRIDProvider) tree.getProvider()).toDocument();
	}

	public OStringBuilderSerializable fromStream(final StringBuilder iSource) {
		((OMVRBTreeRIDProvider) tree.getProvider()).fromStream(iSource);
		return this;
	}

	public OStringBuilderSerializable toStream(StringBuilder iOutput) throws OSerializationException {
		((OMVRBTreeRIDProvider) tree.getProvider()).toStream(iOutput);
		return this;
	}

	public OMVRBTreeRIDSet fromDocument(final ODocument iDocument) {
		((OMVRBTreeRIDProvider) tree.getProvider()).fromDocument(iDocument);
		return this;
	}

	public OMVRBTreeRIDSet copy() {
		final OMVRBTreeRIDSet clone = new OMVRBTreeRIDSet(new OMVRBTreeRID(new OMVRBTreeRIDProvider(null, tree.getProvider()
				.getClusterId())));
		clone.addAll(this);
		return clone;
	}

	public byte[] toStream() throws OSerializationException {
		final StringBuilder buffer = new StringBuilder();
		toStream(buffer);
		return buffer.toString().getBytes();
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		fromStream(new StringBuilder(OBinaryProtocol.bytes2string(iStream)));
		return this;
	}

	@Override
	public String toString() {
		return tree.toString();
	}

	public boolean isAutoConvert() {
		return autoConvertToRecord;
	}

	public OMVRBTreeRIDSet setAutoConvert(boolean autoConvert) {
		this.autoConvertToRecord = autoConvert;
		return this;
	}
}

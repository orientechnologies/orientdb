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
package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Generic abstract wrapper for indexes. It delegates all the operations to the wrapped OIndex instance.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexAbstractDelegate<T> implements OIndex<T> {
	protected OIndex<T>	delegate;

	public OIndexAbstractDelegate(final OIndex<T> iDelegate) {
		this.delegate = iDelegate;
	}

	public OIndexInternal<T> getInternal() {
		return (OIndexInternal<T>) delegate;
	}

	public OIndex<T> create(final String iName, final OType iKeyType, final ODatabaseRecord iDatabase,
			final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener,
			final boolean iAutomatic) {
		return delegate.create(iName, iKeyType, iDatabase, iClusterIndexName, iClusterIdsToIndex, iProgressListener, iAutomatic);
	}

	public Iterator<Entry<Object, T>> iterator() {
		return delegate.iterator();
	}

	public T get(final Object iKey) {
		return delegate.get(iKey);
	}

	public boolean contains(final Object iKey) {
		return delegate.contains(iKey);
	}

	public OIndex<T> put(final Object iKey, final OIdentifiable iValue) {
		return delegate.put(iKey, iValue);
	}

	public boolean remove(final Object iKey) {
		return delegate.remove(iKey);
	}

	public boolean remove(final Object iKey, final OIdentifiable iRID) {
		return delegate.remove(iKey, iRID);
	}

	public int remove(final OIdentifiable iRID) {
		return delegate.remove(iRID);
	}

	public OIndex<T> clear() {
		return delegate.clear();
	}

	public Iterable<Object> keys() {
		return delegate.keys();
	}

	public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
		return delegate.getValuesBetween(iRangeFrom, iRangeTo);
	}

	public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
		return delegate.getValuesBetween(iRangeFrom, iRangeTo, iInclusive);
	}

	public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
		return delegate.getEntriesBetween(iRangeFrom, iRangeTo, iInclusive);
	}

	public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
		return delegate.getEntriesBetween(iRangeFrom, iRangeTo);
	}

	public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
		return delegate.getValuesMajor(fromKey, isInclusive);
	}

	public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
		return delegate.getEntriesMajor(fromKey, isInclusive);
	}

	public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
		return delegate.getValuesMinor(toKey, isInclusive);
	}

	public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
		return delegate.getEntriesMinor(toKey, isInclusive);
	}

	public long getSize() {
		return delegate.getSize();
	}

	public OIndex<T> lazySave() {
		return delegate.lazySave();
	}

	public OIndex<T> delete() {
		return delegate.delete();
	}

	public String getName() {
		return delegate.getName();
	}

	public String getType() {
		return delegate.getType();
	}

	public boolean isAutomatic() {
		return delegate.isAutomatic();
	}

	public void setCallback(final OIndexCallback iCallback) {
		delegate.setCallback(iCallback);
	}

	public ODocument getConfiguration() {
		return delegate.getConfiguration();
	}

	public ORID getIdentity() {
		return delegate.getIdentity();
	}

	public void commit(final ODocument iDocument) {
		delegate.commit(iDocument);
	}

	public void unload() {
		delegate.unload();
	}

	public long rebuild() {
		return delegate.rebuild();
	}

	public long rebuild(final OProgressListener iProgressListener) {
		return delegate.rebuild(iProgressListener);
	}

	public OType getKeyType() {
		return delegate.getKeyType();
	}

    public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
        return delegate.getValues(iKeys);
    }

    public Collection<ODocument> getEntries(final Collection<?> iKeys) {
        return delegate.getEntries(iKeys);
    }
}

/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
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
  protected OIndex<T> delegate;

  public OIndexAbstractDelegate(final OIndex<T> iDelegate) {
    this.delegate = iDelegate;
  }

  @SuppressWarnings("unchecked")
  public OIndexInternal<T> getInternal() {
    OIndex<?> internal = delegate;
    while (!(internal instanceof OIndexInternal) && internal != null)
      internal = internal.getInternal();

    return (OIndexInternal<T>) internal;
  }

  public OIndex<T> create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return delegate.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  public Iterator<Entry<Object, T>> iterator() {
    return delegate.iterator();
  }

  @Override
  public Iterator<Entry<Object, T>> inverseIterator() {
    return delegate.inverseIterator();
  }

  @Override
  public Iterator<OIdentifiable> valuesIterator() {
    return delegate.valuesIterator();
  }

  @Override
  public Iterator<OIdentifiable> valuesInverseIterator() {
    return delegate.valuesInverseIterator();
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

  public boolean remove(final Object key) {
    return delegate.remove(key);
  }

  public boolean remove(final Object iKey, final OIdentifiable iRID) {
    return delegate.remove(iKey, iRID);
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

  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive) {
    return delegate.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);
  }

  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
    return delegate.getEntriesBetween(iRangeFrom, iRangeTo, iInclusive);
  }

  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
    return delegate.getEntriesBetween(iRangeFrom, iRangeTo);
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive) {
    return delegate.getValuesMajor(fromKey, isInclusive);
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive) {
    return delegate.getEntriesMajor(fromKey, isInclusive);
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive) {
    return delegate.getValuesMinor(toKey, isInclusive);
  }

  public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive) {
    return delegate.getEntriesMinor(toKey, isInclusive);
  }

  public long getSize() {
    return delegate.getSize();
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  public OIndex<T> delete() {
    return delegate.delete();
  }

  @Override
  public void deleteWithoutIndexLoad(String indexName) {
    delegate.deleteWithoutIndexLoad(indexName);
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

  public ODocument getConfiguration() {
    return delegate.getConfiguration();
  }

  @Override
  public ODocument getMetadata() {
    return delegate.getMetadata();
  }

  public ORID getIdentity() {
    return delegate.getIdentity();
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

  public OType[] getKeyTypes() {
    return delegate.getKeyTypes();
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
    return delegate.getValues(iKeys);
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    return delegate.getEntries(iKeys);
  }

  public OIndexDefinition getDefinition() {
    return delegate.getDefinition();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OIndexAbstractDelegate<?> that = (OIndexAbstractDelegate<?>) o;

    if (!delegate.equals(that.delegate))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  public void getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive, IndexValuesResultListener valuesResultListener) {
    delegate.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, valuesResultListener);
  }

  public void getValuesMajor(final Object fromKey, final boolean isInclusive, IndexValuesResultListener valuesResultListener) {
    delegate.getValuesMajor(fromKey, isInclusive, valuesResultListener);
  }

  public void getValuesMinor(final Object toKey, final boolean isInclusive, IndexValuesResultListener valuesResultListener) {
    delegate.getValuesMinor(toKey, isInclusive, valuesResultListener);
  }

  public void getEntriesMajor(final Object fromKey, final boolean isInclusive, IndexEntriesResultListener entriesResultListener) {
    delegate.getEntriesMajor(fromKey, isInclusive, entriesResultListener);
  }

  public void getEntriesMinor(final Object toKey, final boolean isInclusive, final IndexEntriesResultListener entriesResultListener) {
    delegate.getEntriesMinor(toKey, isInclusive, entriesResultListener);
  }

  public void getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive,
      final IndexEntriesResultListener entriesResultListener) {
    delegate.getEntriesBetween(iRangeFrom, iRangeTo, iInclusive, entriesResultListener);
  }

  public void getValues(final Collection<?> iKeys, final IndexValuesResultListener resultListener) {
    delegate.getValues(iKeys, resultListener);
  }

  public void getEntries(final Collection<?> iKeys, IndexEntriesResultListener resultListener) {
    delegate.getEntries(iKeys, resultListener);
  }

  public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
    delegate.checkEntry(iRecord, iKey);
  }

  public Set<String> getClusters() {
    return delegate.getClusters();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public long getKeySize() {
    return delegate.getKeySize();
  }

  public String getDatabaseName() {
    return delegate.getDatabaseName();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return delegate.supportsOrderedIterations();
  }

  @Override
  public boolean isRebuiding() {
    return delegate.isRebuiding();
  }
}

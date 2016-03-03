/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Set;

/**
 * Generic abstract wrapper for indexes. It delegates all the operations to the wrapped OIndex instance.
 *
 * @author Luca Garulli
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

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    return delegate.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return delegate.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return delegate.iterateEntriesMinor(toKey, toInclusive, ascOrder);
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

  @Override
  public String getAlgorithm() {
    return delegate.getAlgorithm();
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

  public long rebuild() {
    return delegate.rebuild();
  }

  public long rebuild(final OProgressListener iProgressListener) {
    return delegate.rebuild(iProgressListener);
  }

  public OType[] getKeyTypes() {
    return delegate.getKeyTypes();
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

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return delegate.iterateEntries(keys, ascSortOrder);
  }

  public ODocument checkEntry(final OIdentifiable iRecord, final Object iKey) {
    return delegate.checkEntry(iRecord, iKey);
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
  public long getRebuildVersion() {
    return delegate.getRebuildVersion();
  }

  @Override
  public boolean isRebuilding() {
    return delegate.isRebuilding();
  }

  @Override
  public Object getFirstKey() {
    return delegate.getFirstKey();
  }

  @Override
  public Object getLastKey() {
    return delegate.getLastKey();
  }

  @Override
  public OIndexCursor cursor() {
    return delegate.cursor();
  }

  @Override
  public OIndexCursor descCursor() {
    return delegate.descCursor();
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return delegate.keyCursor();
  }

  @Override
  public int compareTo(OIndex<T> o) {
    return delegate.compareTo(o);
  }
}

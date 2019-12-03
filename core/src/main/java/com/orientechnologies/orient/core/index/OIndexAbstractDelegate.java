/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Generic abstract wrapper for indexes. It delegates all the operations to the wrapped OIndex instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexAbstractDelegate implements OIndex {
  protected OIndex delegate;

  public OIndexAbstractDelegate(final OIndex iDelegate) {
    this.delegate = iDelegate;
  }

  public OIndexInternal getInternal() {
    OIndex internal = delegate;
    while (!(internal instanceof OIndexInternal) && internal != null)
      internal = internal.getInternal();

    return (OIndexInternal) internal;
  }

  public OIndex create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return delegate.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  public Object get(final Object iKey) {
    return delegate.get(iKey);
  }

  public OIndex put(final Object iKey, final OIdentifiable iValue) {
    checkForKeyType(iKey);
    return delegate.put(iKey, iValue);
  }

  @Override
  public long getRebuildVersion() {
    return delegate.getRebuildVersion();
  }

  @Override
  public int getVersion() {
    return delegate.getVersion();
  }

  public boolean remove(final Object key) {
    return delegate.remove(key);
  }

  public boolean remove(final Object iKey, final OIdentifiable iRID) {
    return delegate.remove(iKey, iRID);
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  public OIndex clear() {
    return delegate.clear();
  }

  protected void checkForKeyType(final Object iKey) {
    if (delegate.getDefinition() == null) {
      // RECOGNIZE THE KEY TYPE AT RUN-TIME

      final OType type = OType.getTypeByClass(iKey.getClass());
      if (type == null)
        return;

      OIndexManagerAbstract indexManager = ODatabaseRecordThreadLocal.instance().get().getMetadata().getIndexManagerInternal();
      getInternal().setType(type);
      indexManager.save();
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    return delegate.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return delegate.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return delegate.iterateEntriesMinor(toKey, toInclusive, ascOrder);
  }

  public long getSize() {
    return delegate.getSize();
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  public OIndex delete() {
    return delegate.delete();
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

  @Override
  public boolean isUnique() {
    return delegate.isUnique();
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

    final OIndexAbstractDelegate that = (OIndexAbstractDelegate) o;

    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return delegate.iterateEntries(keys, ascSortOrder);
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
  public Object getLastKey() {
    return delegate.getLastKey();
  }

  @Override
  public int getIndexId() {
    return delegate.getIndexId();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    return delegate.stream();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    return delegate.descStream();
  }

  @Override
  public Stream<Object> keyStream() {
    return delegate.keyStream();
  }

  @Override
  public int compareTo(OIndex o) {
    return delegate.compareTo(o);
  }
}

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
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Generic abstract wrapper for indexes. It delegates all the operations to the wrapped OIndex instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexAbstractDelegate implements OIndexInternal {
  protected OIndexInternal delegate;

  public OIndexAbstractDelegate(final OIndexInternal internal) {
    this.delegate = internal;
  }

  public OIndexInternal getInternal() {
    return this;
  }

  public OIndex create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return delegate.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  @Deprecated
  public Object get(final Object key) {
    return delegate.get(key);
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    return delegate.getRids(key);
  }

  public OIndex put(final Object iKey, final OIdentifiable iValue) {
    checkForKeyType(iKey);
    return delegate.put(iKey, iValue);
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
      delegate.setType(type);
      indexManager.save();
    }
  }

  @Deprecated
  @Override
  public long getSize() {
    return delegate.getSize();
  }

  @Deprecated
  @Override
  public long count(Object key) {
    return delegate.count(key);
  }

  @Deprecated
  @Override
  public long getKeySize() {
    return delegate.getKeySize();
  }

  @Deprecated
  @Override
  public void flush() {
    delegate.flush();
  }

  @Deprecated
  @Override
  public long getRebuildVersion() {
    return delegate.getRebuildVersion();
  }

  @Deprecated
  @Override
  public boolean isRebuilding() {
    return delegate.isRebuilding();
  }

  @Deprecated
  @Override
  public Object getFirstKey() {
    return delegate.getFirstKey();
  }

  @Deprecated
  @Override
  public Object getLastKey() {
    return delegate.getLastKey();
  }

  @Deprecated
  @Override
  public OIndexCursor cursor() {
    return delegate.cursor();
  }

  @Deprecated
  @Override
  public OIndexCursor descCursor() {
    return delegate.descCursor();
  }

  @Deprecated
  @Override
  public OIndexKeyCursor keyCursor() {
    return delegate.keyCursor();
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return delegate.iterateEntries(keys, ascSortOrder);
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    return delegate.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return delegate.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return delegate.iterateEntriesMajor(toKey, toInclusive, ascOrder);
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

  public Set<String> getClusters() {
    return delegate.getClusters();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public String getDatabaseName() {
    return delegate.getDatabaseName();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return delegate.supportsOrderedIterations();
  }

  @Override
  public int getIndexId() {
    return delegate.getIndexId();
  }

  @Override
  public int compareTo(OIndex o) {
    return delegate.compareTo(o);
  }

  @Override
  public Object getCollatingValue(Object key) {
    return delegate.getCollatingValue(key);
  }

  @Override
  public boolean loadFromConfiguration(ODocument config) {
    return delegate.loadFromConfiguration(config);
  }

  @Override
  public ODocument updateConfiguration() {
    return delegate.updateConfiguration();
  }

  @Override
  public OIndex addCluster(String clusterName) {
    return delegate.addCluster(clusterName);
  }

  @Override
  public OIndex removeCluster(String clusterName) {
    return delegate.removeCluster(clusterName);
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return delegate.canBeUsedInEqualityOperators();
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return delegate.hasRangeQuerySupport();
  }

  @Override
  public OIndexMetadata loadMetadata(ODocument config) {
    return delegate.loadMetadata(config);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void preCommit(OIndexAbstract.IndexTxSnapshot snapshots) {
    delegate.preCommit(snapshots);
  }

  @Override
  public void addTxOperation(OIndexAbstract.IndexTxSnapshot snapshots, OTransactionIndexChanges changes) {
    delegate.addTxOperation(snapshots, changes);
  }

  @Override
  public void commit(OIndexAbstract.IndexTxSnapshot snapshots) {
    delegate.commit(snapshots);
  }

  @Override
  public void postCommit(OIndexAbstract.IndexTxSnapshot snapshots) {
    delegate.postCommit(snapshots);
  }

  @Override
  public void setType(OType type) {
    delegate.setType(type);
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return delegate.getIndexNameByKey(key);
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return delegate.acquireAtomicExclusiveLock(key);
  }

  @Override
  public long size() {
    return delegate.size();
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
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    return delegate.streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    return delegate.streamEntries(keys, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return delegate.streamEntriesMajor(fromKey, fromInclusive, ascOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return delegate.streamEntriesMinor(toKey, toInclusive, ascOrder);
  }
}

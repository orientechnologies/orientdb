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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;

public class OIndexRecorder implements OIndex<OIdentifiable>, OIndexInternal<OIdentifiable> {
  private final OIndexInternal<OIdentifiable> delegate;

  private final Set<Object>                   removedKeys = new HashSet<Object>();
  private final Map<Object, OIdentifiable>    updatedKeys = new HashMap<Object, OIdentifiable>();

  public OIndexRecorder(OIndexInternal<OIdentifiable> delegate) {
    this.delegate = delegate;
  }

  public List<Object> getAffectedKeys() {
    List<Object> result = new ArrayList<Object>(removedKeys.size() + updatedKeys.size());

    for (Object key : removedKeys) {
      result.add(copyKeyIfNeeded(key));
    }
    for (Object key : updatedKeys.keySet()) {
      result.add(copyKeyIfNeeded(key));
    }

    return result;
  }

  private Object copyKeyIfNeeded(Object object) {
    if (object instanceof ORecordId)
      return new ORecordId((ORecordId) object);
    else if (object instanceof OCompositeKey) {
      final OCompositeKey copy = new OCompositeKey();
      for (Object key : ((OCompositeKey) object).getKeys()) {
        copy.addKey(copyKeyIfNeeded(key));
      }

      return copy;
    }

    return object;
  }

  @Override
  public OIndex<OIdentifiable> create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    throw new UnsupportedOperationException("Not allowed operation.");

  }

  @Override
  public String getDatabaseName() {
    return delegate.getDatabaseName();
  }

  @Override
  public OType[] getKeyTypes() {
    return delegate.getKeyTypes();
  }

  @Override
  public OIdentifiable get(Object iKey) {
    iKey = delegate.getCollatingValue(iKey);

    if (removedKeys.contains(iKey))
      return null;

    OIdentifiable updated = updatedKeys.get(iKey);
    if (updated != null)
      return updated;

    return delegate.get(iKey);
  }

  @Override
  public boolean contains(Object iKey) {
    return get(iKey) != null;
  }

  @Override
  public OIndex<OIdentifiable> put(Object iKey, OIdentifiable iValue) {
    iKey = delegate.getCollatingValue(iKey);

    removedKeys.remove(iKey);
    updatedKeys.put(iKey, iValue);

    return this;
  }

  @Override
  public boolean remove(Object key) {
    key = delegate.getCollatingValue(key);

    removedKeys.add(key);
    updatedKeys.remove(key);

    return false;
  }

  @Override
  public boolean remove(Object iKey, OIdentifiable iRID) {
    iKey = delegate.getCollatingValue(iKey);

    removedKeys.add(iKey);
    updatedKeys.remove(iKey);

    return false;
  }

  @Override
  public OIndex<OIdentifiable> clear() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public long getSize() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public long getKeySize() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public ODocument checkEntry(OIdentifiable iRecord, Object iKey) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndex<OIdentifiable> delete() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void deleteWithoutIndexLoad(String indexName) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getType() {
    return delegate.getType();
  }

  @Override
  public boolean isAutomatic() {
    return delegate.isAutomatic();
  }

  @Override
  public long rebuild() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public ODocument getConfiguration() {
    return delegate.getConfiguration();
  }

  @Override
  public OIndexInternal<OIdentifiable> getInternal() {
    return this;
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexDefinition getDefinition() {
    return delegate.getDefinition();
  }

  @Override
  public Set<String> getClusters() {
    return delegate.getClusters();
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexCursor cursor() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexCursor descCursor() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public ODocument getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return delegate.supportsOrderedIterations();
  }

  @Override
  public boolean isRebuilding() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public int compareTo(OIndex<OIdentifiable> o) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getCollatingValue(Object key) {
    return delegate.getCollatingValue(key);
  }

  @Override
  public boolean loadFromConfiguration(ODocument iConfig) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public ODocument updateConfiguration() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndex<OIdentifiable> addCluster(String iClusterName) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndex<OIdentifiable> removeCluster(String iClusterName) {
    throw new UnsupportedOperationException("Not allowed operation");
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
  public void acquireModificationLock() {
  }

  @Override
  public void releaseModificationLock() {
  }

  @Override
  public void lockKeysForUpdateNoTx(Object... key) {
  }

  @Override
  public void lockKeysForUpdateNoTx(Collection<Object> keys) {
  }

  @Override
  public void releaseKeysForUpdateNoTx(Object... key) {
  }

  @Override
  public void releaseKeysForUpdateNoTx(Collection<Object> keys) {
  }

  @Override
  public IndexMetadata loadMetadata(ODocument iConfig) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void setRebuildingFlag() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public String getAlgorithm() {
    return delegate.getAlgorithm();
  }

  @Override
  public void preCommit() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void addTxOperation(ODocument operationDocument) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void commit() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void postCommit() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public long getRebuildVersion() {
    throw new UnsupportedOperationException("Not allowed operation");
  }
}

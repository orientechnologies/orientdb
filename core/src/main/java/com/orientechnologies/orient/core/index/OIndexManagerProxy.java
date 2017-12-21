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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

import java.util.Collection;
import java.util.Locale;
import java.util.Set;

import static com.orientechnologies.orient.core.index.OIndexManagerAbstract.getDatabase;

public class OIndexManagerProxy extends OProxedResource<OIndexManagerAbstract> implements OIndexManager {

  public OIndexManagerProxy(final OIndexManagerAbstract iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  public OIndexManager load() {
    return this;
  }

  /**
   * Force reloading of indexes.
   */
  public OIndexManagerProxy reload() {
    delegate.load(database);
    return this;
  }

  public void create() {
    delegate.create(database);
  }

  public Collection<? extends OIndex<?>> getIndexes() {
    return delegate.getIndexes(database);
  }

  public OIndex<?> getIndex(final String iName) {
    return delegate.getIndex(iName);
  }

  public boolean existsIndex(final String iName) {
    return delegate.existsIndex(iName);
  }

  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex, final OProgressListener progressListener, final ODocument metadata) {

    if (isDistributedCommand()) {
      final OIndexManagerRemote remoteIndexManager = new OIndexManagerRemote(database.getStorage());
      return remoteIndexManager.createIndex(iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata);
    }

    return delegate.createIndex(iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata);
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex, final OProgressListener progressListener, final ODocument metadata, final String algorithm) {
    if (isDistributedCommand()) {
      return distributedCreateIndex(iName, iType, iIndexDefinition, iClusterIdsToIndex, progressListener, metadata, algorithm);
    }

    return delegate.createIndex(iName, iType, iIndexDefinition, iClusterIdsToIndex, progressListener, metadata, algorithm);
  }

  public OIndex<?> distributedCreateIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex, final OProgressListener progressListener, ODocument metadata, String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null)
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType, engine);
    else
      createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType, engine);

    if (metadata != null)
      createIndexDDL += " " + OCommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();

    delegate.acquireExclusiveLock();
    try {
      if (progressListener != null)
        progressListener.onBegin(this, 0, false);

      getDatabase().command(new OCommandSQL(createIndexDDL)).execute();

      ORecordInternal
          .setIdentity(delegate.getDocument(), new ORecordId(getDatabase().getStorage().getConfiguration().getIndexMgrRecordId()));

      if (progressListener != null)
        progressListener.onCompletition(this, true);

      reload();

      final Locale locale = delegate.getServerLocale();
      return delegate.preProcessBeforeReturn(getDatabase(), delegate.getIndex(iName));
    } finally {
      delegate.releaseExclusiveLock();
    }
  }

  public ODocument getConfiguration() {
    return delegate.getConfiguration();
  }

  public OIndexManager dropIndex(final String iIndexName) {
    if (isDistributedCommand()) {
      final OIndexManagerRemote remoteIndexManager = new OIndexManagerRemote(database.getStorage());
      return remoteIndexManager.dropIndex(iIndexName);
    }

    return delegate.dropIndex(iIndexName);
  }

  public String getDefaultClusterName() {
    return delegate.getDefaultClusterName();
  }

  public void setDefaultClusterName(final String defaultClusterName) {
    delegate.setDefaultClusterName(defaultClusterName);
  }

  public ODictionary<ORecord> getDictionary() {
    return delegate.getDictionary();
  }

  public void flush() {
    if (delegate != null)
      delegate.flush();
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(className, fields);
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final String... fields) {
    return delegate.getClassInvolvedIndexes(className, fields);
  }

  public boolean areIndexed(final String className, final Collection<String> fields) {
    return delegate.areIndexed(className, fields);
  }

  public boolean areIndexed(final String className, final String... fields) {
    return delegate.areIndexed(className, fields);
  }

  public Set<OIndex<?>> getClassIndexes(final String className) {
    return delegate.getClassIndexes(className);
  }

  @Override
  public void getClassIndexes(final String className, final Collection<OIndex<?>> indexes) {
    delegate.getClassIndexes(className, indexes);
  }

  public OIndex<?> getClassIndex(final String className, final String indexName) {
    return delegate.getClassIndex(className, indexName);
  }

  @Override
  public OIndexUnique getClassUniqueIndex(final String className) {
    return delegate.getClassUniqueIndex(className);
  }

  public OIndex<?> getClassAutoShardingIndex(final String className) {
    return delegate.getClassAutoShardingIndex(className);
  }

  @Override
  public void recreateIndexes() {
    delegate.recreateIndexes(database);
  }

  @Override
  public void waitTillIndexRestore() {
    delegate.waitTillIndexRestore();
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash() {
    return delegate.autoRecreateIndexesAfterCrash(database);
  }

  @Override
  public void addClusterToIndex(String clusterName, String indexName) {
    delegate.addClusterToIndex(clusterName, indexName);
  }

  @Override
  public void removeClusterFromIndex(String clusterName, String indexName) {
    delegate.removeClusterFromIndex(clusterName, indexName);
  }

  @Override
  public <RET extends ODocumentWrapper> RET save() {
    return delegate.save();
  }

  public void removeClassPropertyIndex(final OIndex<?> idx) {
    delegate.removeClassPropertyIndex(idx);
  }

  private boolean isDistributedCommand() {
    return database.getStorage().isDistributed() && !((OAutoshardedStorage) database.getStorage()).isLocalEnv();
  }
}

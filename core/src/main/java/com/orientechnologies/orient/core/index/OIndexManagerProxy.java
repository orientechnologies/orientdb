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
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Set;

public class OIndexManagerProxy extends OProxedResource<OIndexManagerAbstract>
    implements OIndexManager {

  public OIndexManagerProxy(
      final OIndexManagerAbstract iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  public void load() {}

  /** Force reloading of indexes. */
  public OIndexManagerProxy reload() {
    delegate.load(database);
    return this;
  }

  public void create() {
    delegate.create(database);
  }

  public Collection<? extends OIndex> getIndexes() {
    return delegate.getIndexes(database);
  }

  public OIndex getIndex(final String iName) {
    return delegate.getIndex(database, iName);
  }

  public boolean existsIndex(final String iName) {
    return delegate.existsIndex(iName);
  }

  public OIndex createIndex(
      final String iName,
      final String iType,
      final OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final OProgressListener progressListener,
      final ODocument metadata) {
    return delegate.createIndex(
        database, iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata);
  }

  @Override
  public OIndex createIndex(
      final String iName,
      final String iType,
      final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final OProgressListener progressListener,
      final ODocument metadata,
      final String algorithm) {
    return delegate.createIndex(
        database,
        iName,
        iType,
        iIndexDefinition,
        iClusterIdsToIndex,
        progressListener,
        metadata,
        algorithm);
  }

  public ODocument getConfiguration() {
    return delegate.getConfiguration();
  }

  public OIndexManager dropIndex(final String iIndexName) {
    delegate.dropIndex(database, iIndexName);
    return this;
  }

  public String getDefaultClusterName() {
    return delegate.getDefaultClusterName();
  }

  public void setDefaultClusterName(final String defaultClusterName) {
    delegate.setDefaultClusterName(database, defaultClusterName);
  }

  public ODictionary<ORecord> getDictionary() {
    return delegate.getDictionary(database);
  }

  public Set<OIndex> getClassInvolvedIndexes(
      final String className, final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(database, className, fields);
  }

  public Set<OIndex> getClassInvolvedIndexes(final String className, final String... fields) {
    return delegate.getClassInvolvedIndexes(database, className, fields);
  }

  public boolean areIndexed(final String className, final Collection<String> fields) {
    return delegate.areIndexed(className, fields);
  }

  public boolean areIndexed(final String className, final String... fields) {
    return delegate.areIndexed(className, fields);
  }

  public Set<OIndex> getClassIndexes(final String className) {
    return delegate.getClassIndexes(database, className);
  }

  @Override
  public void getClassIndexes(final String className, final Collection<OIndex> indexes) {
    delegate.getClassIndexes(database, className, indexes);
  }

  public OIndex getClassIndex(final String className, final String indexName) {
    return delegate.getClassIndex(database, className, indexName);
  }

  @Override
  public OIndexUnique getClassUniqueIndex(final String className) {
    return delegate.getClassUniqueIndex(className);
  }

  public OIndex getClassAutoShardingIndex(final String className) {
    return delegate.getClassAutoShardingIndex(database, className);
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
  public OIndexManager save() {
    delegate.save();
    return this;
  }

  public void removeClassPropertyIndex(final OIndex idx) {
    //noinspection deprecation
    delegate.removeClassPropertyIndex(idx);
  }

  public void getClassRawIndexes(String name, Collection<OIndex> indexes) {
    delegate.getClassRawIndexes(name, indexes);
  }

  public OIndexManagerAbstract delegate() {
    return delegate;
  }
}

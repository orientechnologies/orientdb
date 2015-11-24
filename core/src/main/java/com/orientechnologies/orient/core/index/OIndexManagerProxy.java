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

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

public class OIndexManagerProxy extends OProxedResource<OIndexManager> implements OIndexManager {

  public OIndexManagerProxy(final OIndexManager iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  public OIndexManager load() {
    return this;
  }

  /**
   * Force reloading of indexes.
   */
  public OIndexManager reload() {
    return delegate.load();
  }

  public void create() {
    delegate.create();
  }

  public Collection<? extends OIndex<?>> getIndexes() {
    return delegate.getIndexes();
  }

  public OIndex<?> getIndex(final String iName) {
    return delegate.getIndex(iName);
  }

  public boolean existsIndex(final String iName) {
    return delegate.existsIndex(iName);
  }

  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex, final OProgressListener progressListener, ODocument metadata) {
    return delegate.createIndex(iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata);
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, OIndexDefinition iIndexDefinition, int[] iClusterIdsToIndex,
      OProgressListener progressListener, ODocument metadata, String algorithm) {
    return delegate.createIndex(iName, iType, iIndexDefinition, iClusterIdsToIndex, progressListener, metadata, algorithm);
  }

  public OIndex<?> getIndexInternal(final String iName) {
    return ((OIndexManagerShared) delegate).getIndexInternal(iName);
  }

  public ODocument getConfiguration() {
    return delegate.getConfiguration();
  }

  public OIndexManager dropIndex(final String iIndexName) {
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
  public void recreateIndexes() {
    delegate.recreateIndexes();
  }

  @Override
  public void waitTillIndexRestore() {
    delegate.waitTillIndexRestore();
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash() {
    return delegate.autoRecreateIndexesAfterCrash();
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

  @Override
  public boolean isFullCheckpointOnChange() {
    return delegate.isFullCheckpointOnChange();
  }

  @Override
  public void setFullCheckpointOnChange(boolean fullCheckpointOnChange) {
    delegate.setFullCheckpointOnChange(fullCheckpointOnChange);
  }
}

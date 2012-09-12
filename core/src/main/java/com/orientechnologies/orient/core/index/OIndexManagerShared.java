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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Manages indexes at database level. A single instance is shared among multiple databases. Contentions are managed by r/w locks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Artem Orobets added composite index managemement
 * 
 */
public class OIndexManagerShared extends OIndexManagerAbstract implements OIndexManager {

  public OIndexManagerShared(final ODatabaseRecord iDatabase) {
    super(iDatabase);
  }

  public OIndex<?> getIndexInternal(final String iName) {
    acquireSharedLock();
    try {
      final OIndex<?> index = indexes.get(iName.toLowerCase());
      return getIndexInstance(index);
    } finally {
      releaseSharedLock();
    }
  }

  /**
   * 
   * 
   * @param iName
   *          - name of index
   * @param iType
   * @param iClusterIdsToIndex
   * @param iProgressListener
   */
  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition indexDefinition,
      final int[] iClusterIdsToIndex, final OProgressListener iProgressListener) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot create a new index inside a transaction");

    final Character c = OSchemaShared.checkNameIfValid(iName);
    if (c != null)
      throw new IllegalArgumentException("Invalid index name '" + iName + "'. Character '" + c + "' is invalid");

    acquireExclusiveLock();
    try {
      final OIndexInternal<?> index = OIndexes.createIndex(getDatabase(), iType);

      // decide which cluster to use ("index" - for automatic and "manindex" for manual)
      final String clusterName = indexDefinition != null && indexDefinition.getClassName() != null ?
          defaultClusterName : manualClusterName;

      index.create(iName, indexDefinition, getDatabase(), clusterName, iClusterIdsToIndex, iProgressListener);
      addIndexInternal(index);

      setDirty();
      save();

      return getIndexInstance(index);
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexManager dropIndex(final String iIndexName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop an index inside a transaction");

    acquireExclusiveLock();
    try {
      final OIndex<?> idx = indexes.remove(iIndexName.toLowerCase());
      if (idx != null) {
        removeClassPropertyIndex(idx);

        idx.delete();
        setDirty();
        save();
      }
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  private void removeClassPropertyIndex(final OIndex<?> idx) {
    final OIndexDefinition indexDefinition = idx.getDefinition();
    if (indexDefinition == null || indexDefinition.getClassName() == null)
      return;

    final Map<OMultiKey, Set<OIndex<?>>> map = classPropertyIndex.get(indexDefinition.getClassName().toLowerCase());

    if (map == null) {
      return;
    }

    final int paramCount = indexDefinition.getParamCount();

    for (int i = 1; i <= paramCount; i++) {
      final List<String> fields = normalizeFieldNames(indexDefinition.getFields().subList(0, i));
      final OMultiKey multiKey = new OMultiKey(fields);
      final Set<OIndex<?>> indexSet = map.get(multiKey);
      if (indexSet == null)
        continue;
      indexSet.remove(idx);
      if (indexSet.isEmpty()) {
        map.remove(multiKey);
      }
    }

    if (map.isEmpty())
      classPropertyIndex.remove(indexDefinition.getClassName().toLowerCase());
  }

  @Override
  protected void fromStream() {
    acquireExclusiveLock();
    try {
      final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);

      if (idxs != null) {
        OIndexInternal<?> index;
        for (final ODocument d : idxs) {
          try {
            index = OIndexes.createIndex(getDatabase(), (String) d.field(OIndexInternal.CONFIG_TYPE));
            ((OIndexInternal<?>) index).loadFromConfiguration(d);
            addIndexInternal(index);

          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on loading index by configuration: %s", e, d);
          }
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Binds POJO to ODocument.
   */
  @Override
  public ODocument toStream() {
    acquireExclusiveLock();
    try {
      document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        final ORecordTrackedSet idxs = new ORecordTrackedSet(document);

        for (final OIndexInternal<?> i : indexes.values()) {
          idxs.add(i.updateConfiguration());
        }
        document.field(CONFIG_INDEXES, idxs, OType.EMBEDDEDSET);

      } finally {
        document.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
      document.setDirty();

      return document;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected OIndex<?> getIndexInstance(final OIndex<?> iIndex) {
    return iIndex;
  }
}

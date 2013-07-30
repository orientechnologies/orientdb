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
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class OIndexManagerRemote extends OIndexManagerAbstract {
  private static final String QUERY_DROP = "drop index %s";

  public OIndexManagerRemote(final ODatabaseRecord iDatabase) {
    super(iDatabase);
  }

  protected OIndex<?> getRemoteIndexInstance(boolean isMultiValueIndex, String type, String name, Set<String> clustersToIndex,
      OIndexDefinition indexDefinition, ORID identity, ODocument configuration) {
    if (isMultiValueIndex)
      return new OIndexRemoteMultiValue(name, type, identity, indexDefinition, configuration, clustersToIndex);

    return new OIndexRemoteOneValue(name, type, identity, indexDefinition, configuration, clustersToIndex);
  }

  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex, final OProgressListener iProgressListener) {
    final String createIndexDDL;
    if (iIndexDefinition != null) {
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType);
    } else {
      createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType);
    }

    acquireExclusiveLock();
    try {
      if (iProgressListener != null) {
        iProgressListener.onBegin(this, 0);
      }

      getDatabase().command(new OCommandSQL(createIndexDDL)).execute();

      document.setIdentity(new ORecordId(document.getDatabase().getStorage().getConfiguration().indexMgrRecordId));

      if (iProgressListener != null) {
        iProgressListener.onCompletition(this, true);
      }

      reload();

      return preProcessBeforeReturn(indexes.get(iName.toLowerCase()));
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexManager dropIndex(final String iIndexName) {
    acquireExclusiveLock();
    try {
      final String text = String.format(QUERY_DROP, iIndexName);
      getDatabase().command(new OCommandSQL(text)).execute();

      // REMOVE THE INDEX LOCALLY
      indexes.remove(iIndexName.toLowerCase());
      reload();

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected void fromStream() {
    acquireExclusiveLock();
    try {
      clearMetadata();

      final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);
      if (idxs != null) {
        for (ODocument d : idxs) {
          try {
            OIndexInternal<?> newIndex = OIndexes.createIndex(getDatabase(), (String) d.field(OIndexInternal.CONFIG_TYPE));
            OIndexInternal.IndexMetadata newIndexMetadata = newIndex.loadMetadata(d);

            addIndexInternal(getRemoteIndexInstance(newIndex instanceof OIndexMultiValues, newIndexMetadata.getType(),
                newIndexMetadata.getName(), newIndexMetadata.getClustersToIndex(), newIndexMetadata.getIndexDefinition(),
                (ORID) d.field(OIndexAbstract.CONFIG_MAP_RID, OType.LINK), d));
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on loading of index by configuration: %s", e, d);
          }
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public ODocument toStream() {
    throw new UnsupportedOperationException("Remote index cannot be streamed");
  }

  @Override
  public void recreateIndexes() {
    throw new UnsupportedOperationException("recreateIndexes()");
  }

  @Override
  public void waitTillIndexRestore() {
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash() {
    return false;
  }
}

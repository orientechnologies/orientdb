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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.Collection;
import java.util.Set;

public class OIndexManagerRemote extends OIndexManagerAbstract {
  private static final String QUERY_DROP = "drop index %s";

  public OIndexManagerRemote(final ODatabaseDocument iDatabase) {
    super(iDatabase);
  }

  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex, final OProgressListener progressListener, ODocument metadata, String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null)
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType,engine);
    else
      createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType,engine);


    if (metadata != null)
      createIndexDDL += " " + OCommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();

    acquireExclusiveLock();
    try {
      if (progressListener != null)
        progressListener.onBegin(this, 0, false);

      getDatabase().command(new OCommandSQL(createIndexDDL)).execute();

      ORecordInternal.setIdentity(document, new ORecordId(
          ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().indexMgrRecordId));

      if (progressListener != null)
        progressListener.onCompletition(this, true);

      reload();
      return preProcessBeforeReturn(indexes.get(iName.toLowerCase()));
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, OIndexDefinition indexDefinition, int[] clusterIdsToIndex,
      OProgressListener progressListener, ODocument metadata) {
    return createIndex(iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata, null);
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

  @Override
  public void removeClassPropertyIndex(OIndex<?> idx) {
  }

  protected OIndex<?> getRemoteIndexInstance(boolean isMultiValueIndex, String type, String name, String algorithm,
      Set<String> clustersToIndex, OIndexDefinition indexDefinition, ODocument configuration) {
    if (isMultiValueIndex)
      return new OIndexRemoteMultiValue(name, type, algorithm, indexDefinition, configuration, clustersToIndex);

    return new OIndexRemoteOneValue(name, type, algorithm, indexDefinition, configuration, clustersToIndex);
  }

  @Override
  protected void fromStream() {
    acquireExclusiveLock();
    try {
      clearMetadata();

      final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);
      if (idxs != null) {
        for (ODocument d : idxs) {
          d.setLazyLoad(false);
          try {
            final boolean isMultiValue = ODefaultIndexFactory.isMultiValueIndex((String) d.field(OIndexInternal.CONFIG_TYPE));

            final OIndexInternal.IndexMetadata newIndexMetadata = OIndexAbstract.loadMetadataInternal(d,
                (String) d.field(OIndexInternal.CONFIG_TYPE), d.<String> field(OIndexInternal.ALGORITHM),
                d.<String> field(OIndexInternal.VALUE_CONTAINER_ALGORITHM));

            addIndexInternal(getRemoteIndexInstance(isMultiValue, newIndexMetadata.getType(), newIndexMetadata.getName(),newIndexMetadata.getAlgorithm(),
                newIndexMetadata.getClustersToIndex(), newIndexMetadata.getIndexDefinition(), d));
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on loading of index by configuration: %s", e, d);
          }
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }
}

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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class OIndexManagerRemote extends OIndexManagerAbstract {
  private              AtomicBoolean skipPush         = new AtomicBoolean(false);
  private static final String        QUERY_DROP       = "drop index `%s` if exists";
  private static final long          serialVersionUID = -6570577338095096235L;
  private OStorage storage;

  public OIndexManagerRemote(OStorage storage) {
    super();
    this.storage = storage;
  }

  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex, final OProgressListener progressListener, ODocument metadata, String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null)
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType, engine);
    else
      createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType, engine);

    if (metadata != null)
      createIndexDDL += " " + OCommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();

    acquireExclusiveLock();
    try {
      if (progressListener != null)
        progressListener.onBegin(this, 0, false);

      getDatabase().command(createIndexDDL).close();

      ORecordInternal.setIdentity(document, new ORecordId(getDatabase().getStorage().getConfiguration().getIndexMgrRecordId()));

      if (progressListener != null)
        progressListener.onCompletition(this, true);

      reload();

      return preProcessBeforeReturn(getDatabase(), indexes.get(iName));
    } catch (OCommandExecutionException x) {
      throw new OIndexException(x.getMessage());
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
      getDatabase().command(text).close();

      // REMOVE THE INDEX LOCALLY
      indexes.remove(iIndexName);
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
  public void recreateIndexes(ODatabaseDocumentInternal database) {
    throw new UnsupportedOperationException("recreateIndexes(ODatabaseDocumentInternal)");
  }

  @Override
  public void waitTillIndexRestore() {
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash(ODatabaseDocumentInternal database) {
    return false;
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash() {
    return false;
  }

  @Override
  public void removeClassPropertyIndex(OIndex<?> idx) {
  }

  protected OIndex<?> getRemoteIndexInstance(boolean isMultiValueIndex, String type, String name, String algorithm,
      Set<String> clustersToIndex, OIndexDefinition indexDefinition, ORID identity, ODocument configuration) {
    if (isMultiValueIndex)
      return new OIndexRemoteMultiValue(name, type, algorithm, identity, indexDefinition, configuration, clustersToIndex,
          getStorage().getName());

    return new OIndexRemoteOneValue(name, type, algorithm, identity, indexDefinition, configuration, clustersToIndex,
        getStorage().getName());
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

            final OIndexMetadata newIndexMetadata = OIndexAbstract
                .loadMetadataInternal(d, (String) d.field(OIndexInternal.CONFIG_TYPE), d.<String>field(OIndexInternal.ALGORITHM),
                    d.<String>field(OIndexInternal.VALUE_CONTAINER_ALGORITHM));

            addIndexInternal(getRemoteIndexInstance(isMultiValue, newIndexMetadata.getType(), newIndexMetadata.getName(),
                newIndexMetadata.getAlgorithm(), newIndexMetadata.getClustersToIndex(), newIndexMetadata.getIndexDefinition(),
                (ORID) d.field(OIndexAbstract.CONFIG_MAP_RID), d));
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on loading of index by configuration: %s", e, d);
          }
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndex<?> preProcessBeforeReturn(ODatabaseDocumentInternal database, final OIndex<?> index) {
    if (index instanceof OIndexRemoteMultiValue)
      return new OIndexTxAwareMultiValue(database, (OIndex<Collection<OIdentifiable>>) index);
    else if (index instanceof OIndexDictionary)
      return new OIndexTxAwareDictionary(database, (OIndex<OIdentifiable>) index);
    else if (index instanceof OIndexRemoteOneValue)
      return new OIndexTxAwareOneValue(database, (OIndex<OIdentifiable>) index);

    return index;
  }

  @Override
  protected void acquireExclusiveLock() {
    skipPush.set(true);
  }

  @Override
  protected void releaseExclusiveLock() {
    skipPush.set(false);
  }

  @Override
  public void fromStream(ODocument iDocument) {
    //This is the only case where the write locking make sense enabling it using super
    super.acquireExclusiveLock();
    try {
      super.fromStream(iDocument);
    } finally {
      super.releaseExclusiveLock();
    }
  }

  public void update(ODocument indexManager) {
    if (!skipPush.get()) {
      super.fromStream(indexManager);
    }
  }

  protected OStorage getStorage() {
    return storage;
  }

}

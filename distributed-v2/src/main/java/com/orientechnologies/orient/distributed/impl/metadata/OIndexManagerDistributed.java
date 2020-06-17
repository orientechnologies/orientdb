package com.orientechnologies.orient.distributed.impl.metadata;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Created by tglman on 23/06/17. */
public class OIndexManagerDistributed extends OIndexManagerShared {

  public OIndexManagerDistributed(OStorage storage) {
    super(storage);
  }

  @Override
  public OIndexManagerAbstract load(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.load(database);
          return null;
        });
    return this;
  }

  public OIndex createIndex(
      ODatabaseDocumentInternal database,
      final String iName,
      final String iType,
      final OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final OProgressListener progressListener,
      final ODocument metadata) {

    if (isDistributedCommand(database)) {
      return distributedCreateIndex(
          database,
          iName,
          iType,
          indexDefinition,
          clusterIdsToIndex,
          progressListener,
          metadata,
          null);
    }

    return super.createIndex(
        database, iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata);
  }

  @Override
  public OIndex createIndex(
      ODatabaseDocumentInternal database,
      final String iName,
      final String iType,
      final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final OProgressListener progressListener,
      final ODocument metadata,
      final String algorithm) {
    if (isDistributedCommand(database)) {
      return distributedCreateIndex(
          database,
          iName,
          iType,
          iIndexDefinition,
          iClusterIdsToIndex,
          progressListener,
          metadata,
          algorithm);
    }

    return super.createIndex(
        database,
        iName,
        iType,
        iIndexDefinition,
        iClusterIdsToIndex,
        progressListener,
        metadata,
        algorithm);
  }

  public OIndex distributedCreateIndex(
      ODatabaseDocumentInternal database,
      final String iName,
      final String iType,
      final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final OProgressListener progressListener,
      ODocument metadata,
      String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null)
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType, engine);
    else createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType, engine);

    if (metadata != null)
      createIndexDDL +=
          " " + OCommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();

    if (progressListener != null) progressListener.onBegin(this, 0, false);

    sendCommand(database, createIndexDDL);

    ORecordInternal.setIdentity(
        getDocument(),
        new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId()));

    if (progressListener != null) progressListener.onCompletition(this, true);

    reload();

    return super.preProcessBeforeReturn(database, super.getIndex(database, iName));
  }

  private boolean isDistributedCommand(ODatabaseDocumentInternal database) {
    return database.getStorage().isDistributed()
        && !((OAutoshardedStorage) database.getStorage()).isLocalEnv();
  }

  public void dropIndex(ODatabaseDocumentInternal database, final String iIndexName) {
    if (isDistributedCommand(database)) {
      distributedDropIndex(database, iIndexName);
    }

    super.dropIndex(database, iIndexName);
  }

  public void distributedDropIndex(ODatabaseDocumentInternal database, final String iName) {

    String dropIndexDDL = "DROP INDEX `" + iName + "`";

    sendCommand(database, dropIndexDDL);
    ORecordInternal.setIdentity(
        getDocument(),
        new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId()));

    reload();
  }

  public void sendCommand(ODatabaseDocumentInternal database, String query) {
    if (database.getConfiguration().getValueAsInteger(DISTRIBUTED_REPLICATION_PROTOCOL_VERSION)
        == 2) {

      ODistributedContext distributed =
          ((OSharedContextDistributed) database.getSharedContext()).getDistributedContext();
      Future<OSubmitResponse> response =
          distributed
              .getSubmitContext()
              .send(new OSessionOperationId(), new ODDLQuerySubmitRequest(query));
      try {
        response.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    } else {
      database.command(new OCommandSQL(query)).execute();
    }
  }
}

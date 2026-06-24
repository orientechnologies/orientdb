package com.orientechnologies.orient.server.distributed.impl.metadata;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OStringCache;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.transaction.ODistributedSynchronizedSequence;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Future;

/** Created by tglman on 22/06/17. */
public class OSharedContextDistributed extends OSharedContextEmbedded {

  private ODistributedDatabaseImpl distributedContext;

  public OSharedContextDistributed(OStorage storage, OrientDBDistributed orientDB) {
    super(storage, orientDB);
  }

  protected void init(OStorage storage) {
    stringCache =
        new OStringCache(
            orientDB
                .getConfigurations()
                .getConfigurations()
                .getValueAsInteger(OGlobalConfiguration.DB_STRING_CAHCE_SIZE));
    schema = new OSchemaDistributed(this);
    security = orientDB.getSecuritySystem().newSecurity(storage.getName());
    indexManager = new OIndexManagerDistributed(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl(orientDB);
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    liveQueryOpsV2 = new OLiveQueryHookV2.OLiveQueryOps();
    statementCache =
        new OStatementCache(
            orientDB
                .getConfigurations()
                .getConfigurations()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache =
        new OExecutionPlanCache(
            orientDB
                .getConfigurations()
                .getConfigurations()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
    this.registerListener(executionPlanCache);

    queryStats = new OQueryStats();
    activeDistributedQueries = new HashMap<>();

    this.viewManager = new ViewManager(orientDB, storage.getName());
    int sequenceSize =
        orientDB
            .getConfigurations()
            .getConfigurations()
            .getValueAsInteger(DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE);

    transactionSequence = new ODistributedSynchronizedSequence(orientDB.getNodeId(), sequenceSize);
    this.distributedContext = new ODistributedDatabaseImpl((OrientDBDistributed) orientDB, storage);
  }

  @Override
  protected void internalLoad(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.internalLoad(database);
          distributedContext.initFirstOpen(database);
          return null;
        });
  }

  @Override
  protected void internalClose() {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.internalClose();
          distributedContext.shutdown();
          return null;
        });
  }

  @Override
  public Optional<Future<Void>> internalUnload() {
    return (Optional<Future<Void>>)
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              super.internalUnload();
              return Optional.of(distributedContext.suspend());
            });
  }

  @Override
  protected void internalReload(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.internalReload(database);
          return null;
        });
  }

  @Override
  protected void internalCreate(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.internalCreate(database);
          distributedContext.initFirstOpen(database);
          return null;
        });
  }

  public ViewManager getViewManager() {
    return viewManager;
  }

  public ODistributedDatabaseImpl getDistributedContext() {
    return distributedContext;
  }

  @Override
  public OrientDBDistributed getOrientDB() {
    return (OrientDBDistributed) super.getOrientDB();
  }
}

package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by tglman on 22/06/17.
 */
public class OSharedContextDistributed extends OSharedContext {

  public OSharedContextDistributed(OStorage storage) {
    schema = new OSchemaDistributed();
    security = OSecurityManager.instance().newSecurity();
    indexManager = new OIndexManagerDistributed(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl();
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    liveQueryOpsV2 = new OLiveQueryHookV2.OLiveQueryOps();
    commandCache = new OCommandCacheSoftRefs(storage);
    statementCache = new OStatementCache(
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
    queryStats = new OQueryStats();

  }

  public synchronized void load(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      final long timer = PROFILER.startChrono();

      try {
        if (!loaded) {
          schema.load(database);
          security.load();
          indexManager.load(database);
          functionLibrary.load(database);
          scheduler.load(database);
          sequenceLibrary.load(database);
          schema.onPostIndexManagement();
          loaded = true;
        }
      } finally {
        PROFILER.stopChrono(PROFILER.getDatabaseMetric(database.getStorage().getName(), "metadata.load"),
            "Loading of database metadata", timer, "db.*.metadata.load");
      }
      return null;
    });
  }

  @Override
  public synchronized void close() {
    schema.close();
    security.close(false);
    indexManager.close();
    functionLibrary.close();
    scheduler.close();
    sequenceLibrary.close();
    commandCache.clear();
    commandCache.shutdown();
    liveQueryOps.close();
    liveQueryOpsV2.close();
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      schema.reload();
      indexManager.reload();
      security.load();
      functionLibrary.load(database);
      sequenceLibrary.load(database);
      commandCache.clear();
      scheduler.load(database);
      return null;
    });
  }

  public synchronized void create(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      schema.create(database);
      indexManager.create(database);
      security.create();
      functionLibrary.create(database);
      sequenceLibrary.create(database);
      security.createClassTrigger();
      scheduler.create(database);

      // CREATE BASE VERTEX AND EDGE CLASSES
      schema.createClass(database, "V");
      schema.createClass(database, "E");
      loaded = true;
      return null;
    });
  }
}

package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;

/**
 * Created by tglman on 13/06/17.
 */
public class OSharedContextEmbedded extends OSharedContext {
  public OSharedContextEmbedded(OStorage storage) {
    schema = new OSchemaEmbedded(storage.getComponentsFactory().classesAreDetectedByClusterId());
    security = OSecurityManager.instance().newSecurity();
    indexManager = new OIndexManagerShared();
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl();
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    commandCache = new OCommandCacheSoftRefs(storage);
    statementCache = new OStatementCache(
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
    queryStats = new OQueryStats();

  }
}

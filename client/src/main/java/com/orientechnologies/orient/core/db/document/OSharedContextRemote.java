package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaRemote;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by tglman on 13/06/17.
 */
public class OSharedContextRemote extends OSharedContext {

  public OSharedContextRemote(OStorage storage) {
    schema = new OSchemaRemote();
    security = OSecurityManager.instance().newSecurity();
    indexManager = new OIndexManagerRemote(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl();
    sequenceLibrary = new OSequenceLibraryImpl();
  }

  public synchronized void load(ODatabaseDocumentInternal database) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        schema.load(database);
        security.load();
        indexManager.load(database);
        sequenceLibrary.load(database);
        schema.onPostIndexManagement();
        loaded = true;
      }
    } finally {
      PROFILER
          .stopChrono(PROFILER.getDatabaseMetric(database.getStorage().getName(), "metadata.load"), "Loading of database metadata",
              timer, "db.*.metadata.load");
    }
  }

  @Override
  public synchronized void close() {
    schema.close();
    security.close(false);
    indexManager.close();
    sequenceLibrary.close();
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    schema.reload();
    indexManager.reload();
    security.load();
    scheduler.load(database);
  }

}

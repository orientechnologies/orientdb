package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBRemote;
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

  public OSharedContextRemote(OStorage storage, OrientDBRemote orientDBRemote) {
    this.orientDB = orientDBRemote;
    this.storage = storage;
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
        indexManager.load(database);
        //The Immutable snapshot should be after index and schema that require and before everything else that use it
        schema.forceSnapshot(database);
        security.load(database);
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
    security.close();
    indexManager.close();
    sequenceLibrary.close();
    loaded = false;
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    schema.reload(database);
    indexManager.reload();
    //The Immutable snapshot should be after index and schema that require and before everything else that use it
    schema.forceSnapshot(database);
    security.load(database);
    scheduler.load(database);
    sequenceLibrary.load(database);
    functionLibrary.load(database);
  }

}

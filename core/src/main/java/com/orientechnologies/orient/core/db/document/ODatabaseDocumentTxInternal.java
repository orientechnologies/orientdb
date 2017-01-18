package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.OrientDBConfig;

/**
 * Created by tglman on 31/03/16.
 */
public class ODatabaseDocumentTxInternal {

  private ODatabaseDocumentTxInternal() {
  }

  public static ODatabaseDocumentInternal getInternal(ODatabaseDocumentInternal db) {
    if (db instanceof ODatabaseDocumentTx)
      db = ((ODatabaseDocumentTx) db).internal;
    return db;
  }

  public static ODatabaseDocumentTx wrap(ODatabaseDocumentInternal database) {
    return new ODatabaseDocumentTx(database, null);
  }

  public static OrientDBEmbedded getOrCreateEmbeddedFactory(String databaseDirectory, OrientDBConfig config) {
    return ODatabaseDocumentTx.getOrCreateEmbeddedFactory(databaseDirectory, config);
  }

}

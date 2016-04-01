package com.orientechnologies.orient.core.db.document;

/**
 * Created by tglman on 31/03/16.
 */
public class ODatabaseDocumentTxInternal {

  private ODatabaseDocumentTxInternal() {
  }

  public static Object getSessionMetadata(ODatabaseDocumentTx db) {
    return db.sessionMetadata;
  }

  public static void setSessionMetadata(ODatabaseDocumentTx db, Object sessionMetadata) {
    db.sessionMetadata = sessionMetadata;
  }

}

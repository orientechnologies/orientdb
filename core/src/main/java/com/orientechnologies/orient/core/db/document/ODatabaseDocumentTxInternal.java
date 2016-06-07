package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseSessionMetadata;

/**
 * Created by tglman on 31/03/16.
 */
public class ODatabaseDocumentTxInternal {

  private ODatabaseDocumentTxInternal() {
  }

  public static ODatabaseSessionMetadata getSessionMetadata(ODatabaseDocumentTx db) {
    return db.sessionMetadata;
  }

  public static void setSessionMetadata(ODatabaseDocumentTx db, ODatabaseSessionMetadata sessionMetadata) {
    db.sessionMetadata = sessionMetadata;
  }

}

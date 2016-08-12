package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSessionMetadata;

/**
 * Created by tglman on 31/03/16.
 */
public class ODatabaseDocumentTxInternal {

  private ODatabaseDocumentTxInternal() {
  }

  public static ODatabaseSessionMetadata getSessionMetadata(ODatabaseDocument db) {
    if(db instanceof ODatabaseDocumentTx)
      db = ((ODatabaseDocumentTx) db).internal;
    return ((ODatabaseDocumentTxOrig)db).sessionMetadata;
  }

  public static void setSessionMetadata(ODatabaseDocument db, ODatabaseSessionMetadata sessionMetadata) {
    if(db instanceof ODatabaseDocumentTx)
      db = ((ODatabaseDocumentTx) db).internal;
    ((ODatabaseDocumentTxOrig)db).sessionMetadata = sessionMetadata;
  }

  public static ODatabaseDocumentTx wrap(ODatabaseDocumentInternal database) {
    return new ODatabaseDocumentTx(database, null);
  }
  
}

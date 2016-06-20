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
    return ((ODatabaseDocumentTx)db).sessionMetadata;
  }

  public static void setSessionMetadata(ODatabaseDocument db, ODatabaseSessionMetadata sessionMetadata) {
    ((ODatabaseDocumentTx)db).sessionMetadata = sessionMetadata;
  }

}

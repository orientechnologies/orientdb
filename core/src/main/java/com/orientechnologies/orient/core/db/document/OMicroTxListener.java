package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;

public interface OMicroTxListener {

  void onBeforeMicroTxBegin(ODatabaseDocumentEmbedded db, OMicroTransaction microTx);

  void onAfterMicroTxBegin(ODatabaseDocumentEmbedded db, OMicroTransaction microTx);

  void onBeforeMicroTxRollback(ODatabaseDocumentEmbedded db, OMicroTransaction microTx);

  void onAfterMicroTxRollback(ODatabaseDocumentEmbedded db, OMicroTransaction microTx);

  void onBeforeMicroTxCommit(ODatabaseDocumentEmbedded db, OMicroTransaction microTx);

  void onAfterMicroTxCommit(ODatabaseDocumentEmbedded db, OMicroTransaction microTx);

}

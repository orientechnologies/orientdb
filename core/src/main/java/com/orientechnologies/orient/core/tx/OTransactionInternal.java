package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OBasicTransaction;

import java.util.Collection;
import java.util.Map;

public interface OTransactionInternal extends OBasicTransaction {

  Collection<ORecordOperation> getRecordOperations();

  Map<String, OTransactionIndexChanges> getIndexOperations();

  void setStatus(final OTransaction.TXSTATUS iStatus);

  ODatabaseDocumentInternal getDatabase();

  void updateIdentityAfterCommit(ORID oldRID, ORID rid);

  boolean isUsingLog();

  ORecordOperation getRecordEntry(ORID currentRid);

}

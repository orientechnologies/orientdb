package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import java.util.SortedSet;

public interface OLockKeySource {
  SortedSet<ORID> getRids();

  SortedSet<OTransactionUniqueKey> getUniqueKeys();

  OTransactionId getTransactionId();
}

package com.orientechnologies.orient.distributed.context.coordination.action;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;

public interface OCompleteAction {

  void success(OTransactionIdPromise promise, Set<ONodeId> all);

  void failure(OTransactionIdPromise promise, Set<ONodeId> all, Optional<OAcceptResult> optional);

  void complete(OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result);

  OResponseCollector newResponseCollector(
      OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes);
}

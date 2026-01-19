package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;

public interface OResponseCollector {

  public record CompleteInfo(
      OCompleteAction action,
      OTransactionIdPromise promise,
      Set<ONodeId> nodes,
      Optional<OAcceptResult> result) {}

  Optional<CompleteInfo> disconnected(ONodeId node);

  Optional<CompleteInfo> receive(ONodeId node);

  Optional<CompleteInfo> fail(ONodeId node, OAcceptResult acceptResult);

  boolean isTotallyFinished();

  Optional<CompleteInfo> applied();
}

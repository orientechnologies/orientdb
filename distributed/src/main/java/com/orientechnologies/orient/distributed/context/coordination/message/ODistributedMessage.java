package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;

public interface ODistributedMessage {
  OTransactionIdPromise getPromiseId();

  Optional<OAcceptResult> validate(OOperationContext ctx);

  void apply(OOperationContext ctx);

  void cancel(OOperationContext ctx);

  void recoordinate(OOperationContext ctx);
}

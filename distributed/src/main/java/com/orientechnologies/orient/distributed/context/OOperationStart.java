package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

public record OOperationStart(
    OTransactionIdPromise promise, Set<ONodeId> nodes, Future<Optional<OAcceptResult>> result) {}

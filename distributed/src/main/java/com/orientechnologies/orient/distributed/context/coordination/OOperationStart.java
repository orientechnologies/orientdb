package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.util.Set;

public record OOperationStart(OTransactionIdPromise promise, Set<ONodeId> nodes) {}

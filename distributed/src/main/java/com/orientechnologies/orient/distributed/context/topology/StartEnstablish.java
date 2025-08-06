package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.util.Set;

public record StartEnstablish(OTransactionIdPromise idPromise, Set<ONodeId> candidates) {}

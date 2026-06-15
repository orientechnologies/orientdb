package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.util.Set;

public record OCanSyncResult(OSyncState state, Set<ONodeId> others) {}

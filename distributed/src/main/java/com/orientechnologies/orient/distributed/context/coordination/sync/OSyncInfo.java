package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.id.ONodeId;
import java.util.Set;
import java.util.concurrent.Future;

public record OSyncInfo(OSyncId syncId, Set<ONodeId> targets, Future<Boolean> finished) {}

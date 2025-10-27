package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.Set;

public record OSyncInfo(OSyncId syncId, Set<ONodeId> targets) {}

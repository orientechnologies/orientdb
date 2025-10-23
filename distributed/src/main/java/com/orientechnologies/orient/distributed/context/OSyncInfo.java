package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.Set;
import java.util.UUID;

public record OSyncInfo(UUID syncId, Set<ONodeId> targets) {}

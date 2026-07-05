package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.ONodeRole;

public record OMemberInfo(ONodeId node, ONodeRole role) {}

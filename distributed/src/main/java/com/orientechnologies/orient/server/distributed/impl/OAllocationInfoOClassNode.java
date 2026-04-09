package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.List;

public record OAllocationInfoOClassNode(ONodeId node, List<String> clusters) {}

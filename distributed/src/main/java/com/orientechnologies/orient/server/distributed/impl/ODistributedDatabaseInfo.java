package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record ODistributedDatabaseInfo(
    String name,
    ODatabaseId id,
    List<OMemberInfo> members,
    int quorum,
    OAllocationInfo allocation) {

  public OResult toResult() {
    // TODO Auto-generated method stub
    return null;
  }

  public OResult toLegacyResult() {
    var result = new OResultInternal();

    Map<String, String> servers = new HashMap<>();
    for (var member : members) {
      servers.put(member.node().getNode(), member.role().name());
    }
    result.setProperty("servers", servers);

    Map<String, List<String>> clusters = new HashMap<>();
    for (var x : allocation.classes().stream().flatMap((s) -> s.nodes().stream()).toList()) {
      String node = x.node().getNode();
      for (var cl : x.clusters()) {
        clusters.computeIfAbsent(cl, (c) -> new ArrayList<>()).add(node);
      }
    }
    result.setProperty("clusters", clusters);
    result.setProperty("writeQuorum", quorum);

    return result;
  }
}

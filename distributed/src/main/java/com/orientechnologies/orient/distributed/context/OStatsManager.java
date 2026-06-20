package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.ONodeLatencies;
import com.orientechnologies.orient.distributed.ONodeMessages;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OStatsManager {

  private static final ConcurrentMap<ONodeId, ONodeStats> noteStats = new ConcurrentHashMap<>();

  public void receiveStats(
      ONodeId nodeId,
      long bootTime,
      long maxMem,
      long totMem,
      long freeMem,
      long usedMem,
      List<ONodeLatencies> nodesLatencies,
      List<ONodeMessages> nodesMessages) {
    var nodeStats = noteStats.computeIfAbsent(nodeId, ONodeStats::new);
    nodeStats.receiveStats(
        bootTime, maxMem, totMem, freeMem, usedMem, nodesLatencies, nodesMessages);
  }

  public Optional<ONodeStats> getStats(ONodeId node) {
    return Optional.ofNullable(noteStats.get(node));
  }
}

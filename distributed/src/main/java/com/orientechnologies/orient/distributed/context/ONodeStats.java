package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.ONodeLatencies;
import com.orientechnologies.orient.distributed.ONodeMessages;
import java.util.List;

public class ONodeStats {

  private final ONodeId node;
  private long bootTime;
  private long maxMem;
  private long totMem;
  private long freeMem;
  private long usedMem;
  private List<ONodeLatencies> nodesLatencies;
  private List<ONodeMessages> nodesMessages;

  public ONodeStats(ONodeId node) {
    this.node = node;
  }

  public synchronized void receiveStats(
      long bootTime,
      long maxMem,
      long totMem,
      long freeMem,
      long usedMem,
      List<ONodeLatencies> nodesLatencies,
      List<ONodeMessages> nodesMessages) {
    this.bootTime = bootTime;
    this.maxMem = maxMem;
    this.totMem = totMem;
    this.freeMem = freeMem;
    this.usedMem = usedMem;
    this.nodesLatencies = nodesLatencies;
    this.nodesMessages = nodesMessages;
  }

  public ONodeId getNode() {
    return node;
  }

  public long getBootTime() {
    return bootTime;
  }

  public long getMaxMem() {
    return maxMem;
  }

  public long getTotMem() {
    return totMem;
  }

  public long getFreeMem() {
    return freeMem;
  }

  public long getUsedMem() {
    return usedMem;
  }

  public List<ONodeLatencies> getNodesLatencies() {
    return nodesLatencies;
  }

  public List<ONodeMessages> getNodesMessages() {
    return nodesMessages;
  }
}

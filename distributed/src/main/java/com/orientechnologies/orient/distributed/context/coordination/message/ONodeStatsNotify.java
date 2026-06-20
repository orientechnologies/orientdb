package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.ONodeLatencies;
import com.orientechnologies.orient.distributed.ONodeMessages;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ONodeStatsNotify implements OStructuralMessage {

  private final ONodeId nodeId;
  private final long bootTime;
  private final long maxMem;
  private final long totMem;
  private final long freeMem;
  private final long usedMem;
  private final List<ONodeLatencies> nodesLatencies;
  private final List<ONodeMessages> nodesMessages;

  public ONodeStatsNotify(
      ONodeId nodeId,
      long bootTime,
      long maxMem,
      long totMem,
      long freeMem,
      long usedMem,
      List<ONodeLatencies> nodesLatencies,
      List<ONodeMessages> nodesMessages) {
    this.nodeId = nodeId;
    this.bootTime = bootTime;
    this.maxMem = maxMem;
    this.totMem = totMem;
    this.freeMem = freeMem;
    this.usedMem = usedMem;
    this.nodesLatencies = nodesLatencies;
    this.nodesMessages = nodesMessages;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.getNodeState()
        .getStats()
        .receiveStats(
            nodeId, bootTime, maxMem, totMem, freeMem, usedMem, nodesLatencies, nodesMessages);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.nodeId.writeNetwork(out);
    out.writeLong(bootTime);
    out.writeLong(maxMem);
    out.writeLong(totMem);
    out.writeLong(freeMem);
    out.writeLong(usedMem);
    out.writeInt(nodesLatencies.size());
    for (var latency : nodesLatencies) {
      latency.writeNetwork(out);
    }
    out.writeInt(nodesMessages.size());
    for (var mess : nodesMessages) {
      mess.writeNetwork(out);
    }
  }

  @Override
  public short getType() {
    return 20;
  }

  public static ONodeStatsNotify fromNetwork(DataInput input) throws IOException {
    var nodeId = ONodeId.readNetwork(input);
    var bootTime = input.readLong();
    var maxMam = input.readLong();
    var totMeme = input.readLong();
    var freeMem = input.readLong();
    var usedMem = input.readLong();
    int size = input.readInt();
    var nodesLatencies = new ArrayList<ONodeLatencies>(size);
    while (size-- > 0) {
      nodesLatencies.add(ONodeLatencies.readNetwork(input));
    }
    var sizeMessages = input.readInt();
    var nodesMessages = new ArrayList<ONodeMessages>(sizeMessages);
    while (sizeMessages-- > 0) {
      nodesMessages.add(ONodeMessages.readNetwork(input));
    }
    return new ONodeStatsNotify(
        nodeId, bootTime, maxMam, totMeme, freeMem, usedMem, nodesLatencies, nodesMessages);
  }

  public ONodeId getNodeId() {
    return nodeId;
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

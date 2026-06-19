package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ONodeLatencies;
import com.orientechnologies.orient.server.distributed.ONodeMessages;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ONodeStatsNotify implements OStructuralMessage {

  private ONodeId nodeId;
  private long maxMem;
  private long totMem;
  private long freeMem;
  private long usedMem;
  private List<ONodeLatencies> nodesLatencies;
  private List<ONodeMessages> nodesMessages;

  public ONodeStatsNotify(
      ONodeId nodeId,
      long maxMem,
      long totMem,
      long freeMem,
      long usedMem,
      List<ONodeLatencies> nodesLatencies,
      List<ONodeMessages> nodesMessages) {
    this.nodeId = nodeId;
    this.maxMem = maxMem;
    this.totMem = totMem;
    this.freeMem = freeMem;
    this.usedMem = usedMem;
    this.nodesLatencies = nodesLatencies;
    this.nodesMessages = nodesMessages;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {}

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.nodeId.writeNetwork(out);
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
    var nodesMessages = new ArrayList<ONodeMessages>(size);
    while (sizeMessages-- > 0) {
      nodesMessages.add(ONodeMessages.readNetwork(input));
    }
    return new ONodeStatsNotify(
        nodeId, maxMam, totMeme, freeMem, usedMem, nodesLatencies, nodesMessages);
  }
}

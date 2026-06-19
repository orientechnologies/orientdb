package com.orientechnologies.orient.distributed;

import com.orientechnologies.common.profiler.OProfilerEntrySnapshot;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ONodeLatencies(ONodeId node, OProfilerEntrySnapshot stats) {

  public void writeNetwork(DataOutput out) throws IOException {
    node.writeNetwork(out);
    stats.writeNetwork(out);
  }

  public static ONodeLatencies readNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var stats = OProfilerEntrySnapshot.readNetwork(input);
    return new ONodeLatencies(node, stats);
  }
}

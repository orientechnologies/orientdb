package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ONodeStateNetwork {

  private final OTopologyStateNetwork topology;
  private final List<ODatabaseStateNetwork> databases;
  private final OTransactionSequenceStatus sequenceStatus;

  public ONodeStateNetwork(
      OTopologyStateNetwork topology,
      List<ODatabaseStateNetwork> databases,
      OTransactionSequenceStatus sequenceStatus) {
    super();
    this.topology = topology;
    this.databases = databases;
    this.sequenceStatus = sequenceStatus;
  }

  public OTopologyStateNetwork getTopology() {
    return topology;
  }

  public List<ODatabaseStateNetwork> getDatabases() {
    return databases;
  }

  public OTransactionSequenceStatus getSequenceStatus() {
    return sequenceStatus;
  }

  public void writeNetwork(DataOutput output) throws IOException {
    topology.writeNetwork(output);
    output.writeInt(databases.size());
    for (ODatabaseStateNetwork db : databases) {
      db.writeNetwork(output);
    }
    sequenceStatus.writeNetwork(output);
  }

  public static ONodeStateNetwork fromNetwork(DataInput input) throws IOException {
    var topology = OTopologyStateNetwork.fromNetwork(input);
    int dbSize = input.readInt();
    List<ODatabaseStateNetwork> databases = new ArrayList<>();
    while (dbSize-- > 0) {
      databases.add(ODatabaseStateNetwork.readNetwork(input));
    }
    var seqStatus = OTransactionSequenceStatus.readNetwork(input);
    return new ONodeStateNetwork(topology, databases, seqStatus);
  }
}

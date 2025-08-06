package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OOperationMessage;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class OEnstablishTopology implements OOperationMessage {

  private Set<ONodeId> candidates;

  public OEnstablishTopology(Set<ONodeId> candidates) {
    this.candidates = candidates;
  }

  @Override
  public void apply(OrientDBDistributed ctx) {
    ctx.getNodeState().enstablish(candidates);
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx) {
    return ctx.getNodeState().validateEnstablish(candidates);
  }

  public static OEnstablishTopology readNetwork(DataInput input) throws IOException {
    int size = input.readInt();
    Set<ONodeId> candidates = new HashSet<>();
    while (size-- > 0) {
      candidates.add(ONodeId.readNetwork(input));
    }
    return new OEnstablishTopology(candidates);
  }

  @Override
  public short getType() {
    return 3;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(candidates.size());
    for (ONodeId id : candidates) {
      id.writeNetwork(out);
    }
  }
}

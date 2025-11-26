package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class OQuorumNotReached implements OAcceptResult {

  private Set<OAcceptResult> reasons;

  public OQuorumNotReached(Set<OAcceptResult> reasons) {
    this.reasons = reasons;
  }

  @Override
  public boolean executeRetry() {
    return true;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(reasons.size());
    for (OAcceptResult res : this.reasons) {
      res.writeNetwork(out);
    }
  }

  @Override
  public short getType() {
    return 2;
  }

  public static OQuorumNotReached fromNetwork(DataInput input) throws IOException {
    int size = input.readInt();
    Set<OAcceptResult> reasons = new HashSet<>();
    for (int i = 0; i < size; i++) {
      reasons.add(OAcceptResult.readNetwork(input));
    }
    return new OQuorumNotReached(reasons);
  }

  @Override
  public String toString() {
    return "Quorum Not Reached [reasons=" + reasons + "]";
  }
}

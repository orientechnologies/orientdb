package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class OTransactionIdPromise {

  private final ONodeId coordinator;
  private final OTransactionId id;
  private final int recoverSequence;

  public OTransactionIdPromise(ONodeId node, OTransactionId id) {
    this.coordinator = Objects.requireNonNull(node);
    this.id = Objects.requireNonNull(id);
    this.recoverSequence = 0;
  }

  public OTransactionIdPromise(ONodeId node, OTransactionId id, int recoverSequence) {
    this.coordinator = Objects.requireNonNull(node);
    this.id = Objects.requireNonNull(id);
    this.recoverSequence = recoverSequence;
  }

  public ONodeId getCoordinator() {
    return coordinator;
  }

  public OTransactionId getId() {
    return id;
  }

  public int getRecoverSequence() {
    return recoverSequence;
  }

  public static OTransactionIdPromise readNetwork(DataInput input) throws IOException {
    ONodeId node = ONodeId.readNetwork(input);
    OTransactionId id = OTransactionId.readNetwork(input);
    int recoverSequence = OVarIntSerializer.readAsInt(input);
    return new OTransactionIdPromise(node, id, recoverSequence);
  }

  public void writeNetwork(DataOutput output) throws IOException {
    coordinator.writeNetwork(output);
    id.writeNetwork(output);
    OVarIntSerializer.write(output, recoverSequence);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((coordinator == null) ? 0 : coordinator.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + recoverSequence;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OTransactionIdPromise other = (OTransactionIdPromise) obj;
    if (coordinator == null) {
      if (other.coordinator != null) return false;
    } else if (!coordinator.equals(other.coordinator)) return false;
    if (id == null) {
      if (other.id != null) return false;
    } else if (!id.equals(other.id)) return false;
    if (recoverSequence != other.recoverSequence) return false;
    return true;
  }
}

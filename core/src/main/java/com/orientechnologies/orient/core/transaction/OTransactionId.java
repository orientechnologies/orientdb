package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class OTransactionId {
  private int position;
  private long sequence;

  public OTransactionId(int position, long sequence) {
    this.position = position;
    this.sequence = sequence;
  }

  public int getPosition() {
    return position;
  }

  public long getSequence() {
    return sequence;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OTransactionId that = (OTransactionId) o;
    return position == that.position && sequence == that.sequence;
  }

  @Override
  public int hashCode() {
    return Objects.hash(position, sequence);
  }

  public static OTransactionId read(DataInput input) throws IOException {
    Optional<String> nodeOwner;
    if (input.readBoolean()) {
      nodeOwner = Optional.of(input.readUTF());
    } else {
      nodeOwner = Optional.empty();
    }
    int position = input.readInt();
    long sequence = input.readLong();
    return new OTransactionId(position, sequence);
  }

  public void write(DataOutput out) throws IOException {
    out.writeBoolean(false);
    out.writeInt(position);
    out.writeLong(sequence);
  }

  public static OTransactionId readDisk(DataInput input) throws IOException {
    return read(input);
  }

  /** Write the record without the node name, but still keep the boolean for back compatibility
   *
   * @param out
   * @throws IOException
   */
  public void writeDisk(DataOutput out) throws IOException {
    out.writeBoolean(false);
    out.writeInt(position);
    out.writeLong(sequence);
  }

  public static OTransactionId readNetwork(DataInput input) throws IOException {
    int position = OVarIntSerializer.readAsInt(input);
    long sequence = OVarIntSerializer.readAsLong(input);
    return new OTransactionId(position, sequence);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    OVarIntSerializer.write(out, position);
    OVarIntSerializer.write(out, sequence);
  }

  @Override
  public String toString() {
    return "" + position + ":" + sequence;
  }
}

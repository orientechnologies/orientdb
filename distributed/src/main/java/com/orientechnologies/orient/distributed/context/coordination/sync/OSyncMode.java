package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public sealed interface OSyncMode
    permits OSyncMode.BlockingBackup, OSyncMode.NonBlockingBackup, OSyncMode.Delta {

  public record BlockingBackup() implements OSyncMode {

    public byte getType() {
      return 1;
    }

    public static BlockingBackup fromNetwork(DataInput input) throws IOException {
      return new BlockingBackup();
    }

    public void serialize(DataOutput output) throws IOException {}
  }

  public record NonBlockingBackup() implements OSyncMode {

    public byte getType() {
      return 2;
    }

    public static NonBlockingBackup fromNetwork(DataInput input) throws IOException {
      return new NonBlockingBackup();
    }

    public void serialize(DataOutput output) throws IOException {}
  }

  public record Delta(OTransactionSequenceStatus status) implements OSyncMode {

    public byte getType() {
      return 3;
    }

    public static Delta fromNetwork(DataInput input) throws IOException {
      var status = OTransactionSequenceStatus.readNetwork(input);
      return new Delta(status);
    }

    public void serialize(DataOutput output) throws IOException {
      status.writeNetwork(output);
    }
  }

  public byte getType();

  public void serialize(DataOutput output) throws IOException;

  public default void writeNetwork(DataOutput out) throws IOException {
    out.writeByte(getType());
    this.serialize(out);
  }

  public static OSyncMode fromNetwork(DataInput input) throws IOException {
    byte type = input.readByte();
    return switch (type) {
      case 1 -> BlockingBackup.fromNetwork(input);
      case 2 -> NonBlockingBackup.fromNetwork(input);
      case 3 -> Delta.fromNetwork(input);
      default -> throw new IllegalArgumentException("Unexpected OSyncMode type: " + type);
    };
  }
}

package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public sealed interface OCanSyncAccept
    permits OCanSyncAccept.NotAccepted,
        OCanSyncAccept.DeltaSync,
        OCanSyncAccept.NonBlockingSync,
        OCanSyncAccept.BlockingSync {
  public record NotAccepted() implements OCanSyncAccept {

    @Override
    public void serialize(DataOutput output) throws IOException {}

    @Override
    public byte getType() {
      return 1;
    }

    @Override
    public boolean isSync() {
      return false;
    }

    static NotAccepted fromNetwork(DataInput input) throws IOException {
      return new NotAccepted();
    }
  }

  public record DeltaSync(OTransactionSequenceStatus status) implements OCanSyncAccept {

    @Override
    public void serialize(DataOutput output) throws IOException {
      status.writeNetwork(output);
    }

    @Override
    public byte getType() {
      return 2;
    }

    static DeltaSync fromNetwork(DataInput input) throws IOException {
      var status = OTransactionSequenceStatus.readNetwork(input);
      return new DeltaSync(status);
    }
  }

  public record NonBlockingSync() implements OCanSyncAccept {

    @Override
    public void serialize(DataOutput output) throws IOException {}

    @Override
    public byte getType() {
      return 3;
    }

    static NonBlockingSync fromNetwork(DataInput input) throws IOException {
      return new NonBlockingSync();
    }

    @Override
    public boolean isNonBlocking() {
      return true;
    }
  }

  public record BlockingSync() implements OCanSyncAccept {

    @Override
    public void serialize(DataOutput output) throws IOException {}

    @Override
    public byte getType() {
      return 4;
    }

    static BlockingSync fromNetwork(DataInput input) throws IOException {
      return new BlockingSync();
    }
  }

  default void writeNetwork(DataOutput output) throws IOException {
    output.writeByte(getType());
    serialize(output);
  }

  void serialize(DataOutput output) throws IOException;

  byte getType();

  static OCanSyncAccept readNetwork(DataInput input) throws IOException {
    var type = input.readByte();
    return switch (type) {
      case 1 -> NotAccepted.fromNetwork(input);
      case 2 -> DeltaSync.fromNetwork(input);
      case 3 -> NonBlockingSync.fromNetwork(input);
      case 4 -> BlockingSync.fromNetwork(input);

      default -> throw new IllegalArgumentException("Unexpected OCanSyncAccept type: " + type);
    };
  }

  default boolean isSync() {
    return true;
  }

  default boolean isNonBlocking() {
    return false;
  }
}

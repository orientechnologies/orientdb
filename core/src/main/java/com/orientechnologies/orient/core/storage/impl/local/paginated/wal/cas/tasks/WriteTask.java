package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.tasks;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.nio.ByteBuffer;

public final class WriteTask implements Task {
  private final ByteBuffer         buffer;
  private final long               pointer;
  private final OLogSequenceNumber lastLSN;
  private final OLogSequenceNumber checkpointLSN;

  public WriteTask(ByteBuffer buffer, long pointer, OLogSequenceNumber lastLSN, OLogSequenceNumber checkpointLSN) {
    this.buffer = buffer;
    this.pointer = pointer;
    this.lastLSN = lastLSN;
    this.checkpointLSN = checkpointLSN;
  }

  @Override
  public TaskType getType() {
    return TaskType.WRITE;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public long getPointer() {
    return pointer;
  }

  public OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }

  public OLogSequenceNumber getCheckpointLSN() {
    return checkpointLSN;
  }
}

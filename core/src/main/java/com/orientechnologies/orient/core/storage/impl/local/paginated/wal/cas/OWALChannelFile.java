package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.io.OIOUtils;

import com.orientechnologies.common.log.OLogManager;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class OWALChannelFile implements OWALFile {

  private final Path path;
  private FileChannel channel;
  private final long segmentId;

  private final ScalableRWLock channelLock = new ScalableRWLock();

  private boolean closed;
  private StackTraceElement[] closedTrace;

  OWALChannelFile(final Path path, final FileChannel channel, long segmentId) {
    this.path = path;
    this.channel = channel;
    this.segmentId = segmentId;
  }

  @Override
  public long position() throws IOException {
    return executeOperationWithResult("position", FileChannel::position);
  }

  @Override
  public void position(long position) throws IOException {
    executeOperation("position", channel -> channel.position(position));
  }

  @Override
  public void readBuffer(ByteBuffer buffer) throws IOException {
    executeOperation("readBuffer", channel -> OIOUtils.readByteBuffer(buffer, channel));
  }

  @Override
  public long segmentId() {
    return segmentId;
  }

  @Override
  public void force(boolean forceMetadata) throws IOException {
    executeOperation("force", channel -> channel.force(forceMetadata));
  }

  @Override
  public int write(ByteBuffer buffer) throws IOException {
    return executeOperationWithResult("write", channel -> channel.write(buffer));
  }

  @Override
  public void close() throws IOException {
    channelLock.exclusiveLock();
    try {
      closed = true;
      channel.close();
      closedTrace = Thread.currentThread().getStackTrace();
    } finally {
      channelLock.exclusiveUnlock();
    }
  }

  private <T> T executeOperationWithResult(
      final String name, final IOOperationWithResult<T> operation) throws IOException {
    try {
      channelLock.sharedLock();
      try {
        if (closed) {
          final StringWriter writer = new StringWriter();
          writer.append("File ").append(path.toString()).append(" was closed at \n");
          printClosedTrace(writer);
          writer.append("\n").append("will try to reopen file.");

          OLogManager.instance().errorNoDb(this, writer.toString(), null);
        } else {
          return operation.execute(channel);
        }
      } finally {
        channelLock.sharedUnlock();
      }
    } catch (Exception e) {
      OLogManager.instance()
          .errorNoDb(
              this,
              "Error during execution of operation "
                  + name
                  + " will try to reopen file "
                  + path
                  + " and re-try operation.",
              e);
    }

    channelLock.exclusiveLock();
    try {
      try {
        channel.close();
      } catch (IOException ignore) {
      }

      closed = false;
      closedTrace = null;

      channel =
          FileChannel.open(
              path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      return operation.execute(channel);
    } finally {
      channelLock.exclusiveUnlock();
    }
  }

  private void executeOperation(final String name, final IOOperation operation) throws IOException {
    executeOperationWithResult(
        name,
        channel -> {
          operation.execute(channel);
          return null;
        });
  }

  private void printClosedTrace(final StringWriter writer) {
    if (closedTrace != null) {
      for (final StackTraceElement element : closedTrace) {
        writer.append(element.toString()).append("\n");
      }
    }
  }

  private interface IOOperationWithResult<T> {

    T execute(final FileChannel channel) throws IOException;
  }

  private interface IOOperation {

    void execute(final FileChannel channel) throws IOException;
  }
}

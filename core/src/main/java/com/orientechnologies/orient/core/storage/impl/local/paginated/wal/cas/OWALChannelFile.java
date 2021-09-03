package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.io.OIOUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class OWALChannelFile implements OWALFile {
  private final FileChannel channel;
  private final long segmentId;

  OWALChannelFile(FileChannel channel, long segmentId) {
    this.channel = channel;
    this.segmentId = segmentId;
  }

  @Override
  public long position() throws IOException {
    return this.channel.position();
  }

  @Override
  public void position(long position) throws IOException {
    this.channel.position(position);
  }

  @Override
  public void readBuffer(ByteBuffer buffer) throws IOException {
    OIOUtils.readByteBuffer(buffer, channel);
  }

  @Override
  public long segmentId() {
    return segmentId;
  }

  @Override
  public void force(boolean forceMetadata) throws IOException {
    channel.force(forceMetadata);
  }

  @Override
  public int write(ByteBuffer buffer) throws IOException {
    return channel.write(buffer);
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}

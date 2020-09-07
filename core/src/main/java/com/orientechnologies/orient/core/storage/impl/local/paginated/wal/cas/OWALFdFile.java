package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.jnr.LastErrorException;
import com.orientechnologies.common.jnr.ONative;
import java.io.IOException;
import java.nio.ByteBuffer;

public class OWALFdFile implements OWALFile {
  private final int fd;
  private final int blockSize;
  private final long segmentId;

  OWALFdFile(int fd, int blockSize, long segmentId) {
    this.fd = fd;
    this.blockSize = blockSize;
    this.segmentId = segmentId;
  }

  @Override
  public void force(boolean forceMetadata) throws IOException {
    try {
      ONative.instance().fsync(fd);
    } catch (LastErrorException e) {
      throw new IOException("Can not perform force sync.", e);
    }
  }

  @Override
  public int write(ByteBuffer buffer) throws IOException {
    if (buffer.limit() % blockSize != 0) {
      throw new IOException(
          "In direct IO mode, size of the written buffers should be quantified by block size (block size : "
              + blockSize
              + ", buffer size: "
              + buffer.limit()
              + " )");
    }
    try {
      final int written = (int) ONative.instance().write(fd, buffer, buffer.remaining());
      buffer.position(buffer.position() + written);
      return written;
    } catch (LastErrorException e) {
      throw new IOException("Error during writing of data to file", e);
    }
  }

  @Override
  public long position() throws IOException {
    try {
      return ONative.instance().lseek(fd, 0, ONative.SEEK_CUR);
    } catch (LastErrorException e) {
      throw new IOException("Can not retrieve position of file", e);
    }
  }

  @Override
  public void position(long position) throws IOException {
    if (position % blockSize != 0) {
      throw new IOException(
          "In direct IO mode, position of the file should be quantified by block size (block size : "
              + blockSize
              + ", position : "
              + position
              + " )");
    }
    try {
      ONative.instance().lseek(fd, position, ONative.SEEK_SET);
    } catch (LastErrorException e) {
      throw new IOException("Can not set position of file", e);
    }
  }

  @Override
  public void readBuffer(ByteBuffer buffer) throws IOException {
    if (buffer.limit() % blockSize != 0) {
      throw new IOException(
          "In direct IO mode, size of the written buffers should be quantified by block size (block size : "
              + blockSize
              + ", buffer size: "
              + buffer.limit()
              + " )");
    }

    OIOUtils.readByteBuffer(buffer, fd);
  }

  @Override
  public long segmentId() {
    return segmentId;
  }

  @Override
  public void close() throws IOException {
    try {
      ONative.instance().close(fd);
    } catch (LastErrorException e) {
      throw new IOException("Error during closing the file", e);
    }
  }
}

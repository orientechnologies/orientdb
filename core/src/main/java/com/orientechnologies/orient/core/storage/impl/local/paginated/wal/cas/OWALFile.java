package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public interface OWALFile extends Closeable {

  void force(boolean forceMetadata) throws IOException;

  int write(ByteBuffer buffer) throws IOException;

  long position() throws IOException;

  void position(long position) throws IOException;

  void readBuffer(ByteBuffer buffer) throws IOException;

  long segmentId();

  static OWALFile createWriteWALFile(final Path path, final long segmentId) throws IOException {

    return new OWALChannelFile(
        FileChannel.open(
            path,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING),
        segmentId);
  }

  static OWALFile createReadWALFile(Path path, long segmentId) throws IOException {
    return new OWALChannelFile(FileChannel.open(path, StandardOpenOption.READ), segmentId);
  }
}

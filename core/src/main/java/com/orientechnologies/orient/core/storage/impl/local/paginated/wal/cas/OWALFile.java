package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.jna.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.sun.jna.LastErrorException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public interface OWALFile extends Closeable {
  void force(boolean forceMetadata) throws IOException;

  int write(ByteBuffer buffer) throws IOException;

  long position() throws IOException;

  void position(long position) throws IOException;

  void readBuffer(ByteBuffer buffer) throws IOException;

  static OWALFile createWriteWALFile(Path path, boolean allowDirectIO, int blockSize) throws IOException {
    if (allowDirectIO) {
      try {
        final int fd = ONative.instance().open(path.toAbsolutePath().toString(),
            ONative.O_WRONLY | ONative.O_CREAT | ONative.O_EXCL | ONative.O_APPEND | ONative.O_DIRECT);
        return new OWALFdFile(fd, blockSize);
      } catch (LastErrorException e) {
        OLogManager.instance()
            .errorNoDb(OWALFile.class, "Can not open file using Linux API, Java FileChannel will be used instead", e);

      }
    }

    Files.deleteIfExists(path);

    return new OWALChannelFile(
        FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND));
  }

  static OWALFile createReadWALFile(Path path, boolean allowDirectIO, int blockSize) throws IOException {
    if (allowDirectIO) {
      try {
        final int fd = ONative.instance().open(path.toAbsolutePath().toString(), ONative.O_RDONLY | ONative.O_DIRECT);
        return new OWALFdFile(fd, blockSize);
      } catch (LastErrorException e) {
        OLogManager.instance()
            .errorNoDb(OWALFile.class, "Can not open file using Linux API, Java FileChannel will be used instead", e);
      }
    }

    return new OWALChannelFile(FileChannel.open(path, StandardOpenOption.READ));
  }
}


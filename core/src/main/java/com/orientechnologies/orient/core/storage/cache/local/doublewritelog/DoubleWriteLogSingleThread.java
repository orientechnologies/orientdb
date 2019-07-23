package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OStorageException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public class DoubleWriteLogSingleThread implements DoubleWriteLog {

  /**
   * Stands for "double write log"
   */
  public static final String EXTENSION = ".dwl";

  private static final ODirectMemoryAllocator ALLOCATOR          = ODirectMemoryAllocator.instance();
  private static final int                    DEFAULT_BLOCK_SIZE = 4 * 1024;

  private Path   storagePath;
  private String storageName;

  private FileChannel currentFile;
  private long        currentSegment;

  private       long currentLogSize;
  private final long maxLogSize;

  private int blockSize;

  private static final LZ4Compressor LZ_4_COMPRESSOR;

  static {
    final LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ_4_COMPRESSOR = factory.fastCompressor();
  }

  public DoubleWriteLogSingleThread(final long maxLogSize) {
    this.maxLogSize = maxLogSize;
  }

  @Override
  public void open(final String storageName, final Path storagePath) throws IOException {
    this.storagePath = storagePath;
    this.storageName = storageName;

    final int len = storageName.length();
    final Optional<Path> latestPath = Files.list(storagePath).filter(DoubleWriteLogSingleThread::fileFilter)
        .min((pathOne, pathTwo) -> {

          final String indexOneStr = pathOne.getFileName().toString().substring(len + 1);
          final String indexTwoStr = pathTwo.getFileName().toString().substring(len + 1);

          final long indexOne = Long.parseLong(indexOneStr);
          final long indexTwo = Long.parseLong((indexTwoStr));

          return -Long.compare(indexOne, indexTwo);
        });

    this.currentSegment = latestPath.map(path -> {
      final String indexStr = path.getFileName().toString().substring(len + 1);
      return Long.parseLong(indexStr) + 1;
    }).orElse(0L);

    this.currentFile = createLogFile();
    this.currentLogSize = calculateLogSize();

    blockSize = OIOUtils.calculateBlockSize(storagePath.toAbsolutePath().toString());
    if (blockSize == -1) {
      blockSize = DEFAULT_BLOCK_SIZE;
    }
  }

  private FileChannel createLogFile() throws IOException {
    final Path currentFilePath = storagePath.resolve(storageName + "_" + currentSegment + EXTENSION);
    return FileChannel.open(currentFilePath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.SYNC,
        StandardOpenOption.CREATE_NEW);
  }

  private static boolean fileFilter(final Path path) {
    return path.endsWith(EXTENSION);
  }

  @Override
  public boolean write(final ByteBuffer[] buffers, final long fileId, final int pageIndex) throws IOException {
    int bytesToWrite = OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE;

    for (ByteBuffer buffer : buffers) {
      bytesToWrite += buffer.limit();
    }

    final OPointer pageContainer;
    if (buffers.length > 1) {
      pageContainer = ALLOCATOR.allocate(bytesToWrite, -1);
    } else {
      pageContainer = null;
    }

    try {
      final ByteBuffer containerBuffer;
      if (buffers.length > 1) {
        containerBuffer = pageContainer.getNativeByteBuffer();
        for (ByteBuffer buffer : buffers) {
          buffer.rewind();
          containerBuffer.put(buffer);
        }
      } else {
        containerBuffer = buffers[0];
      }

      containerBuffer.rewind();

      containerBuffer.putLong(0, fileId);
      containerBuffer.putInt(OLongSerializer.LONG_SIZE, pageIndex);
      containerBuffer.putInt(OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE, buffers.length);

      final int maxCompressedLength = LZ_4_COMPRESSOR.maxCompressedLength(bytesToWrite);
      final OPointer compressedPointer = ODirectMemoryAllocator.instance().allocate(maxCompressedLength, -1);
      try {
        final ByteBuffer compressedBuffer = compressedPointer.getNativeByteBuffer();
        LZ_4_COMPRESSOR.compress(containerBuffer, compressedBuffer);

        compressedBuffer.rewind();

        this.currentLogSize += OIOUtils.writeByteBuffer(compressedBuffer, currentFile, this.currentLogSize);

        //all writes are quantified by page size, to prevent corruption of already written data at the next write
        this.currentLogSize = ((this.currentLogSize + blockSize - 1) / blockSize) * blockSize;
      } finally {
        ODirectMemoryAllocator.instance().deallocate(compressedPointer);
      }
    } finally {
      if (pageContainer != null) {
        ODirectMemoryAllocator.instance().deallocate(pageContainer);
      }
    }

    return this.currentLogSize >= maxLogSize;
  }

  @Override
  public void truncate() throws IOException {
    currentFile.close();

    Files.list(storagePath).filter(DoubleWriteLogSingleThread::fileFilter).forEach(path -> {
      try {
        Files.delete(path);
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException(
            "Can not delete file " + path.toAbsolutePath().toString() + " during truncation of page log for storage "
                + storageName), e);
      }
    });

    this.currentLogSize = 0;
    this.currentSegment++;

    this.currentFile = createLogFile();
  }

  private long calculateLogSize() throws IOException {
    return Files.list(storagePath).filter(DoubleWriteLogSingleThread::fileFilter).mapToLong(path -> {
      try {
        return Files.size(path);
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Can not calculate size of file " + path.toAbsolutePath()), e);
      }
    }).sum();
  }
}

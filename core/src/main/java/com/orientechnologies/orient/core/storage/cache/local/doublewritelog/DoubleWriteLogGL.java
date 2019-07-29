package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class DoubleWriteLogGL implements DoubleWriteLog {

  /**
   * Stands for "double write log"
   */
  public static final String EXTENSION = ".dwl";

  private static final ODirectMemoryAllocator ALLOCATOR          = ODirectMemoryAllocator.instance();
  private static final int                    DEFAULT_BLOCK_SIZE = 4 * 1024;
  private static final int                    METADATA_SIZE      = 4 * OIntegerSerializer.INT_SIZE;

  private Path   storagePath;
  private String storageName;

  private int pageSize;

  private FileChannel currentFile;
  private long        currentSegment;

  private       long currentLogSize;
  private final long maxLogSize;

  private int blockSize;

  private static final LZ4Compressor       LZ_4_COMPRESSOR;
  private static final LZ4FastDecompressor LZ_4_DECOMPRESSOR;

  private List<Long> tailSegments;

  private boolean restoreMode;

  private Map<ORawPair<Integer, Integer>, ORawPair<Long, Long>> pageMap;

  static {
    final LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ_4_COMPRESSOR = factory.fastCompressor();
    LZ_4_DECOMPRESSOR = factory.fastDecompressor();
  }

  private final Object mutex = new Object();

  public DoubleWriteLogGL(final long maxLogSize) {
    this.maxLogSize = maxLogSize;
  }

  @Override
  public void open(final String storageName, final Path storagePath, int pageSize) throws IOException {
    synchronized (mutex) {
      this.pageSize = pageSize;
      this.storagePath = storagePath;
      this.storageName = storageName;

      this.tailSegments = new ArrayList<>();
      this.pageMap = new HashMap<>();

      final int len = storageName.length();
      final Optional<Path> latestPath = Files.list(storagePath).filter(DoubleWriteLogGL::fileFilter).min((pathOne, pathTwo) -> {

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
  }

  private FileChannel createLogFile() throws IOException {
    final Path currentFilePath = storagePath.resolve(createSegmentName(currentSegment));
    return FileChannel.open(currentFilePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
  }

  private String createSegmentName(long id) {
    return storageName + "_" + id + EXTENSION;
  }

  private static boolean fileFilter(final Path path) {
    return path.endsWith(EXTENSION);
  }

  @Override
  public boolean write(final ByteBuffer[] buffers, final int fileId, final int pageIndex) throws IOException {
    synchronized (mutex) {
      if (currentFile.size() > maxLogSize) {
        currentFile.close();

        tailSegments.add(currentSegment);

        this.currentSegment++;
        this.currentFile = createLogFile();
      }

      final OPointer pageContainer;
      if (buffers.length > 1) {
        pageContainer = ALLOCATOR.allocate(buffers.length * pageSize, -1);
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

        final int maxCompressedLength = LZ_4_COMPRESSOR.maxCompressedLength(buffers.length * pageSize);
        final OPointer compressedPointer = ODirectMemoryAllocator.instance().allocate(maxCompressedLength, -1);
        try {
          final ByteBuffer compressedBuffer = compressedPointer.getNativeByteBuffer();
          LZ_4_COMPRESSOR.compress(containerBuffer, compressedBuffer);

          final int compressedSize = compressedBuffer.position();
          compressedBuffer.rewind();
          compressedBuffer.limit(compressedSize);

          final ByteBuffer metadataBuffer = ByteBuffer.allocate(4 * OIntegerSerializer.INT_SIZE).order(ByteOrder.nativeOrder());
          metadataBuffer.putInt(fileId);
          metadataBuffer.putInt(pageIndex);
          metadataBuffer.putInt(buffers.length);
          metadataBuffer.putInt(compressedSize);

          metadataBuffer.rewind();

          this.currentLogSize += OIOUtils.writeByteBuffer(metadataBuffer, currentFile, this.currentLogSize);
          this.currentLogSize += OIOUtils.writeByteBuffer(compressedBuffer, currentFile, this.currentLogSize);

          currentFile.force(false);

          //all writes are quantified by page size, to prevent corruption of already written data at the next write
          this.currentLogSize = ((this.currentLogSize + blockSize - 1) / blockSize) * blockSize;
        } finally {
          ALLOCATOR.deallocate(compressedPointer);
        }
      } finally {
        if (pageContainer != null) {
          ALLOCATOR.deallocate(pageContainer);
        }
      }

      //we can not truncate log in restore mode because we remove all restore information
      return !restoreMode && this.currentLogSize >= maxLogSize;
    }
  }

  @Override
  public void truncate() throws IOException {
    synchronized (mutex) {
      if (restoreMode) {
        return;
      }

      tailSegments.stream().map(this::createSegmentName).forEach((segment) -> {
        try {
          final Path segmentPath = storagePath.resolve(segment);
          Files.delete(segmentPath);
        } catch (IOException e) {
          throw OException.wrapException(
              new OStorageException("Can not delete segment of double write log - " + segment + " in storage " + storageName), e);
        }
      });

      this.currentLogSize = calculateLogSize();
      tailSegments.clear();
    }
  }

  private long calculateLogSize() throws IOException {
    return Files.list(storagePath).filter(DoubleWriteLogGL::fileFilter).mapToLong(path -> {
      try {
        return Files.size(path);
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Can not calculate size of file " + path.toAbsolutePath()), e);
      }
    }).sum();
  }

  @Override
  public OPointer loadPage(int fileId, int pageIndex, OByteBufferPool bufferPool) throws IOException {
    synchronized (mutex) {
      if (!restoreMode) {
        return null;
      }

      final ORawPair<Long, Long> segmentPosition = pageMap.get(new ORawPair<>(fileId, pageIndex));
      if (segmentPosition == null) {
        return null;
      }

      final String segmentName = createSegmentName(segmentPosition.getFirst());
      final Path segmentPath = storagePath.resolve(segmentName);

      //bellow set of precautions to prevent errors during page restore
      if (Files.exists(segmentPath)) {
        try (final FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
          final long channelSize = channel.size();
          if (channelSize - segmentPosition.getSecond() > METADATA_SIZE) {
            channel.position(segmentPosition.getSecond());

            final ByteBuffer metadataBuffer = ByteBuffer.allocate(METADATA_SIZE).order(ByteOrder.nativeOrder());
            OIOUtils.readByteBuffer(metadataBuffer, channel);
            metadataBuffer.rewind();

            final int storedFileId = metadataBuffer.getInt();
            final int storedPageIndex = metadataBuffer.getInt();
            final int pages = metadataBuffer.getInt();
            final int compressedLen = metadataBuffer.getInt();

            if (storedFileId == fileId && storedPageIndex >= pageIndex && pageIndex < storedPageIndex + pages) {
              if (channelSize - segmentPosition.getSecond() - METADATA_SIZE >= compressedLen) {
                final ByteBuffer compressedBuffer = ByteBuffer.allocate(compressedLen).order(ByteOrder.nativeOrder());
                OIOUtils.readByteBuffer(compressedBuffer, channel);
                compressedBuffer.rewind();

                final ByteBuffer pagesBuffer = ByteBuffer.allocate(pages * pageSize).order(ByteOrder.nativeOrder());
                LZ_4_DECOMPRESSOR.decompress(compressedBuffer, pagesBuffer);

                final int pagePosition = (pageIndex - storedPageIndex) * pageSize;

                pagesBuffer.position(pagePosition);
                pagesBuffer.limit(pagePosition + pageSize);

                final OPointer pointer = bufferPool.acquireDirect(false);
                final ByteBuffer pageBuffer = pointer.getNativeByteBuffer();
                pageBuffer.put(pagesBuffer);

                pageBuffer.rewind();

                return pointer;
              }
            }
          }
        }
      }

      return null;
    }
  }

  @Override
  public void restoreModeOn() throws IOException {
    synchronized (mutex) {
      if (restoreMode) {
        return;
      }

      pageMap.clear();

      final int len = storageName.length();

      final Path[] segments = Files.list(storagePath).filter(DoubleWriteLogGL::fileFilter).toArray(Path[]::new);

      segmentLoop:
      for (final Path segment : segments) {
        try (final FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
          long position = 0;
          long fileSize = channel.size();

          while (fileSize - position > METADATA_SIZE) {
            final ByteBuffer metadataBuffer = ByteBuffer.allocate(METADATA_SIZE).order(ByteOrder.nativeOrder());
            OIOUtils.readByteBuffer(metadataBuffer, channel);
            metadataBuffer.rewind();

            final int fileId = metadataBuffer.getInt();
            final int pageIndex = metadataBuffer.getInt();
            final int pages = metadataBuffer.getInt();
            final int compressedLen = metadataBuffer.getInt();

            final String indexStr = segment.getFileName().toString().substring(len + 1);
            final long segmentId;
            try {
              segmentId = Long.parseLong(indexStr);
            } catch (NumberFormatException e) {
              continue segmentLoop;
            }

            for (int i = 0; i < pages; i++) {
              pageMap.put(new ORawPair<>(fileId, pageIndex + i), new ORawPair<>(segmentId, position));
            }

            position += METADATA_SIZE + compressedLen;
            channel.position(position);
          }
        }
      }

      restoreMode = true;
    }
  }

  @Override
  public void restoreModeOff() {
    synchronized (mutex) {
      pageMap = new HashMap<>();
      restoreMode = false;
    }
  }
}

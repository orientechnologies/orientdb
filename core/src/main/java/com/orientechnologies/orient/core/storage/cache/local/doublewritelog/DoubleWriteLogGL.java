package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

public class DoubleWriteLogGL implements DoubleWriteLog {

  /** Stands for "double write log" */
  public static final String EXTENSION = ".dwl";

  private static final ODirectMemoryAllocator ALLOCATOR = ODirectMemoryAllocator.instance();
  static final int DEFAULT_BLOCK_SIZE = 4 * 1024;

  private static final int XX_HASH_LEN = 8;
  private static final int FILE_ID_LEN = 4;
  private static final int START_PAGE_INDEX_LEN = 4;
  private static final int CHUNK_SIZE_LEN = 4;
  private static final int COMPRESSED_SIZE_LEN = 4;

  private static final int METADATA_SIZE =
      XX_HASH_LEN + FILE_ID_LEN + START_PAGE_INDEX_LEN + CHUNK_SIZE_LEN + COMPRESSED_SIZE_LEN;

  private static final long XX_HASH_SEED = 0x3242AEDF76L;
  private static final XXHash64 XX_HASH;

  static {
    final XXHashFactory factory = XXHashFactory.fastestInstance();
    XX_HASH = factory.hash64();
  }

  private Path storagePath;
  private String storageName;

  private int pageSize;

  private FileChannel currentFile;
  private long currentSegment;

  private long currentLogSize;
  private final long maxSegSize;

  private int blockSize;

  private static final LZ4Compressor LZ_4_COMPRESSOR;
  private static final LZ4FastDecompressor LZ_4_DECOMPRESSOR;

  private List<Long> tailSegments;

  private volatile boolean restoreMode;

  private int checkpointCounter;

  private Map<ORawPair<Integer, Integer>, ORawPair<Long, Long>> pageMap;

  static {
    final LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ_4_COMPRESSOR = factory.fastCompressor();
    LZ_4_DECOMPRESSOR = factory.fastDecompressor();
  }

  private final Object mutex = new Object();

  public DoubleWriteLogGL(final long maxSegSize) {
    this.maxSegSize = maxSegSize;
  }

  @Override
  public void open(final String storageName, final Path storagePath, int pageSize)
      throws IOException {
    synchronized (mutex) {
      this.pageSize = pageSize;
      this.storagePath = storagePath;
      this.storageName = storageName;

      this.tailSegments = new ArrayList<>();
      this.pageMap = new HashMap<>();

      final Optional<Path> latestPath;
      try (final Stream<Path> stream = Files.list(storagePath)) {
        latestPath =
            stream
                .filter(DoubleWriteLogGL::fileFilter)
                .peek((path) -> tailSegments.add(extractSegmentId(path.getFileName().toString())))
                .min(
                    (pathOne, pathTwo) -> {
                      final long indexOne = extractSegmentId(pathOne.getFileName().toString());
                      final long indexTwo = extractSegmentId(pathTwo.getFileName().toString());
                      return -Long.compare(indexOne, indexTwo);
                    });
      }

      this.currentSegment =
          latestPath.map(path -> extractSegmentId(path.getFileName().toString()) + 1).orElse(0L);

      this.currentFile = createLogFile();
      this.currentLogSize = calculateLogSize();

      blockSize = OIOUtils.calculateBlockSize(storagePath.toAbsolutePath().toString());
      if (blockSize == -1) {
        blockSize = DEFAULT_BLOCK_SIZE;
      }

      OLogManager.instance()
          .info(
              this,
              "DWL:%s: block size = %d bytes, maximum segment size = %d MB",
              storageName,
              blockSize,
              maxSegSize / 1024 / 1024);
    }
  }

  private long extractSegmentId(final String segmentName) {
    final int len = storageName.length();

    String index = segmentName.substring(len + 1);
    index = index.substring(0, index.length() - EXTENSION.length());

    return Long.parseLong(index);
  }

  private FileChannel createLogFile() throws IOException {
    final Path currentFilePath = storagePath.resolve(generateSegmentsName(currentSegment));
    return FileChannel.open(
        currentFilePath,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.SYNC);
  }

  private String generateSegmentsName(long id) {
    return storageName + "_" + id + EXTENSION;
  }

  private static boolean fileFilter(final Path path) {
    return path.toString().endsWith(EXTENSION);
  }

  @Override
  public boolean write(final ByteBuffer[] buffers, final int[] fileIds, final int[] pageIndexes)
      throws IOException {
    synchronized (mutex) {
      assert checkpointCounter >= 0;

      if (checkpointCounter == 0 && currentFile.position() >= maxSegSize) {
        addNewSegment();
      }

      int sizeToAllocate = 0;
      for (final ByteBuffer byteBuffer : buffers) {
        sizeToAllocate += LZ_4_COMPRESSOR.maxCompressedLength(byteBuffer.limit());
      }

      sizeToAllocate += buffers.length * METADATA_SIZE;
      final OPointer pageContainer =
          ALLOCATOR.allocate(sizeToAllocate, false, Intention.DWL_ALLOCATE_CHUNK);

      try {
        final ByteBuffer containerBuffer;

        containerBuffer = pageContainer.getNativeByteBuffer();
        assert containerBuffer.position() == 0;

        for (int i = 0; i < buffers.length; i++) {
          final ByteBuffer buffer = buffers[i];
          buffer.rewind();

          final int maxCompressedLength = LZ_4_COMPRESSOR.maxCompressedLength(buffer.limit());
          final OPointer compressedPointer =
              ODirectMemoryAllocator.instance()
                  .allocate(maxCompressedLength, false, Intention.DWL_ALLOCATE_COMPRESSED_CHUNK);
          try {
            final ByteBuffer compressedBuffer = compressedPointer.getNativeByteBuffer();
            LZ_4_COMPRESSOR.compress(buffer, compressedBuffer);

            final int compressedSize = compressedBuffer.position();
            compressedBuffer.rewind();
            compressedBuffer.limit(compressedSize);

            final int xxHashPosition = containerBuffer.position();
            containerBuffer.position(xxHashPosition + XX_HASH_LEN);

            containerBuffer.putInt(fileIds[i]);
            containerBuffer.putInt(pageIndexes[i]);
            containerBuffer.putInt(buffer.limit() / pageSize);
            containerBuffer.putInt(compressedSize);

            containerBuffer.put(compressedBuffer);
            containerBuffer.putLong(
                xxHashPosition,
                XX_HASH.hash(
                    containerBuffer,
                    xxHashPosition + XX_HASH_LEN,
                    containerBuffer.position() - xxHashPosition - XX_HASH_LEN,
                    XX_HASH_SEED));
          } finally {
            ALLOCATOR.deallocate(compressedPointer);
          }
        }

        containerBuffer.limit(containerBuffer.position());
        containerBuffer.rewind();

        final long filePosition = currentFile.position();
        long bytesWritten = OIOUtils.writeByteBuffer(containerBuffer, currentFile, filePosition);
        bytesWritten = ((bytesWritten + blockSize - 1) / blockSize) * blockSize;
        currentFile.position(bytesWritten + filePosition);

        this.currentLogSize += bytesWritten;
      } finally {
        ALLOCATOR.deallocate(pageContainer);
      }

      // we can not truncate log in restore mode because we remove all restore information
      return !restoreMode && this.currentLogSize >= maxSegSize && !tailSegments.isEmpty();
    }
  }

  private void addNewSegment() throws IOException {
    currentFile.close();

    tailSegments.add(currentSegment);

    this.currentSegment++;
    this.currentFile = createLogFile();
  }

  @Override
  public void truncate() throws IOException {
    synchronized (mutex) {
      if (restoreMode) {
        return;
      }

      //noinspection resource
      tailSegments.stream()
          .map(this::generateSegmentsName)
          .forEach(
              (segment) -> {
                try {
                  final Path segmentPath = storagePath.resolve(segment);
                  Files.delete(segmentPath);
                } catch (final IOException e) {
                  OLogManager.instance()
                      .errorNoDb(
                          this,
                          "Can not delete segment of double write log - %s in storage %s",
                          e,
                          segment,
                          storageName);
                }
              });

      this.currentLogSize = calculateLogSize();
      tailSegments.clear();
    }
  }

  private long calculateLogSize() throws IOException {
    try (final Stream<Path> stream = Files.list(storagePath)) {
      return stream
          .filter(DoubleWriteLogGL::fileFilter)
          .mapToLong(
              path -> {
                try {
                  return Files.size(path);
                } catch (IOException e) {
                  throw OException.wrapException(
                      new OStorageException(
                          "Can not calculate size of file " + path.toAbsolutePath()),
                      e);
                }
              })
          .sum();
    }
  }

  @Override
  public OPointer loadPage(int fileId, int pageIndex, OByteBufferPool bufferPool)
      throws IOException {
    if (!restoreMode) {
      return null;
    }

    synchronized (mutex) {
      if (!restoreMode) {
        return null;
      }

      final ORawPair<Long, Long> segmentPosition = pageMap.get(new ORawPair<>(fileId, pageIndex));
      if (segmentPosition == null) {
        return null;
      }

      final String segmentName = generateSegmentsName(segmentPosition.first);
      final Path segmentPath = storagePath.resolve(segmentName);

      // bellow set of precautions to prevent errors during page restore
      if (Files.exists(segmentPath)) {
        try (final FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
          final long channelSize = channel.size();
          if (channelSize - segmentPosition.second > METADATA_SIZE) {
            final ByteBuffer metadataBuffer =
                ByteBuffer.allocate(METADATA_SIZE).order(ByteOrder.nativeOrder());

            OIOUtils.readByteBuffer(metadataBuffer, channel, segmentPosition.second, true);
            metadataBuffer.rewind();

            final long xxHash = metadataBuffer.getLong();
            final int storedFileId = metadataBuffer.getInt();
            final int storedPageIndex = metadataBuffer.getInt();
            final int pages = metadataBuffer.getInt();
            final int compressedLen = metadataBuffer.getInt();

            if (pages >= 0
                && storedFileId == fileId
                && pageIndex >= storedPageIndex
                && pageIndex < storedPageIndex + pages) {
              if (channelSize - segmentPosition.second - METADATA_SIZE >= compressedLen) {
                final ByteBuffer buffer =
                    ByteBuffer.allocate(compressedLen + METADATA_SIZE)
                        .order(ByteOrder.nativeOrder());
                OIOUtils.readByteBuffer(buffer, channel, segmentPosition.second, true);

                buffer.rewind();

                if (XX_HASH.hash(buffer, XX_HASH_LEN, buffer.capacity() - XX_HASH_LEN, XX_HASH_SEED)
                    != xxHash) {
                  return null;
                }

                final ByteBuffer pagesBuffer =
                    ByteBuffer.allocate(pages * pageSize).order(ByteOrder.nativeOrder());
                LZ_4_DECOMPRESSOR.decompress(
                    buffer, METADATA_SIZE, pagesBuffer, 0, pagesBuffer.capacity());

                final int pagePosition = (pageIndex - storedPageIndex) * pageSize;

                pagesBuffer.position(pagePosition);
                pagesBuffer.limit(pagePosition + pageSize);

                final OPointer pointer = bufferPool.acquireDirect(false, Intention.LOAD_WAL_PAGE);
                final ByteBuffer pageBuffer = pointer.getNativeByteBuffer();
                assert pageBuffer.position() == 0;
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

      // sort to fetch the
      final Path[] segments;
      try (final Stream<Path> stream = Files.list(storagePath)) {
        //noinspection resource
        segments =
            stream
                .filter(DoubleWriteLogGL::fileFilter)
                .sorted(
                    (pathOne, pathTwo) -> {
                      final long indexOne = extractSegmentId(pathOne.getFileName().toString());
                      final long indexTwo = extractSegmentId((pathTwo.getFileName().toString()));
                      return Long.compare(indexOne, indexTwo);
                    })
                .toArray(Path[]::new);
      }

      segmentLoop:
      for (final Path segment : segments) {
        try (final FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
          long position = 0;
          long fileSize = channel.size();

          while (fileSize - position > METADATA_SIZE) {
            final ByteBuffer metadataBuffer =
                ByteBuffer.allocate(METADATA_SIZE).order(ByteOrder.nativeOrder());
            OIOUtils.readByteBuffer(metadataBuffer, channel, position, true);
            metadataBuffer.rewind();

            final long xxHash = metadataBuffer.getLong();
            final int fileId = metadataBuffer.getInt();
            final int pageIndex = metadataBuffer.getInt();
            final int pages = metadataBuffer.getInt();
            final int compressedLen = metadataBuffer.getInt();

            if (fileId >= 0
                && pages >= 0
                && pageIndex >= 0
                && compressedLen >= 0
                && position + METADATA_SIZE + compressedLen <= fileSize) {
              final ByteBuffer buffer =
                  ByteBuffer.allocate(METADATA_SIZE + compressedLen).order(ByteOrder.nativeOrder());
              OIOUtils.readByteBuffer(buffer, channel, position, true);
              buffer.rewind();

              if (XX_HASH.hash(buffer, XX_HASH_LEN, buffer.capacity() - XX_HASH_LEN, XX_HASH_SEED)
                  != xxHash) {
                OLogManager.instance()
                    .warnNoDb(
                        this,
                        "DWL Segment "
                            + segment
                            + " is broken and will not be used during restore");
                continue segmentLoop;
              }
            } else {
              continue segmentLoop;
            }

            final long segmentId;
            try {
              segmentId = extractSegmentId(segment.getFileName().toString());
            } catch (NumberFormatException e) {
              continue segmentLoop;
            }

            for (int i = 0; i < pages; i++) {
              pageMap.put(
                  new ORawPair<>(fileId, pageIndex + i), new ORawPair<>(segmentId, position));
            }

            position +=
                (long) ((METADATA_SIZE + compressedLen + blockSize - -1) / blockSize) * blockSize;
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

  @Override
  public void close() throws IOException {
    synchronized (mutex) {
      currentFile.close();

      try (final Stream<Path> stream = Files.list(storagePath)) {
        stream
            .filter(DoubleWriteLogGL::fileFilter)
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new OStorageException(
                        "Can not delete file " + path.toString() + " in storage " + storageName);
                  }
                });
      }
    }
  }

  @Override
  public void startCheckpoint() throws IOException {
    synchronized (mutex) {
      addNewSegment();

      checkpointCounter++;
    }
  }

  @Override
  public void endCheckpoint() {
    synchronized (mutex) {
      checkpointCounter--;
    }
  }
}

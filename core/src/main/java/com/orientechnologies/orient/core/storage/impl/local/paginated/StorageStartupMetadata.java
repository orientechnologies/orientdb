/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/6/14
 */
public class StorageStartupMetadata {
  private static final OLogger logger = OLogManager.instance().logger(StorageStartupMetadata.class);
  private static final long XX_HASH_SEED = 0xADF678FE45L;
  private static final XXHash64 XX_HASH_64;

  private static final int XX_HASH_OFFSET = 0;
  private static final int VERSION_OFFSET = XX_HASH_OFFSET + 8;
  private static final int DIRTY_FLAG_OFFSET = VERSION_OFFSET + 4;

  static {
    final XXHashFactory xxHashFactory = XXHashFactory.fastestInstance();
    XX_HASH_64 = xxHashFactory.hash64();
  }

  private static final int VERSION_WITHOUT_DB_OPEN_VERSION = 3;
  private static final int VERSION = 4;

  private final Path filePath;
  private final Path backupPath;

  private FileChannel channel;
  private FileLock fileLock;

  private volatile boolean dirtyFlag;
  private volatile long lastTxId;
  private volatile String openedAtVersion;
  private volatile byte[] txMetadata;

  public StorageStartupMetadata(final Path filePath, final Path backupPath) {
    this.filePath = filePath;
    this.backupPath = backupPath;
  }

  public void addFileToArchive(ZipOutputStream zos, String name) throws IOException {
    final ZipEntry ze = new ZipEntry(name);
    zos.putNextEntry(ze);
    try {
      assert !isDirty();
      final ByteBuffer byteBuffer = serialize();
      zos.write(byteBuffer.array());
    } finally {
      zos.closeEntry();
    }
  }

  public synchronized void create(final String openedAtVersion) throws IOException {

    if (Files.exists(filePath)) {
      Files.delete(filePath);
    }

    channel = createFile(filePath);
    if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean()) {
      lockFile();
    }

    dirtyFlag = true;
    lastTxId = -1;
    this.openedAtVersion = openedAtVersion;

    final ByteBuffer buffer = serialize();
    buffer.rewind();

    update(buffer);
  }

  private void update(ByteBuffer buffer) throws IOException {
    Files.deleteIfExists(backupPath);

    try (FileChannel backupChannel = createFileNew(backupPath)) {
      OIOUtils.writeByteBuffer(buffer, backupChannel, 0);
    }

    channel.truncate(0);
    OIOUtils.writeByteBuffer(buffer, channel, 0);

    Files.deleteIfExists(backupPath);
  }

  protected FileChannel createFile(Path path) throws IOException {
    return FileChannel.open(
        path,
        StandardOpenOption.READ,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.SYNC);
  }

  protected FileChannel createFileNew(Path path) throws IOException {
    return FileChannel.open(
        path,
        StandardOpenOption.READ,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE,
        StandardOpenOption.SYNC);
  }

  private void lockFile() throws IOException {
    try {
      fileLock = channel.tryLock();
    } catch (OverlappingFileLockException e) {
      logger.warn("File is already locked by other thread", e);
    }

    if (fileLock == null)
      throw new OStorageException(
          "Database is locked by another process, please shutdown process and try again");
  }

  public synchronized boolean exists() {
    return Files.exists(filePath);
  }

  public synchronized void open(final String createdAtVersion) throws IOException {
    while (true) {
      if (!Files.exists(filePath)) {
        if (Files.exists(backupPath)) {
          try {
            Files.move(backupPath, filePath, StandardCopyOption.ATOMIC_MOVE);
          } catch (final AtomicMoveNotSupportedException e) {
            Files.move(backupPath, filePath);
          }
        } else {
          logger.infoNoDb("File with startup metadata does not exist, creating new one");
          create(createdAtVersion);
          return;
        }
      }

      channel = createFile(filePath);

      final long size = channel.size();

      if (size < 9) {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        OIOUtils.readByteBuffer(buffer, channel, 0, true);

        buffer.position(0);
        dirtyFlag = buffer.get() > 0;
      } else if (size == 9) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1);
        OIOUtils.readByteBuffer(buffer, channel, 0, true);

        buffer.position(0);
        dirtyFlag = buffer.get() > 0;
        lastTxId = buffer.getLong();
      } else {
        final ByteBuffer buffer = ByteBuffer.allocate((int) size);
        OIOUtils.readByteBuffer(buffer, channel);

        buffer.rewind();

        final long xxHash = XX_HASH_64.hash(buffer, 8, buffer.capacity() - 8, XX_HASH_SEED);
        if (xxHash != buffer.getLong(0)) {
          if (!Files.exists(backupPath)) {
            logger.error(
                "File with startup metadata is broken and can not be used, "
                    + "creation of new one",
                null);
            channel.close();
            create(createdAtVersion);
            return;
          } else {
            logger.error(
                "File with startup metadata is broken and can not be used, "
                    + "will try to use backup version",
                null);
          }

          channel.close();
          Files.deleteIfExists(filePath);

          continue;
        }

        buffer.position(8);
        final int version = buffer.getInt();
        if (version != VERSION && version != VERSION_WITHOUT_DB_OPEN_VERSION) {
          throw new IllegalStateException(
              "Invalid version of the binary format of startup metadata file found "
                  + version
                  + " but expected "
                  + VERSION
                  + " or "
                  + VERSION_WITHOUT_DB_OPEN_VERSION);
        }

        dirtyFlag = buffer.get() > 0;
        lastTxId = buffer.getLong();

        final int metadataLen = buffer.getInt();
        if (metadataLen > 0) {
          final byte[] txMeta = new byte[metadataLen];
          buffer.get(txMeta);

          txMetadata = txMeta;
        }

        if (version == VERSION) {
          final int openedAtVersionLen = buffer.getInt();

          if (openedAtVersionLen > 0) {
            final byte[] rawOpenedAtVersion = new byte[openedAtVersionLen];
            buffer.get(rawOpenedAtVersion);

            this.openedAtVersion = new String(rawOpenedAtVersion, StandardCharsets.UTF_8);
          }
        }
      }

      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean()) {
        lockFile();
      }

      break;
    }
  }

  public synchronized void close() throws IOException {
    if (channel == null) return;

    if (Files.exists(filePath)) {
      if (fileLock != null) {
        fileLock.release();
        fileLock = null;
      }

      channel.close();
      channel = null;
    }
  }

  public synchronized void delete() throws IOException {
    if (channel == null) return;

    if (Files.exists(filePath)) {

      if (fileLock != null) {
        fileLock.release();
        fileLock = null;
      }

      channel.close();
      channel = null;

      Files.delete(filePath);
    }
  }

  public synchronized void makeDirty(final String openedAtVersion) throws IOException {

    if (dirtyFlag) return;

    dirtyFlag = true;
    this.openedAtVersion = openedAtVersion;

    update(serialize());
  }

  public synchronized void clearDirty() throws IOException {
    if (!dirtyFlag) return;

    dirtyFlag = false;
    update(serialize());
  }

  public synchronized void setTxMetadata(final byte[] txMetadata) throws IOException {
    this.txMetadata = txMetadata;

    update(serialize());
  }

  public synchronized void finalMetatada(long lastTxId, byte[] txMetadata) throws IOException {
    this.lastTxId = lastTxId;
    this.txMetadata = txMetadata;
    dirtyFlag = false;
    update(serialize());
  }

  public boolean isDirty() {
    return dirtyFlag;
  }

  public boolean isOpen() {
    return this.channel != null;
  }

  public long getLastTxId() {
    return lastTxId;
  }

  public byte[] getTxMetadata() {
    return txMetadata;
  }

  public String getOpenedAtVersion() {
    return openedAtVersion;
  }

  private ByteBuffer serialize() {
    final ByteBuffer buffer;
    int bufferSize = 8 + 4 + 1 + 8 + 4 + 4;

    if (txMetadata != null) {
      bufferSize += txMetadata.length;
    }

    final byte[] openedAtVersionRaw;
    if (openedAtVersion != null) {
      openedAtVersionRaw = openedAtVersion.getBytes(StandardCharsets.UTF_8);
      bufferSize += openedAtVersionRaw.length;
    } else {
      openedAtVersionRaw = null;
    }

    buffer = ByteBuffer.allocate(bufferSize);

    buffer.position(8);

    buffer.putInt(VERSION);
    // dirty flag
    buffer.put(dirtyFlag ? (byte) 1 : (byte) 0);
    // transaction id
    buffer.putLong(lastTxId);

    // tx metadata
    if (txMetadata == null) {
      buffer.putInt(-1);
    } else {
      buffer.putInt(txMetadata.length);
      buffer.put(txMetadata);
    }

    if (this.openedAtVersion == null) {
      buffer.putInt(-1);
    } else {

      buffer.putInt(openedAtVersionRaw.length);
      buffer.put(openedAtVersionRaw);
    }

    final long xxHash = XX_HASH_64.hash(buffer, 8, buffer.capacity() - 8, XX_HASH_SEED);
    buffer.putLong(0, xxHash);

    buffer.rewind();

    return buffer;
  }
}

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
package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/** Handles the database configuration in one big record. */
public class OStorageConfigurationSegment extends OStorageConfigurationImpl {
  private static final int VERSION_OFFSET = 48;

  // This class uses "double write" pattern.
  // Whenever we want to update configuration, first we write data in backup file and make fsync.
  // Then we write the same data
  // in primary file and make fsync. Then we remove backup file. So does not matter if we have error
  // on any of this stages
  // we always will have consistent storage configuration.
  // Downside of this approach is the double overhead during write of configuration, but it was
  // chosen to keep binary compatibility
  // between versions.

  /** Name of primary file */
  private static final String NAME = "database.ocf";

  /**
   * Name of backup file which is used when we update storage configuration using double write
   * pattern
   */
  private static final String BACKUP_NAME = "database.ocf2";

  private static final long ENCODING_FLAG_1 = 128975354756545L;
  private static final long ENCODING_FLAG_2 = 587138568122547L;
  private static final long ENCODING_FLAG_3 = 812587836547249L;

  private static final int CRC_32_OFFSET = 100;
  private static final byte FORMAT_VERSION = (byte) 42;

  private final String storageName;
  private final Path storagePath;

  public OStorageConfigurationSegment(final OLocalPaginatedStorage storage) {
    super(storage, Charset.forName("UTF-8"));

    this.storageName = storage.getName();
    this.storagePath = storage.getStoragePath();
  }

  @Override
  public void delete() throws IOException {
    lock.writeLock().lock();
    try {
      super.delete();

      clearConfigurationFiles();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove both backup and primary configuration files on delete
   *
   * @see #update()
   */
  private void clearConfigurationFiles() throws IOException {
    final Path file = storagePath.resolve(NAME);
    Files.deleteIfExists(file);

    final Path backupFile = storagePath.resolve(BACKUP_NAME);
    Files.deleteIfExists(backupFile);
  }

  @Override
  public void create() throws IOException {
    lock.writeLock().lock();
    try {
      clearConfigurationFiles();

      super.create();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public OStorageConfigurationImpl load(final OContextConfiguration configuration)
      throws OSerializationException {
    lock.writeLock().lock();
    try {
      initConfiguration(configuration);

      final Path file = storagePath.resolve(NAME);
      final Path backupFile = storagePath.resolve(BACKUP_NAME);

      if (Files.exists(file)) {
        if (readData(file)) {
          return this;
        }

        OLogManager.instance()
            .warnNoDb(
                this,
                "Main storage configuration file %s is broken in storage %s, try to read from backup file %s",
                file,
                storageName,
                backupFile);

        if (Files.exists(backupFile)) {
          if (readData(backupFile)) {
            return this;
          }

          OLogManager.instance()
              .errorNoDb(this, "Backup configuration file %s is broken too", null);
          throw new OStorageException(
              "Invalid format for configuration file " + file + " for storage" + storageName);
        } else {
          OLogManager.instance()
              .errorNoDb(this, "Backup configuration file %s does not exist", null, backupFile);
          throw new OStorageException(
              "Invalid format for configuration file " + file + " for storage" + storageName);
        }
      } else if (Files.exists(backupFile)) {
        OLogManager.instance()
            .warn(
                this,
                "Seems like previous update to the storage '%s' configuration was finished incorrectly, "
                    + "main configuration file %s is absent, reading from backup",
                backupFile,
                file);

        if (readData(backupFile)) {
          return this;
        }

        OLogManager.instance()
            .errorNoDb(this, "Backup configuration file %s is broken", null, backupFile);
        throw new OStorageException(
            "Invalid format for configuration file " + backupFile + " for storage" + storageName);
      } else {
        throw new OStorageException("Can not find configuration file for storage " + storageName);
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OSerializationException(
              "Cannot load database configuration. The database seems corrupted"),
          e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void update() throws OSerializationException {
    lock.writeLock().lock();
    try {
      final Charset utf8 = Charset.forName("UTF-8");
      final byte[] buffer = toStream(utf8);

      final ByteBuffer byteBuffer =
          ByteBuffer.allocate(buffer.length + OIntegerSerializer.INT_SIZE);
      byteBuffer.putInt(buffer.length);
      byteBuffer.put(buffer);

      try {
        if (!Files.exists(storagePath)) {
          Files.createDirectories(storagePath);
        }

        final Path backupFile = storagePath.resolve(BACKUP_NAME);
        Files.deleteIfExists(backupFile);

        try (FileChannel channel =
            FileChannel.open(backupFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
          writeConfigFile(buffer, byteBuffer, channel);
        }

        final Path file = storagePath.resolve(NAME);
        Files.deleteIfExists(file);

        try (FileChannel channel =
            FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
          writeConfigFile(buffer, byteBuffer, channel);
        }

        Files.delete(backupFile);
      } catch (Exception e) {
        throw OException.wrapException(
            new OSerializationException("Error on update storage configuration"), e);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void writeConfigFile(byte[] buffer, ByteBuffer byteBuffer, FileChannel channel)
      throws IOException {
    final ByteBuffer versionBuffer = ByteBuffer.allocate(1);
    versionBuffer.put(FORMAT_VERSION);

    versionBuffer.position(0);
    OIOUtils.writeByteBuffer(versionBuffer, channel, VERSION_OFFSET);

    final ByteBuffer crc32buffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
    final CRC32 crc32 = new CRC32();
    crc32.update(buffer);
    crc32buffer.putInt((int) crc32.getValue());

    crc32buffer.position(0);
    OIOUtils.writeByteBuffer(crc32buffer, channel, CRC_32_OFFSET);

    channel.force(true);

    byteBuffer.position(0);
    OIOUtils.writeByteBuffer(byteBuffer, channel, OFile.HEADER_SIZE);

    channel.force(true);
  }

  private boolean readData(Path file) throws IOException {
    final ByteBuffer byteBuffer;
    final byte fileVersion;
    final int crc32content;

    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      // file header + size of content + at least one byte of content
      if (channel.size()
          < OFile.HEADER_SIZE + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE) {
        return false;
      }

      final ByteBuffer versionBuffer = ByteBuffer.allocate(1);
      OIOUtils.readByteBuffer(versionBuffer, channel, VERSION_OFFSET, true);
      versionBuffer.position(0);

      fileVersion = versionBuffer.get();
      if (fileVersion >= 42) {
        final ByteBuffer crc32buffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
        OIOUtils.readByteBuffer(crc32buffer, channel, CRC_32_OFFSET, true);

        crc32buffer.position(0);
        crc32content = crc32buffer.getInt();
      } else {
        crc32content = 0;
      }

      byteBuffer = ByteBuffer.allocate((int) channel.size() - OFile.HEADER_SIZE);
      OIOUtils.readByteBuffer(byteBuffer, channel, OFile.HEADER_SIZE, true);
    }

    byteBuffer.position(0);
    final int size = byteBuffer.getInt(); // size of string which contains database configuration
    byte[] buffer = new byte[size];

    byteBuffer.get(buffer);

    if (fileVersion < 42) {
      if (byteBuffer.limit()
          >= size + 2 * OIntegerSerializer.INT_SIZE + 3 * OLongSerializer.LONG_SIZE) {
        final long encodingFagOne = byteBuffer.getLong();
        final long encodingFagTwo = byteBuffer.getLong();
        final long encodingFagThree = byteBuffer.getLong();

        final Charset streamEncoding;

        // those flags are added to distinguish between old version of configuration file and new
        // one.
        if (encodingFagOne == ENCODING_FLAG_1
            && encodingFagTwo == ENCODING_FLAG_2
            && encodingFagThree == ENCODING_FLAG_3) {
          final byte[] utf8Encoded = "UTF-8".getBytes(Charset.forName("UTF-8"));

          final int encodingNameLength = byteBuffer.getInt();

          if (encodingNameLength == utf8Encoded.length) {
            final byte[] binaryEncodingName = new byte[encodingNameLength];
            byteBuffer.get(binaryEncodingName);

            final String encodingName = new String(binaryEncodingName, "UTF-8");

            if (encodingName.equals("UTF-8")) {
              streamEncoding = Charset.forName("UTF-8");
            } else {
              return false;
            }

            try {
              fromStream(buffer, 0, buffer.length, streamEncoding);
            } catch (Exception e) {
              OLogManager.instance()
                  .errorNoDb(
                      this,
                      "Error during reading of configuration %s of storage %s",
                      e,
                      file,
                      storageName);
              return false;
            }

          } else {
            return false;
          }
        } else {
          try {
            fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
          } catch (Exception e) {
            OLogManager.instance()
                .errorNoDb(
                    this,
                    "Error during reading of configuration %s of storage %s",
                    e,
                    file,
                    storageName);
            return false;
          }
        }
      } else {
        try {
          fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
        } catch (Exception e) {
          OLogManager.instance()
              .errorNoDb(
                  this,
                  "Error during reading of configuration %s of storage %s",
                  e,
                  file,
                  storageName);
          return false;
        }
      }
    } else {
      CRC32 crc32 = new CRC32();
      crc32.update(buffer);

      if (crc32content != (int) crc32.getValue()) {
        return false;
      }

      try {
        fromStream(buffer, 0, buffer.length, Charset.forName("UTF-8"));
      } catch (Exception e) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "Error during reading of configuration %s of storage %s",
                e,
                file,
                storageName);
        return false;
      }
    }

    return true;
  }
}

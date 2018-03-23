/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Handles the database configuration.
 */
@SuppressWarnings("serial")
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED")
public class OStorageConfigurationSegment extends OStorageConfigurationImpl {
  //This class uses "double write" pattern.
  //Whenever we want to update configuration, first we write data in backup file and make fsync. Then we write the same data
  //in primary file and make fsync. Then we remove backup file. So does not matter if we have error on any of this stages
  //we always will have consistent storage configuration.
  //Downside of this approach is the double overhead during write of configuration, but it was chosen to keep binary compatibility
  //between versions.

  /**
   * Name of primary file
   */
  private static final String NAME = "database.ocf";

  /**
   * Name of backup file which is used when we update storage configuration using double write pattern
   */
  private static final String BACKUP_NAME = "database.ocf2";

  private static final long serialVersionUID = 638874446554389034L;

  private static final long ENCODING_FLAG_1 = 128975354756545L;
  private static final long ENCODING_FLAG_2 = 587138568122547L;
  private static final long ENCODING_FLAG_3 = 812587836547249L;

  private static final int  CRC_32_OFFSET  = 100;
  private static final byte FORMAT_VERSION = (byte) 42;

  private final String storageName;
  private final String storagePath;

  public OStorageConfigurationSegment(final OLocalPaginatedStorage storage) {
    super(storage, Charset.forName("UTF-8"));

    this.storageName = storage.getName();
    this.storagePath = storage.getStoragePath();
  }

  public void close() throws IOException {
    super.close();
  }

  public void delete() throws IOException {
    super.delete();

    clearConfigurationFiles();
  }

  /**
   * Remove both backup and primary configuration files on delete
   *
   * @see #update()
   */
  private void clearConfigurationFiles() {
    final File file = new File(storagePath, NAME);
    if (file.exists()) {
      if (!file.delete()) {
        OLogManager.instance().warn(this, "Can not delete database configuration file %s", file);
      }
    }

    final File backupFile = new File(storagePath, BACKUP_NAME);
    if (backupFile.exists()) {
      if (!backupFile.delete()) {
        OLogManager.instance().warn(this, "Can not delete backup of database configuration file %s", backupFile);
      }
    }
  }

  public void create() throws IOException {
    clearConfigurationFiles();

    super.create();
  }

  @Override
  public OStorageConfigurationImpl load(final Map<String, Object> iProperties) throws OSerializationException {
    try {
      initConfiguration();

      bindPropertiesToContext(iProperties);

      final File file = new File(storagePath, NAME);
      final File backupFile = new File(storagePath, BACKUP_NAME);

      if (file.exists()) {
        if (readData(file)) {
          return this;
        }

        OLogManager.instance()
            .warnNoDb(this, "Main storage configuration file %s is broken in storage %s, try to read from backup file %s", file,
                storageName, backupFile);

        if (backupFile.exists()) {
          if (readData(backupFile)) {
            return this;
          }

          OLogManager.instance().errorNoDb(this, "Backup configuration file %s is broken too", null);
          throw new OStorageException("Invalid format for configuration file " + file + " for storage" + storageName);
        } else {
          OLogManager.instance().errorNoDb(this, "Backup configuration file %s does not exist", null, backupFile);
          throw new OStorageException("Invalid format for configuration file " + file + " for storage" + storageName);
        }
      } else if (backupFile.exists()) {
        OLogManager.instance().warn(this, "Seems like previous update to the storage '%s' configuration was finished incorrectly, "
            + "main configuration file %s is absent, reading from backup", backupFile, file);

        if (readData(backupFile)) {
          return this;
        }

        OLogManager.instance().errorNoDb(this, "Backup configuration file %s is broken", null, backupFile);
        throw new OStorageException("Invalid format for configuration file " + backupFile + " for storage" + storageName);
      } else {
        throw new OStorageException("Can not find configuration file for storage " + storageName);
      }

    } catch (IOException e) {
      throw OException
          .wrapException(new OSerializationException("Cannot load database configuration. The database seems corrupted"), e);
    }
  }

  @Override
  public void lock() {
  }

  @Override
  public void unlock() {
  }

  @Override
  public void update() throws OSerializationException {
    final Charset utf8 = Charset.forName("UTF-8");
    final byte[] buffer = toStream(utf8);

    final CRC32 crc32 = new CRC32();
    crc32.update(buffer);

    final int crc32content = (int) crc32.getValue();
    try {
      final File storagePath = new File(this.storagePath);
      if (!storagePath.exists()) {
        if (!storagePath.mkdirs()) {
          throw new OStorageException("Can not create directory " + storagePath + " of location of storage " + storageName);
        }
      }

      final File backupFile = new File(storagePath, BACKUP_NAME);
      if (backupFile.exists()) {
        if (!backupFile.delete()) {
          throw new OStorageException("Can not delete backup file " + backupFile + " in storage " + storageName);
        }
      }

      RandomAccessFile rnd = new RandomAccessFile(backupFile, "rw");
      try {
        writeConfigFile(buffer, crc32content, rnd);
      } finally {
        rnd.close();
      }

      final File file = new File(storagePath, NAME);
      if (file.exists()) {
        if (!file.delete()) {
          throw new OStorageException("Can not delete configuration file " + file + " in storage " + storageName);
        }
      }

      rnd = new RandomAccessFile(file, "rw");
      try {
        writeConfigFile(buffer, crc32content, rnd);
      } finally {
        rnd.close();
      }

      if (!backupFile.delete()) {
        throw new OStorageException("Can not delete backup file " + backupFile + " in storage " + storageName);
      }

    } catch (Exception e) {
      throw OException.wrapException(new OSerializationException("Error on update storage configuration"), e);
    }
  }

  private void writeConfigFile(byte[] buffer, int crc32content, RandomAccessFile rnd) throws IOException {
    rnd.seek(OFileClassic.VERSION_OFFSET);
    rnd.write(FORMAT_VERSION);

    rnd.seek(CRC_32_OFFSET);
    rnd.writeInt(crc32content);

    if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean()) {
      rnd.getFD().sync();
    }

    rnd.seek(OFileClassic.HEADER_SIZE);
    rnd.writeInt(buffer.length);
    rnd.write(buffer);

    if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean()) {
      rnd.getFD().sync();
    }
  }

  private boolean readData(File file) throws IOException {
    final RandomAccessFile rnd = new RandomAccessFile(file, "r");
    try {
      final byte fileVersion;
      final int crc32content;

      //file header + size of content + at least one byte of content
      if (rnd.length() < OFileClassic.HEADER_SIZE + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE) {
        return false;
      }

      rnd.seek(OFileClassic.VERSION_OFFSET);
      final int ver = rnd.read();
      if (ver < 0) {
        return false;
      }

      fileVersion = (byte) ver;

      if (fileVersion >= FORMAT_VERSION) {
        rnd.seek(CRC_32_OFFSET);
        crc32content = rnd.readInt();
      } else {
        crc32content = 0;
      }

      rnd.seek(OFileClassic.HEADER_SIZE);//padding is added to keep binary compatibility with previous versions

      final int size = rnd.readInt();//size of string which contains database configuration
      byte[] buffer = new byte[size];

      rnd.readFully(buffer);

      if (fileVersion < FORMAT_VERSION) {
        if (rnd.length() >= OFileClassic.HEADER_SIZE + size + 2 * OIntegerSerializer.INT_SIZE + 3 * OLongSerializer.LONG_SIZE) {
          final long encodingFagOne = rnd.readLong();
          final long encodingFagTwo = rnd.readLong();
          final long encodingFagThree = rnd.readLong();

          final Charset streamEncoding;

          //those flags are added to distinguish between old version of configuration file and new one.
          if (encodingFagOne == ENCODING_FLAG_1 && encodingFagTwo == ENCODING_FLAG_2 && encodingFagThree == ENCODING_FLAG_3) {
            final byte[] utf8Encoded = "UTF-8".getBytes(Charset.forName("UTF-8"));

            final int encodingNameLength = rnd.readInt();

            if (encodingNameLength == utf8Encoded.length) {
              final byte[] binaryEncodingName = new byte[encodingNameLength];
              rnd.readFully(binaryEncodingName);

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
                    .errorNoDb(this, "Error during reading of configuration %s of storage %s", e, file, storageName);
                return false;
              }

            } else {
              return false;
            }
          } else {
            return false;
          }
        } else {
          try {
            fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
          } catch (Exception e) {
            OLogManager.instance().errorNoDb(this, "Error during reading of configuration %s of storage %s", e, file, storageName);
            return false;
          }

        }
      } else {
        final CRC32 crc32 = new CRC32();
        crc32.update(buffer);

        if (crc32content != (int) crc32.getValue()) {
          return false;
        }

        try {
          fromStream(buffer, 0, buffer.length, Charset.forName("UTF-8"));
        } catch (Exception e) {
          OLogManager.instance().errorNoDb(this, "Error during reading of configuration %s of storage %s", e, file, storageName);
          return false;
        }
      }
    } finally {
      rnd.close();
    }

    return true;
  }

  public void synch() {
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) {
  }

  public String getFileName() {
    return NAME;
  }
}

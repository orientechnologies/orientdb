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
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
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

  private static final long ENCODING_FLAG_1        = 128975354756545L;
  private static final long ENCODING_FLAG_2        = 587138568122547L;
  private static final long ENCODING_FLAG_3        = 812587836547249L;
  private static final long ENCODING_FLAG_3_CRC_32 = 224737381L;

  private final String storageName;
  private final String storagePath;

  public OStorageConfigurationSegment(final OLocalPaginatedStorage storage) throws IOException {
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

          OLogManager.instance()
              .warnNoDb(this, "Backup configuration file %s is broken too, try to read from main file if possible");
          readNoValidation(file);
        } else {
          OLogManager.instance()
              .warnNoDb(this, "Backup configuration file %s does not exist, try to read from main file if possible");
          readNoValidation(file);
        }
      } else if (backupFile.exists()) {
        OLogManager.instance().warn(this, "Seems like previous update to the storage '%s' configuration was finished incorrectly, "
            + "main configuration file %s is absent, reading from backup", backupFile, file);

        if (readData(backupFile)) {
          return this;
        }

        OLogManager.instance().warnNoDb(this, "Backup configuration file %s is broken, try to read from it if possible");
        readNoValidation(backupFile);
      } else {
        throw new OStorageException("Can not find configuration file for storage " + storageName);
      }

    } catch (IOException e) {
      throw OException
          .wrapException(new OSerializationException("Cannot load database configuration. The database seems corrupted"), e);
    }

    return this;
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

    final String encodingName = "UTF-8";
    final byte[] binaryEncodingName = encodingName.getBytes(utf8);
    //length of presentation of configuration + configuration + 3 utf-8 encoding flags + length of utf-8 encoding name +
    //utf-8 encoding name + crc_32
    final int len = buffer.length + 3 * OBinaryProtocol.SIZE_INT + binaryEncodingName.length + 3 * OLongSerializer.LONG_SIZE;

    final ByteBuffer byteBuffer = ByteBuffer.allocate(len);
    byteBuffer.putInt(buffer.length);
    byteBuffer.put(buffer);

    byteBuffer.putLong(ENCODING_FLAG_1);
    byteBuffer.putLong(ENCODING_FLAG_2);
    byteBuffer.putLong(ENCODING_FLAG_3_CRC_32);

    byteBuffer.putInt(binaryEncodingName.length);
    byteBuffer.put(binaryEncodingName);

    CRC32 crc32 = new CRC32();
    crc32.update(buffer);

    byteBuffer.putInt((int) crc32.getValue());

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
        rnd.write(byteBuffer.array());

        if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean()) {
          rnd.getFD().sync();
        }
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
        rnd.write(byteBuffer.array());

        if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean()) {
          rnd.getFD().sync();
        }
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

  private boolean readData(File file) throws IOException {
    final RandomAccessFile rnd = new RandomAccessFile(file, "r");
    try {
      final int size = rnd.readInt();//size of string which contains database configuration
      byte[] buffer = new byte[size];

      rnd.readFully(buffer);

      if (rnd.length() >= size + 2 * OIntegerSerializer.INT_SIZE + 3 * OLongSerializer.LONG_SIZE) {
        final long encodingFagOne = rnd.readLong();
        final long encodingFagTwo = rnd.readLong();
        final long encodingFagThree = rnd.readLong();

        final Charset streamEncoding;

        //those flags are added to distinguish between old version of configuration file and new one.
        if (encodingFagOne == ENCODING_FLAG_1 && encodingFagTwo == ENCODING_FLAG_2) {
          if (encodingFagThree == ENCODING_FLAG_3 || encodingFagThree == ENCODING_FLAG_3_CRC_32) {
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

              if (encodingFagThree == ENCODING_FLAG_3_CRC_32) {
                if (rnd.length() == rnd.getFilePointer() + OIntegerSerializer.INT_SIZE) {
                  final int contentCRC32 = rnd.readInt();
                  final CRC32 crc32 = new CRC32();
                  crc32.update(buffer);

                  if ((int) crc32.getValue() != contentCRC32) {
                    return false;
                  }
                }
              }

              fromStream(buffer, 0, buffer.length, streamEncoding);
            } else {
              return false;
            }
          } else {
            return false;
          }
        } else {
          return false;
        }
      } else {
        fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
      }
    } finally {
      rnd.close();
    }

    return true;
  }

  private void readNoValidation(File file) throws IOException {
    final RandomAccessFile rnd = new RandomAccessFile(file, "r");
    try {
      final int size = rnd.readInt();
      byte[] buffer = new byte[size];
      rnd.readFully(buffer);
      fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
    } finally {
      rnd.close();
    }

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

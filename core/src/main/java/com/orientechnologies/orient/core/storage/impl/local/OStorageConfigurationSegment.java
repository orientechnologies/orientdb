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
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Handles the database configuration in one big record.
 */
@SuppressWarnings("serial")
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED")
public class OStorageConfigurationSegment extends OStorageConfiguration {
  private static final long serialVersionUID = 638874446554389034L;

  private static final long ENCODING_FLAG_1 = 128975354756545L;
  private static final long ENCODING_FLAG_2 = 587138568122547L;
  private static final long ENCODING_FLAG_3 = 812587836547249L;

  private static final int START_SIZE = 10000;
  private final transient OSingleFileSegment segment;

  public OStorageConfigurationSegment(final OLocalPaginatedStorage iStorage) throws IOException {
    super(iStorage, Charset.forName("UTF-8"));
    segment = new OSingleFileSegment((OLocalPaginatedStorage) storage,
        new OStorageFileConfiguration(null, getDirectory() + "/database.ocf", "classic", fileTemplate.maxSize,
            fileTemplate.fileIncrementSize));
  }

  public void close() throws IOException {
    super.close();
    segment.close();
  }

  public void delete() throws IOException {
    super.delete();
    segment.delete();
  }

  public void create() throws IOException {
    segment.create(START_SIZE);
    super.create();

    final OFile f = segment.getFile();
    if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean())
      f.synch();
  }

  @Override
  public OStorageConfiguration load(final Map<String, Object> iProperties) throws OSerializationException {
    try {
      initConfiguration();

      bindPropertiesToContext(iProperties);

      if (segment.getFile().exists())
        segment.open();
      else {
        segment.create(START_SIZE);

        // @COMPATIBILITY0.9.25
        // CHECK FOR OLD VERSION OF DATABASE
        final ORawBuffer rawRecord = storage.readRecord(CONFIG_RID, null, false, false, null).getResult();
        if (rawRecord != null)
          fromStream(rawRecord.buffer);

        update();
        return this;
      }

      final int size = segment.getFile().readInt(0);
      byte[] buffer = new byte[size];
      segment.getFile().read(OBinaryProtocol.SIZE_INT, buffer, size);

      if (segment.getFile().getFileSize() >= size + 2 * OIntegerSerializer.INT_SIZE + 3 * OLongSerializer.LONG_SIZE) {
        //previous versions of database encode data using native charset encoding
        //as result special characters in cluster or index names can be broken
        //in new versions we use UTF-8 encoding to check which encoding is used
        //encoding flag was added, we check it to know whether we should use UTF-8 or native encoding

        final long encodingFagOne = segment.getFile().readLong(OIntegerSerializer.INT_SIZE + size);
        final long encodingFagTwo = segment.getFile().readLong(OIntegerSerializer.INT_SIZE + size + OLongSerializer.LONG_SIZE);
        final long encodingFagThree = segment.getFile()
            .readLong(OIntegerSerializer.INT_SIZE + size + 2 * OLongSerializer.LONG_SIZE);

        Charset streamEncoding = Charset.defaultCharset();

        if (encodingFagOne == ENCODING_FLAG_1 && encodingFagTwo == ENCODING_FLAG_2 && encodingFagThree == ENCODING_FLAG_3) {
          final byte[] utf8Encoded = "UTF-8".getBytes(Charset.forName("UTF-8"));

          final int encodingNameLength = segment.getFile()
              .readInt(OIntegerSerializer.INT_SIZE + size + 3 * OLongSerializer.LONG_SIZE);

          if (encodingNameLength == utf8Encoded.length) {
            final byte[] binaryEncodingName = new byte[encodingNameLength];
            segment.getFile().read(2 * OIntegerSerializer.INT_SIZE + size + 3 * OLongSerializer.LONG_SIZE, binaryEncodingName,
                encodingNameLength);
            final String encodingName = new String(binaryEncodingName, "UTF-8");

            if (encodingName.equals("UTF-8")) {
              streamEncoding = Charset.forName("UTF-8");
            }
          }
        }

        fromStream(buffer, 0, buffer.length, streamEncoding);
      } else {
        fromStream(buffer, 0, buffer.length, Charset.defaultCharset());
      }

    } catch (IOException e) {
      throw OException
          .wrapException(new OSerializationException("Cannot load database configuration. The database seems corrupted"), e);
    }
    return this;
  }

  @Override
  public void lock() throws IOException {
  }

  @Override
  public void unlock() throws IOException {
  }

  @Override
  public void update() throws OSerializationException {
    try {
      final OFile f = segment.getFile();

      if (!f.isOpen())
        return;

      final Charset utf8 = Charset.forName("UTF-8");
      final byte[] buffer = toStream(utf8);

      final String encodingName = "UTF-8";
      final byte[] binaryEncodingName = encodingName.getBytes(utf8);

      //length of presentation of configuration + configuration + 3 utf-8 encoding flags + length of utf-8 encoding name +
      //utf-8 encoding name
      final int len = buffer.length + 2 * OBinaryProtocol.SIZE_INT + binaryEncodingName.length + 3 * OLongSerializer.LONG_SIZE;

      if (len > f.getFileSize())
        f.allocateSpace(len - f.getFileSize());

      f.writeInt(0, buffer.length);
      f.write(OBinaryProtocol.SIZE_INT, buffer);

      //indicator that stream is encoded using UTF-8 encoding
      f.writeLong(OIntegerSerializer.INT_SIZE + buffer.length, ENCODING_FLAG_1);
      f.writeLong(OIntegerSerializer.INT_SIZE + buffer.length + OLongSerializer.LONG_SIZE, ENCODING_FLAG_2);
      f.writeLong(OIntegerSerializer.INT_SIZE + buffer.length + 2 * OLongSerializer.LONG_SIZE, ENCODING_FLAG_3);

      f.writeInt(OIntegerSerializer.INT_SIZE + buffer.length + 3 * OLongSerializer.LONG_SIZE, binaryEncodingName.length);
      f.write(2 * OIntegerSerializer.INT_SIZE + buffer.length + 3 * OLongSerializer.LONG_SIZE, binaryEncodingName);

      if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean())
        f.synch();

    } catch (Exception e) {
      throw OException.wrapException(new OSerializationException("Error on update storage configuration"), e);
    }
  }

  public void synch() throws IOException {
    segment.getFile().synch();
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  public String getFileName() {
    return segment.getFile().getName();
  }
}
